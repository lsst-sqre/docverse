# Edition reconciliation

`edition_reconcile` is the periodic loop that finds editions whose
recorded publish state has drifted from what the CDN actually serves,
and re-drives the publish — or removes the pointer — so the database's
desired state converges with the edge. It is the "Reconciliation loop"
SQR-112 describes. Keeper-sync projects have an in-run self-heal that
catches some of this during a sync; for a client-upload project this
loop is the only recovery path a lost publish has.

This page is for operators: what the loop compares, every decision it
can reach, the knobs that shape it, and how to read what one tick did.

## What it compares

An edition's presence at the edge is exactly one Cloudflare Workers KV
key, `{project_slug}/{edition_slug}`, whose value names the build the
Worker should serve (`build_id`, plus the `r2_prefix` those objects
live under). The loop compares three things per edition:

- **What the edition wants** — `editions.current_build_id`, and that
  build's `public_id` and `storage_prefix`.
- **What Docverse thinks it did** — the most recent
  `edition_build_history` row for the `(edition_id, current_build_id)`
  pair, and its `publish_status`. The per-pair status is the signal,
  not `editions.publish_status`: the latter is a single slot that
  `set_current_build` never clears, so it cannot distinguish a stale
  success from a fresh one.
- **What the edge serves** — the KV value for that key, read back in
  bulk (`POST .../bulk/get`, 100 keys per request).

An organization with no `cdn_service_label` has no edge to read. Its
tick runs the database leg only and reports `cdn_checked: false`; a
`published` pair is then taken at its word. That is deliberately
different from an organization whose edge was read and answered with
nothing, which is a whole org's worth of drift.

## How a tick runs

The loop is the usual maintenance-pool dispatcher/job/reaper trio,
mirroring `purgatory_cleanup`:

| Function | Pool | Cron | What it does |
| --- | --- | --- | --- |
| `edition_reconcile_dispatcher` | maintenance | `minute={9, 39}` | One `queue_jobs` row per organization (`kind='edition_reconcile'`, `subject_label=<org slug>`), then one arq job each |
| `edition_reconcile` | maintenance | — | The per-org pass; `max_tries=1` |
| `edition_reconcile_reaper` | maintenance | `minute={15, 45}` | Fails rows stuck past the threshold, releasing the per-org mutex |

Every organization gets a row every tick. There is no pre-flight query
that could name the drifted orgs, because drift is by definition state
nobody has compared against the edge yet — a query that could find it
would have to do the per-org job's own work. A healthy org's tick is a
handful of indexed reads plus one bulk KV read per hundred editions,
and plans nothing.

Concurrency is held by the partial unique index
`idx_queue_jobs_edition_reconcile_active_uq` (one active row per org).
A tick whose org already has a live row steps over that org rather
than failing, and a row wedged by an OOM-killed worker holds the mutex
until the reaper fails it — which is why this reaper's threshold is
the tight one on the pool (see below).

The job never publishes anything itself:

- A **republish** is an *enqueue*. The loop calls
  `enqueue_publish_for_edition` with `trigger_override=reconcile`, and
  the ordinary `publish_edition` path on the default pool — with its
  edition lock, its deleted-build guard, and its own retries — remains
  the only thing that writes a pointer. The resulting
  `edition_published` event is tagged `trigger=reconcile`, so repairs
  are separable from ordinary publish traffic.
- An **unpublish** goes through `EditionPublishingService.unpublish`,
  the same route the edition DELETE path uses, so removing a pointer
  means exactly one thing everywhere.

Nothing here ever writes `failed` or clears a publish state. The worst
a bad tick can do is enqueue a redundant publish of the build an
edition already points at, or delete a key for an edition the database
has already tombstoned.

## The decision table

Each of the org's editions — tombstones included — lands in exactly
one row below. The checks are applied in this order, and the order is
load-bearing at two points: an in-flight job outranks whatever state
the pair is recorded in, and the grace window is consulted *last*, so
an edition that was never a repair candidate is not counted as one the
window held back.

| # | Edition | Pair (`edition_build_history`) | Edge pointer | Outcome | Reported as |
| --- | --- | --- | --- | --- | --- |
| 1 | `date_deleted` set, **or its project's** | — | present | **Unpublish** — delete the key | `unpublished` |
| 2 | `date_deleted` set, **or its project's** | — | absent | Nothing to do | `tombstoned` |
| 3 | Live, `current_build_id IS NULL` | — | present | Log only — never deleted, never published | `unexpected_pointers` |
| 4 | Live, `current_build_id IS NULL` | — | absent | Nothing to do | `unpointed` |
| 5 | Live, current build soft-deleted or purged | any | any | Skipped — its objects are gone or on their way out | `retired_build_skipped` |
| 6 | Live | any | any, but a `publish_edition` job is live for the pair | Skipped — a publish is in progress | `in_flight_skipped` |
| 7 | Live | no row, or `publish_status IS NULL` | any | **Republish**, reason `lost_enqueue` | `republished` |
| 8 | Live | `failed` | any | Left alone for an operator | `failed_left_alone` |
| 9 | Live | `pending` or `publishing`, no live job | any | **Republish**, reason `stalled_publish` | `republished` |
| 10 | Live | `published` | not read (`cdn_checked: false`) | Converged as far as this org can be checked | `healthy` |
| 11 | Live | `published` | absent | **Republish**, reason `pointer_missing` | `republished` |
| 12 | Live | `published` | `build_id` or `r2_prefix` disagrees | **Republish**, reason `pointer_stale` | `republished` |
| 13 | Live | `published` | agrees | Converged | `healthy` |
| 14 | Any republish candidate (rows 7, 9, 11, 12) whose pair settled less than the grace window ago | | | Skipped — an enqueue may still be in progress | `grace_skipped` |
| 15 | Any action past the per-job cap | | | Deferred to the next tick | `capped` |

"A `publish_edition` job is live" (row 6) means `in_progress`, or
`queued` **with** a `backend_job_id` written back. A `queued` row whose
backend id is still NULL is the lost-Phase-B shape the loop exists to
re-drive, so it deliberately does not count as live.

The tombstone leg (rows 1–2) is taken on the owning project's
`date_deleted` as readily as on the edition's own. The listing query
deliberately does not filter deleted projects — their editions' keys are
the most likely to be stranded — and the project soft-delete cascade
that tombstones a project's editions shipped without a backfill, so a
project deleted before it still owns editions reading NULL. Left in the
live legs those editions look like the plainest drift there is and earn
a republish nothing can satisfy: `publish_edition` resolves the project
by slug through a lookup that filters tombstones, so the job raises
before it can mark the pair failed, the reaper fails the row an hour
later, and the next tick re-drives the same pair — Sentry noise and a
burnt action-cap slot, every tick, forever. Such an edition is therefore
unpublished if the edge still serves its key, and counted `tombstoned`
otherwise.

### The grace window

Five minutes, and deliberately equal to the `ORPHAN_IDLE_WINDOW` the
orphan sweeps age rows out against, so the reconciler never re-drives
a pair whose two-phase enqueue the sweep would still consider in
progress. It is measured from the later of the edition's
`date_updated` and the history row's `date_created`.

Only republishes are held back by it. A tombstoned edition's stranded
key (row 1) is deleted with no grace: the edition is gone, and the key
can only serve deleted content.

### The cap

`edition_reconcile_max_actions_per_job` bounds republishes **and**
unpublishes together — both are CDN writes, so capping them separately
would let a badly drifted org do twice the work the operator asked
for. The work list is ordered by edition id and cut at the cap, so a
capped org re-attempts the same prefix every tick until those pairs
converge, rather than sampling a rotating window that could starve its
tail. `capped` above zero on consecutive ticks means the org is
drifting faster than one tick can repair.

## Configuration

| Setting | Environment variable | Default | Phalanx value |
| --- | --- | --- | --- |
| `edition_reconcile_enabled` | `DOCVERSE_EDITION_RECONCILE_ENABLED` | `true` | `maintenance.editionReconcileEnabled` |
| `edition_reconcile_max_actions_per_job` | `DOCVERSE_EDITION_RECONCILE_MAX_ACTIONS_PER_JOB` | `100` | `maintenance.editionReconcileMaxActionsPerJob` |
| `edition_reconcile_reaper_threshold_seconds` | `DOCVERSE_EDITION_RECONCILE_REAPER_THRESHOLD_SECONDS` | `3600` | `reaperThresholds.editionReconcileSeconds` |

Notes:

- The feature flag ships **true**, unlike `purgatory_cleanup_enabled`.
  The loop's only action is to enqueue a publish of a build an edition
  already points at, so a wrong tick republishes something that was
  already correct; running with the flag off simply preserves the
  drift. Turn it off when an org has drifted badly enough that you
  want the repair load off the publishing queue while you look at it.
  The cron stays registered either way, so flipping the flag needs no
  worker restart.
- The reaper threshold is one hour rather than the six its
  maintenance-pool siblings use, because this is the only loop on the
  pool that ticks twice an hour: at a sibling-sized threshold, one
  wedged row would cost an organization twelve consecutive passes.
- To reconcile an org immediately in a test environment, drive the
  threshold and the cron cadence down there rather than reaching for a
  manual entrypoint — there is no CLI for this loop.

## Reading an outcome

One tally, reported through four channels with four audiences, all
written after the tick's `queue_jobs` row reaches its terminal state.

### The queue job

`GET /orgs/{org}/jobs?kind=edition_reconcile` lists the per-org rows;
each carries the tally as `progress`:

```json
{
  "editions_scanned": 214,
  "pointers_read": 203,
  "republished": 1,
  "republish_failed": 0,
  "unpublished": 0,
  "unpublish_failed": 0,
  "in_flight_skipped": 0,
  "grace_skipped": 1,
  "failed_left_alone": 2,
  "retired_build_skipped": 0,
  "tombstoned": 9,
  "unpointed": 0,
  "unexpected_pointers": 0,
  "healthy": 201,
  "capped": 0,
  "cdn_checked": true,
  "failed_editions": []
}
```

Read `cdn_checked` first: without it, a tick reporting no drift could
equally mean "nothing is wrong" or "nothing was looked at". Then read
`pointers_read` against `editions_scanned` — a large gap on an org
with `cdn_checked: true` is an edge that has lost keys wholesale,
which no single edition's bucket would show.

`republish_failed` and `unpublish_failed` count actions the tick
planned and could not apply; the editions behind them are named in
`failed_editions`, and the job ends `completed_with_errors`. A failure
on one edition never aborts the rest of the org.

A failed *read-back*, though, ends the whole tick: the row is `failed`
with no `progress`, and nothing was republished. That is deliberate.
The loop acts on absence — a key with no pointer is what makes it
re-drive a publish — so a read it cannot trust is never reported as an
edge with no pointers. Both ways the read can break do this: a non-2xx
Cloudflare keeps returning raises `httpx.HTTPStatusError`, and a 2xx
whose body is not the documented `{"result": {"values": {...}}}` shape
raises `CloudflareKvReadError`, whose Sentry event tags the namespace,
the chunk's key count, and which part of the shape was missing. Either
way the next tick re-plans from current state, so a transient edge
failure costs one pass, not a repair.

### The metrics event

Every tick publishes one org-scoped `edition_reconcile_completed`
event — including the ticks that found nothing, because a reconciler
that goes quiet when it finds nothing is indistinguishable on a
dashboard from one that stopped running. Its fields are
`editions_scanned`, `pointers_read`, `republished`, `unpublished`,
`in_flight_skipped`, `failed_left_alone`, `unexpected_pointers`,
`capped`, `cdn_checked`, and `elapsed`.

`project` is always null: one tick spans every project in the org. The
project-scoped detail of a repair arrives instead as the
`edition_published` event the re-driven publish emits, tagged
`trigger=reconcile` — charting that against `trigger=build` shows how
much of an environment's publish traffic is the system healing itself.

### Logs

- A tick that repaired something logs at **warning**: `"Reconciled
  drifted editions"`, carrying the full tally plus
  `republished_editions` and `unpublished_editions` (sorted
  `project/edition` names). These names are not on the queue row on
  purpose — by the time anyone reads that row the re-driven publishes
  have their own rows and the deleted keys are gone, so the log line
  is the one record of what was repaired.
- A tick that changed nothing logs at **debug**: `"No edition drift to
  reconcile"`. That is the steady state for every org, twice an hour.
- Each applied action also logs at info (`"Re-drove a drifted edition
  publish"` with its `reason`, or `"Removed a stranded edition
  pointer"`), tagged `phase="reconcile"`.

### Sentry

A tick that repaired something also sends exactly one warning-level
message per org tick, tagged with `organization`. The title is a
constant, so every drifted org lands in one issue you filter by that
tag:

```
Edition reconciliation repaired drifted editions
```

The counts and the edition names ride in an `edition_reconcile`
context on each event.

One message per org, never one per edition: an org that lost a hundred
keys lost them to one cause, and a message each would bury it. A clean
tick sends nothing: the message is not "the loop ran" but "something
else in the system lost work, and the loop has cleaned up after it".

## `failed` pairs are left for operators

A pair reading `failed` is one whose `publish_edition` job exhausted
its retries or hit a non-retryable error. The loop reports these as
`failed_left_alone` and never re-drives them: re-enqueueing a publish
that has already failed for a reason the publish path considered final
would loop forever and bury the real cause. Deciding whether the cause
is fixed is an operator's call.

To re-drive one by hand:

1. Find the failure.
   `GET /orgs/{org}/jobs?kind=publish_edition&status=failed` lists the
   failed publish jobs, each naming its edition (`edition_url`) and
   carrying the reason in `errors`. Fix whatever that names — a bad
   credential, a deleted build, a CDN namespace misconfiguration —
   before re-driving, or the retry fails the same way. (The pair's own
   `publish_status` is in `edition_build_history` and is not on the
   API; the edition resource's `publish_status` reports only the last
   attempt against the edition as a whole.)
2. Repoint the edition, which re-enqueues the publish:

   ```
   POST /orgs/{org}/projects/{project}/editions/{edition}/rollback
   {"build": "<base32 build id>"}
   ```

   The target may be the build the edition already serves; the
   endpoint requires the `admin` role and the build must appear in the
   edition's history. It records a **fresh** history row for the pair
   in `pending` and enqueues a `publish_edition` job tagged
   `trigger=rollback`.

That fresh row is what takes the pair out of `failed`: the loop reads
the most recent row per pair, so if this publish also stalls, the next
tick sees `stalled_publish` and re-drives it normally. Rolling forward
to a newer build works the same way, and is the better move when the
failed build itself is the problem.

## What the loop deliberately does not do

- Publish or delete a pointer directly (it enqueues, or calls the
  shared unpublish service).
- Re-drive a `failed` pair, or mark any pair `failed` itself.
- Delete the key of a live edition that has no current build
  (`unexpected_pointers`): it cannot republish, since there is nothing
  to point at, and must not delete, since the key is the only
  surviving evidence of whatever wrote it.
- Publish at a soft-deleted or purged build (`retired_build_skipped`),
  which would race the purgatory sweep's reclamation.
- Probe the published URL, or check any CDN cache state. The loop's
  question is only whether the KV pointer names the right build.

## Related

- `src/docverse_server/domain/edition_reconcile.py` — the pure planner
  whose cases this page's decision table mirrors.
- `src/docverse_server/services/edition_reconcile.py` — the per-org
  service that applies a plan.
- `src/docverse_server/worker/functions/edition_reconcile*.py` — the
  dispatcher, job, and reaper.
- `tests/docs_test.py` — fails when this page stops naming every case,
  counter, event field, or knob the code has.
- SQR-112, "Reconciliation loop".
