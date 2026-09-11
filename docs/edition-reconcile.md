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
  are separable from ordinary publish traffic. Each enqueue re-tests
  the pair against current state first (see "The apply-time re-check"
  below).
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

### The apply-time re-check

The decision table above describes an instant that has passed by the
time an action is applied. The plan's read transaction commits before
the CDN read-back, and up to a whole cap's worth of other actions can
run before any one republish reaches the queue. Two things can change
in that window, so each republish is re-tested as the first statement
of the transaction that enqueues it — inside
`enqueue_publish_for_edition`'s Phase A, before it writes anything:

| Re-check | Dropped when | Reported as |
| --- | --- | --- |
| `editions.current_build_id`, read `FOR UPDATE` | the edition no longer points at the build the plan chose | `superseded_skipped` |
| a live `publish_edition` job for the pair (row 6's test, asked of one pair) | another driver enqueued the pair since the snapshot | `in_flight_skipped` |

A dropped action writes nothing at all: the edition is not marked
`pending`, no history row is touched, and no `queue_jobs` row is
created. It is logged at **info** with the edition and both build ids,
and the tick carries on with the org's remaining actions.

The lock is what makes the first check hold. Every repoint on
`editions` — tracking, an API rollback, keeper-sync — goes through an
`UPDATE` of that row, so it waits on the lock rather than slipping
between the check and the queue insert, and the enqueue commits
together with the current build it names. Without it the reconciler
could enqueue a publish of superseded content, and the `EDITION_UPDATE`
advisory lock would not save it: that lock serializes publish jobs by
pickup order, not enqueue order, so the stale job can win and leave the
edge on the old build until the next tick notices.

The second check is a point read, not a mutex. `queue_jobs` has no
partial unique index for `publish_edition` and this loop deliberately
does not add one — publish jobs for a pair are legitimately enqueued by
several drivers, and the reconciler only needs to not be one more.

`superseded_skipped` above zero is normal on a busy org and is not a
failure: it means editions moved while the tick walked them. A steady
trickle argues for a lower `edition_reconcile_max_actions_per_job`, so
each tick's plan is younger when it is applied.

## Configuration

**The Phalanx values below do not exist yet.** The `docverse` chart in
lsst-sqre/phalanx carries none of these three keys until the companion
chart change tracked in
[#620](https://github.com/lsst-sqre/docverse/issues/620) merges, and
Helm accepts an unrecognized key in a values file silently — the
configmap simply never renders the variable, the setting stays at its
default, and nothing warns you. Until #620 lands, the only way to
change any of these settings is the environment variable, set on the
deployment.

<!-- Remove the paragraph above as part of #620, once the chart change
     has merged and these values are real. -->

| Setting | Environment variable | Default | Phalanx value (pending #620) |
| --- | --- | --- | --- |
| `edition_reconcile_enabled` | `DOCVERSE_EDITION_RECONCILE_ENABLED` | `true` | `maintenance.editionReconcileEnabled` |
| `edition_reconcile_max_actions_per_job` | `DOCVERSE_EDITION_RECONCILE_MAX_ACTIONS_PER_JOB` | `100` | `maintenance.editionReconcileMaxActionsPerJob` |
| `edition_reconcile_reaper_threshold_seconds` | `DOCVERSE_EDITION_RECONCILE_REAPER_THRESHOLD_SECONDS` | derived: `maintenance_job_timeout_seconds` + 1800 (`5400` at the stock timeout) | `reaperThresholds.editionReconcileSeconds` |

Notes:

- The feature flag ships **true**, unlike `purgatory_cleanup_enabled`.
  The loop's only action is to enqueue a publish of a build an edition
  already points at, so a wrong tick republishes something that was
  already correct; running with the flag off simply preserves the
  drift. Turn it off when an org has drifted badly enough that you
  want the repair load off the publishing queue while you look at it.
  The cron stays registered either way, so flipping the flag needs no
  worker restart.
- The reaper threshold has no literal default of its own. It derives
  from `maintenance_job_timeout_seconds` — the timeout arq actually
  enforces on the tick — plus a 30-minute margin, which is one full
  `edition_reconcile_reaper` cron gap. Raising the maintenance timeout
  therefore moves the threshold with it. A flat literal would invert
  the moment an operator raised that shared timeout for one of the
  pool's other functions, and the reaper would then fail ticks that
  were still running: the row would leave
  `idx_queue_jobs_edition_reconcile_active_uq`, the next dispatcher
  tick would mint a second reconciler for the same org, and the first
  job's eventual completion would raise `InvalidJobStateError`.
- An explicit `DOCVERSE_EDITION_RECONCILE_REAPER_THRESHOLD_SECONDS`
  wins over the derivation, but the config refuses one that is not
  strictly greater than `maintenance_job_timeout_seconds`. The
  resulting threshold still lands far below the six hours the
  maintenance-pool siblings use, which matters because this is the
  only loop on the pool that ticks twice an hour: at a sibling-sized
  threshold, one wedged row would cost an organization twelve
  consecutive passes.
- To reconcile an org immediately in a test environment, drive the
  threshold and the cron cadence down there rather than reaching for a
  manual entrypoint — there is no CLI for this loop. Because of the
  floor above, driving the threshold down to seconds means driving
  `DOCVERSE_MAINTENANCE_JOB_TIMEOUT_SECONDS` down alongside it.

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
  "superseded_skipped": 0,
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
`in_flight_skipped`, `superseded_skipped`, `failed_left_alone`,
`unexpected_pointers`, `capped`, `cdn_checked`, and `elapsed`.

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
- So does each action the apply-time re-check dropped —
  `"Skipped a superseded edition republish"` or
  `"Skipped an in-flight edition republish"` — both carrying
  `edition_id`, `planned_build_id`, and the `current_build_id` read
  under the lock.

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

A publish job left over from an *earlier* row of the pair cannot undo
the repair. Each `publish_edition` job carries the id of the row it was
enqueued for, so a job that sat on a backed-up queue while the edition
was rolled away and back retires as `superseded_skipped` — completed,
nothing published, no row and no pointer touched — rather than
resolving the pair afresh and writing its outcome, a `failed` included,
over the row a later publish had already carried to `published`.

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
