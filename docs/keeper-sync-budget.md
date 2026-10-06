# Keeper-sync time budget

A `keeper_sync_project` job no longer has to sync a whole LTD product
inside one arq job. It walks the product's editions until its **slice
budget** runs out. Then it completes its `queue_jobs` row and hands the
rest of the walk to a continuation job, so a project of any size
converges across a chain of jobs that each finish well inside the arq
timeout. Every `queue_jobs`-backed worker function also records its own
cancellation now: when arq cancels a job at its timeout, or because the
worker is shutting down, the job fails its row and rolls up its run
instead of leaving both for a reaper.

This page is for operators syncing a large product, or reading a
cancelled job. It covers the budget, timeout and reaper ladder and how
the three numbers derive from each other, the tier crons' own timeouts,
how a chain of slices reads
in `GET /jobs` and on a run, the cursor a chain resumes from and what
the cursor misses, what a cancelled job records and how to tell a
timeout from a deploy, the no-progress guard and the timeout escape
hatch, and the cap on the reaper threshold.

## Why this exists

On 2026-09-24, backfill run `1xes-dmzz-6r4a-96` on roundtable-prod ran
four `keeper_sync_project` jobs (`documenteer`, `safir`, `gafaelfawr`
and `phalanx`) past `keeper_sync_job_timeout_seconds`, slowed by the R2
connect-timeout storm that
[Keeper-sync transport resilience](keeper-sync-transport.md) describes.
arq cancelled each one on schedule at 3600 s. A cancel surfaces inside
the job as `asyncio.CancelledError`, a `BaseException`, so the job's
`except Exception` branch never ran: the four rows stayed
`in_progress`, the run's `pending_count` never reached 0, and
`POST …/keeper-sync/runs` returned 409 for the organization. Only
`keeper_sync_reaper` could free them, and the Phalanx chart pinned its
threshold at 21600 s, so the organization was blocked for six hours
until an operator failed the rows by hand (#699).

The next production milestone made a bigger version of the same
failure certain. `pipelines` lists 2,932 LTD editions and 3,455 builds,
and its `main` build alone is 8,684 objects (369 MB). No single job can
copy all of that inside any reasonable timeout, and a job that walks
every edition or dies would die every time.

PRD #765 fixed both:

- Every `queue_jobs`-backed arq function records its own cancellation
  (see [Cancellation](#cancellation)).
- The keeper-sync reaper threshold is capped at the job timeout plus a
  margin, whatever a deployment sets (see
  [The reaper cap](#the-reaper-cap)).
- `keeper_sync_project` stops at a cooperative budget and continues in a
  new job (see [How a project syncs in slices](#how-a-project-syncs-in-slices)).

## The ladder

Three settings bound how long keeper-sync work runs, and they always
nest: slice budget < job timeout < reaper threshold. The timeout is the
one to set; the other two derive from it.

| Setting | Environment variable | Default | Phalanx value |
| --- | --- | --- | --- |
| `keeper_sync_slice_budget_seconds` | `DOCVERSE_KEEPER_SYNC_SLICE_BUDGET_SECONDS` | derived: timeout − `KEEPER_SYNC_SLICE_MARGIN_SECONDS` (600), so `3000` at the stock timeout | `config.keeperSync.sliceBudgetSeconds` |
| `keeper_sync_job_timeout_seconds` | `DOCVERSE_KEEPER_SYNC_JOB_TIMEOUT_SECONDS` | `3600` | `config.keeperSync.jobTimeoutSeconds` |
| `keeper_sync_reaper_threshold_seconds` | `DOCVERSE_KEEPER_SYNC_REAPER_THRESHOLD_SECONDS` | derived: timeout + `KEEPER_SYNC_REAPER_MARGIN_SECONDS` (1800), so `5400` at the stock timeout | `config.reaperThresholds.keeperSyncSeconds` |

- **The slice budget** is how long one `keeper_sync_project` job keeps
  *starting* editions. It is checked before each edition and never
  during one, so the last edition a slice starts runs past the budget
  by however long its copy takes. The margin between budget and
  timeout, 600 s unless the budget is set explicitly, is the time that
  edition has to finish before arq cancels the job. Below a 900 s
  timeout the derivation is floored at a third of the timeout, so a
  seconds-long test timeout still derives a positive budget. An
  explicit value wins over the derivation, does not follow the timeout
  afterwards, and must be greater than 0 and less than the timeout: a
  budget outside that range fails configuration at startup with an
  error naming both values.
- **The job timeout** wraps `keeper_sync_project` and
  `keeper_sync_run_discovery` on the keeper-sync pool. arq cancels a job
  that runs past it, and with `max_tries=1` never retries it. Raising
  it raises both neighbours with it; it is also the escape hatch for an
  edition too large for one slice (see
  [The timeout escape hatch](#the-timeout-escape-hatch)).
- **The reaper threshold** is how long a keeper-sync row may sit
  `in_progress` (or `queued` after arq lost its job) before
  `keeper_sync_reaper` fails it. Since every cancel now records itself,
  the reaper is left with the rows nothing else can fail: a worker pod
  OOM-killed mid-job, or a job arq lost. An explicit value can lower
  the threshold but never raise it past the derived one (see
  [The reaper cap](#the-reaper-cap)).

The sync worker logs the ladder it runs with once at startup, as
`Keeper-sync time ladder` with all three values. Only that pool logs
it, because it is the pool where all three apply.

The Phalanx chart change that renders `sliceBudgetSeconds` (left unset
by default, so the server derives the budget) and drops the
`keeperSyncSeconds: 21600` pin from the chart's `values.yaml` ships
separately from the server release (#773). Until an environment has it,
the pin is capped at startup and the sync worker logs a warning about
it.

To watch a chain on a mid-size project in a test environment, set an
explicit budget well below the timeout, such as
`sliceBudgetSeconds: 300`. Leave room for the work a slice does before
its first edition (see
[The no-progress guard](#the-no-progress-guard)), and remove the
override afterwards.

### Tier-cron timeouts

The three tier crons that keep projects in step with LTD between runs
sit outside the ladder: they sync nothing themselves and hold no
`queue_jobs` row, they only enqueue `keeper_sync_project` jobs. Each
pass still runs under an arq timeout of its own, which
`tier_cron_timeout` derives from the tier's cron interval: the interval
less a margin of one `TIER_CRON_TIMEOUT_MARGIN_DIVISOR` (6)th of it,
and never less than `TIER_CRON_TIMEOUT_MIN_MARGIN` (one minute). The
result is floored at arq's default, `ARQ_DEFAULT_JOB_TIMEOUT` (300 s),
and capped at the interval itself. None of the three is a setting; each
follows its cadence constant.

| Cron | Interval | Timeout |
| --- | --- | --- |
| `keeper_sync_tier_main` | 5 min (`TIER_MAIN_CRON_INTERVAL`) | 300 s |
| `keeper_sync_tier_discovery` | 30 min (`TIER_DISCOVERY_CRON_INTERVAL`) | 1500 s |
| `keeper_sync_tier_other` | 60 min (`TIER_OTHER_CRON_INTERVAL`) | 3000 s |

Until these timeouts, the crons were registered without one and ran
under arq's default, `ARQ_DEFAULT_JOB_TIMEOUT_SECONDS` (300 s). On
2026-10-06, once `pipelines` (2,938 LTD editions) entered the `rubin`
organization's scope on roundtable-prod, `keeper_sync_tier_discovery`
and `keeper_sync_tier_other` reached it on every tick. A cancelled pass
starts again from the top of the scope on its next tick, so the
projects at the tail of the scope were never visited.

The floor is for `keeper_sync_tier_main`, the one tier whose cadence is
no longer than arq's default. Its interval less the one-minute margin
would leave 240 s, a fifth less than the 300 s it ran under before, and
a pass that needed the difference would be cancelled, and start again
from the top of its scope, on every tick. The cap keeps a tier whose
cadence is shorter than the floor, should one be added, from running a
pass that started on time into its own next tick.

The timeout bounds a pass. For `keeper_sync_tier_discovery` and
`keeper_sync_tier_other` the margin also keeps a pass that started a
little late, because the pool's job slots were all busy at the tick,
from running into the same tier's next tick in the common case, with
room for the cleanup a cancelled pass runs on its way out. It cannot
rule an overlap out: arq measures the timeout from when a pass starts,
not from its tick, so a pass that started late on a saturated pool can
still be running when the next tick's pass starts, and
`keeper_sync_tier_main`, held at the floor, has no margin at all. Two
passes that overlap can each try to enqueue the same project's sync;
the per-project active-job slot lets only one `keeper_sync_project` job
be active, and the other pass logs that it skipped the project (see
[Log lines](#log-lines)). The sync worker registers each tier's cron
job, and the same function on the pool's `functions` list, with the
derived value.

A cancelled pass has no row to fail, so it logs one warning,
`Keeper-sync tier pass cancelled` (see [Log lines](#log-lines)). The
line names the tier, the `reason` (inferred the way a job's row is; see
[Timeout or deploy](#timeout-or-deploy)), how long the pass ran, and
how many orgs it finished. For the org in flight it gives
`projects_visited` of `projects_total` and the `ltd_slug` the pass was
on. A cancel that lands while a pass is handing a project job to arq
also fails that job's row (see
[Which jobs record their cancellation](#which-jobs-record-their-cancellation)).

## How a project syncs in slices

Every `keeper_sync_project` job is one slice of its project's sync,
whether a backfill run, a tier cron or `POST …/refresh` started the
chain. A project small enough to finish inside the budget, as most
are, is a chain of one slice.

1. The job starts its budget, plans its walk, and visits editions in
   order: the `main` edition first, whatever order LTD lists editions
   in, so the first slice of a chain syncs and publishes the default
   edition before anything else; then the rest in LTD's order.
2. Before each edition it checks the budget. Once the budget has run
   out, the walk stops there and the slice ends.
3. A slice that visited at least one edition, and stopped at the
   budget, continues. In **one** transaction it records its position on
   its row's `progress`, completes the row (`completed`, or
   `completed_with_errors` when the slice isolated an edition failure),
   creates the continuation's `queue_jobs` row, and rolls up the parent
   run. The continuation carries the same organization, kind,
   `subject_label` and `keeper_sync_run_id`, so a run's chain stays
   attributed to the run while a tier cron's or a refresh's chain stays
   unattributed.
4. With no transaction open, the slice enqueues the continuation's arq
   job, with the same payload plus the cursor
   (`resume_after_ltd_edition_id`) and `slice_index` advanced by one,
   and then stamps the arq job id on the continuation's row.
5. A slice that walked to the end of LTD's list ends the chain: it
   completes its row and logs the usual `Keeper-sync project completed`
   line.

Completing the old row before creating the new one is what lets the
insert pass `idx_queue_jobs_keeper_sync_project_active_uq`, the partial
unique index that allows one active `keeper_sync_project` row per
project. Doing both in one transaction means the project's slot never
looks free between slices. A tier cron that ticks mid-chain logs a
`Skipping keeper_sync_project enqueue` line (see
[Log lines](#log-lines)) and moves on, a run's discovery skips the slug
the same way, and `POST …/refresh` for the project returns 409 until
the chain ends.

The other per-project guards apply to each slice on its own.
`MAX_CONSECUTIVE_EDITION_FAILURES` consecutive edition failures still
fail the slice outright, and so does a slice that ends with failures
and imported nothing. A slice that stopped at its budget before
attempting any edition has no failures, so it never counts as such an
outage (see [The no-progress guard](#the-no-progress-guard)). A slice
whose isolated edition failures stayed under the threshold completes
`completed_with_errors`, with the failures on its `progress`, and still
continues.

A crash between the commit in step 3 and the stamp in step 4 leaves
the continuation's row `queued` with no arq job, the same orphan the
tier crons and run discovery can leave, and the existing orphan sweeps
fail it. A *cancel* in that window is recorded at once instead (see
[Cancellation](#cancellation)).

## Reading a chain

### The job rows

Each slice is its own row in
`GET /orgs/{org}/jobs?kind=keeper_sync_project`, newest first, with the
LTD slug as its `subject_label`. For a run's chain, add
`&run=<run id>`. A tier cron's or a refresh's chain belongs to no run,
and the listing has no `subject_label` filter, so either read
`subject_label` off the listing or start from one slice and follow
`continuation_job_id` with `GET /orgs/{org}/jobs/{job}`.

Every slice records where its walk stood on its row's `progress`:

| Key | Present | Meaning |
| --- | --- | --- |
| `slice_index` | every slice | Position of the slice in its chain, from 0. |
| `editions_total` | every slice | How many editions LTD lists for the product. `null` on a slice cancelled before it fetched the list. |
| `editions_visited` | every slice | Editions this slice dealt with: synced, failed, or tombstoned by the lifecycle pass. Editions skipped at the cursor are not counted. |
| `editions_remaining` | every slice | Editions of this slice's walk it did not reach, which a continuation picks up. `0` once a slice has walked to the end of LTD's list. `null` with `editions_total`. |
| `last_visited_ltd_edition_id` | every slice | LTD id of the last edition the slice visited, which is the next slice's cursor. `null` on a slice that visited nothing. |
| `continued` | a slice that ended normally | `true` when the slice enqueued a continuation, `false` when it ended the chain. |
| `continuation_job_id` | a continued slice | Public id of the next slice's job. |
| `reason` | a no-progress slice | `no_progress`: the budget ran out before the slice visited anything (see [The no-progress guard](#the-no-progress-guard)). |

A slice that isolated edition failures also carries `message`,
`edition_failure_count` and `edition_failures` on the same `progress`,
as an unsliced project sync always has. A cancelled slice carries the
five position keys but not `continued`.

A middle slice of a `pipelines` chain reads like this (the values are
illustrative):

```json
{
  "slice_index": 2,
  "editions_total": 2932,
  "editions_visited": 412,
  "editions_remaining": 1708,
  "last_visited_ltd_edition_id": 18234,
  "continued": true,
  "continuation_job_id": "1xgk-4f7m-a2qe-31"
}
```

Each slice of a healthy chain is `completed` with `continued: true`
until the last, which is `completed` (or `completed_with_errors`) with
`continued: false` and `editions_remaining: 0`. The slices partition
the walk: in a chain that started from the top, synced `main` in its
first slice, and saw no change to LTD's list, the slices'
`editions_visited` sum to `editions_total`.

### On the run

A run's counters (`total_count`, `pending_count`, `succeeded_count`
and `failed_count`) count every `queue_jobs` row attributed to the run:
the discovery job, each project's slices, and the `publish_edition` jobs
the slices enqueue. So a chain adds one to `total_count` per
continuation, and a run that synced a large project reports more jobs
than it had projects.

`pending_count` never reaches 0 in the middle of a chain, because each
continuation is created in the transaction that completes its
predecessor. The run therefore stays `in_progress` until the chain's
last slice is terminal, and finalises exactly once.
`date_last_activity` moves with every slice. A slice that completed
`completed_with_errors`, whether from isolated edition failures or the
no-progress guard, counts in `failed_count`, so the run finalises
`partial_failure`; so does a slice that was cancelled or failed.

## The cursor rule

A continuation resumes after its predecessor's
`last_visited_ltd_edition_id`, which arrives in its payload as
`resume_after_ltd_edition_id`. The walk order is `main` first and then
LTD's order, so the cursor is a position in that order:

- A resumed slice fetches only LTD's list of edition URLs up front. It
  drops every edition up to and including the cursor on the strength
  of its URL alone, with no LTD call for the edition or its builds,
  then fetches each remaining edition before it starts visiting them.
- `main` is placed at the front from its `keeper_sync_state` row rather
  than from LTD, since an edition URL does not say which edition is
  `main`. Once an earlier slice has synced `main`, it sits before every
  cursor and is never visited again in the same chain.
- A cursor that LTD no longer lists (the edition was deleted mid-chain)
  gives no position to resume from, so the slice walks from the top and
  logs a "Resume cursor is no longer in LTD's edition list" line.
  Editions already synced cost about two LTD calls each on that walk,
  because a revisit short-circuits when LTD's `date_rebuilt` matches
  the state row's `date_rebuilt_seen`.

### What the cursor does not catch

A chain is not a snapshot of LTD. Editions LTD adds at the tail of its
list after the cursor are reached by a later slice. A change to an
edition *before* the cursor is not: a rebuild of an edition an earlier
slice already synced, or a new edition LTD lists ahead of the cursor,
waits for the tier crons, which pick changes up whatever a chain did:

- `keeper_sync_tier_main` (every 5 minutes) re-syncs a project whose
  `main` edition LTD has rebuilt since it was last seen.
- `keeper_sync_tier_discovery` (every 30 minutes) re-syncs a project
  with an edition that has no state row yet.
- `keeper_sync_tier_other` (hourly) re-syncs a project with a stale
  non-`main` edition.

Each of those starts a new chain from the top once the current chain
has ended and freed the project's slot. Until then, it skips the
project.

## Cancellation

arq runs each job in its own task under `asyncio.wait_for`, so the
job's timeout cancels that task, and so does a worker shutdown: the
SIGTERM of a rolling deploy or a deleted pod. Either way the job sees
`asyncio.CancelledError` at whatever it was awaiting. A worker function
wrapped in `record_cancellation` then:

1. Opens a fresh database session, since the job's own session may be
   mid-rollback from the very cancel being recorded.
2. In one transaction, fails the row if it is still active, with the
   [errors payload](#the-errors-payload) below; merges whatever progress
   the job exposes (for a `keeper_sync_project` slice, its five position
   keys); and closes out what the job owned besides its row, such as
   rolling up its keeper-sync run.
3. Publishes `keeper_sync_run_completed` when that roll-up finalised a
   run.
4. Logs `Queue job cancelled`, and captures a Sentry event for a
   `job_timeout` only.
5. Re-raises the `CancelledError`, so arq records the job as failed.

A row that already went terminal (a reaper reached it first, or the job
completed its row before a best-effort tail step was cancelled) keeps
the status it earned, and nothing else happens. `publish_edition`
relies on that: it completes its row *before* the CDN purge, so a
cancel during the purge costs only the purge. A `publish_edition`
cancelled before then leaves its edition's `publish_status` at
`publishing`, which `edition_reconcile` re-drives as `stalled_publish`.

A cancelled `keeper_sync_project` slice fails its row with its position
on `progress`, and its run finalises `partial_failure` once nothing
else is pending. The cancel path never enqueues a continuation: the
project's slot is free as soon as the row fails, so the tier crons
re-drive whatever the slice did not reach, or an operator can
`POST …/refresh` the project at once.

An OOM kill gives the process no chance to run any of this, so a row
orphaned that way is still the reaper's to fail (see
[The reaper cap](#the-reaper-cap)).

### The errors payload

| Key | Value |
| --- | --- |
| `type` | Always `CancelledError`. |
| `reason` | `job_timeout` or `worker_shutdown` (see [Timeout or deploy](#timeout-or-deploy)). |
| `message` | A sentence saying which, with the timeout in seconds. |
| `elapsed_seconds` | How long the job had run when it was cancelled, to 0.1 s: `now()` less the row's `date_started`, both on the database's clock. For a hand-off cancel, the time since the creating job started. |
| `timeout_seconds` | The per-job timeout arq enforces on the function (on the creating job, for a hand-off cancel), which the elapsed time is compared against. |
| `job_function` | Hand-off cancels only: the arq function whose cancel stranded this row. |

### Timeout or deploy

`reason` is `job_timeout` when `elapsed_seconds` has reached
`timeout_seconds`, less `TIMEOUT_REASON_SLACK` (5 s), and
`worker_shutdown` otherwise. The slack absorbs the gap between arq
starting its clock and the job stamping `date_started` at pickup, so a
real timeout is never recorded as a shutdown. The cost is that a deploy
landing in the last few seconds of a job's allowance reads as a
timeout.

At the stock keeper-sync timeout, a `keeper_sync_project` row that ran
out of time reads:

```json
{
  "type": "CancelledError",
  "reason": "job_timeout",
  "message": "arq cancelled the job at its 3600 s timeout",
  "elapsed_seconds": 3598.7,
  "timeout_seconds": 3600.0
}
```

and one a deploy interrupted reads:

```json
{
  "type": "CancelledError",
  "reason": "worker_shutdown",
  "message": "arq cancelled the job before its 3600 s timeout; the worker shut down",
  "elapsed_seconds": 812.4,
  "timeout_seconds": 3600.0
}
```

A `worker_shutdown` is routine: it happens on every rolling deploy,
and it is only logged. A `job_timeout` is the signal an operator needs,
so it is also captured in Sentry as a warning titled
`Queue job cancelled at its arq timeout`. Every timeout groups under
that one issue, with `job_function` and `queue_name` tags and the
job's public id, reason, times and progress in the event's
`queue_job_cancellation` context. To confirm a `worker_shutdown`, look
for the pod restart or rollout at the row's `date_completed`.

For a sliced `keeper_sync_project`, a `job_timeout` most likely means
an edition the slice started inside its budget was still copying when
the margin ran out (see
[The timeout escape hatch](#the-timeout-escape-hatch)).

### Which jobs record their cancellation

`record_cancellation` wraps a job that holds an `in_progress` row of
its own. `record_handoff_cancellation` covers the other window a cancel
can strand a row in: between a job committing a *child* row and
stamping the child's arq job id. It fails each such child still
`queued` with no arq job id, with the same payload plus `job_function`,
instead of leaving it, and any active-job slot it holds, to the orphan
sweeps. A function the arq settings register without a timeout of its
own runs under arq's default, `ARQ_DEFAULT_JOB_TIMEOUT_SECONDS` (300 s),
and its reason is read against that. The tier crons run under their
tier's `tier_cron_timeout` instead (see
[Tier-cron timeouts](#tier-cron-timeouts)).

| Function | Pool | Timeout | Helper | Also closes out |
| --- | --- | --- | --- | --- |
| `build_processing` | default | `ARQ_DEFAULT_JOB_TIMEOUT_SECONDS` | `record_cancellation` | Fails its build, if the build is not already terminal. |
| `dashboard_build` | default | `ARQ_DEFAULT_JOB_TIMEOUT_SECONDS` | `record_cancellation` | Nothing. |
| `dashboard_sync` | default | `ARQ_DEFAULT_JOB_TIMEOUT_SECONDS` | `record_cancellation` | Marks its GitHub binding's last sync failed. |
| `publish_edition` | default | `publish_edition_job_timeout_seconds` | `record_cancellation` | Rolls up its keeper-sync run, if it has one. |
| `keeper_sync_project` | keeper-sync | `keeper_sync_job_timeout_seconds` | both | Records its slice position and rolls up its run, if it has one; the hand-off covers its continuation. |
| `keeper_sync_run_discovery` | keeper-sync | `keeper_sync_job_timeout_seconds` | `record_cancellation` | Fails its run outright, since a part-finished fan-out has no counters to roll up. |
| `keeper_sync_tier_main` | keeper-sync | `tier_cron_timeout` (300 s) | `record_handoff_cancellation` | The project jobs it enqueues. |
| `keeper_sync_tier_discovery` | keeper-sync | `tier_cron_timeout` (1500 s) | `record_handoff_cancellation` | The project jobs it enqueues. |
| `keeper_sync_tier_other` | keeper-sync | `tier_cron_timeout` (3000 s) | `record_handoff_cancellation` | The project jobs it enqueues. |
| `edition_reconcile` | maintenance | `maintenance_job_timeout_seconds` | `record_cancellation` | Nothing; the row is the tick's whole record. |
| `git_ref_audit` | maintenance | `maintenance_job_timeout_seconds` | `record_cancellation` | Rolls up its audit run. |
| `lifecycle_eval` | maintenance | `maintenance_job_timeout_seconds` | `record_cancellation` | Rolls up its lifecycle run. |
| `purgatory_cleanup` | maintenance | `maintenance_job_timeout_seconds` | `record_cancellation` | Nothing; the builds it already purged are stamped, and the next tick resumes. |
| `project_github_resolve` | maintenance | `ARQ_DEFAULT_JOB_TIMEOUT_SECONDS` | `record_handoff_cancellation` | The `dashboard_sync` jobs it enqueues. |

Every function that uses either helper is decorated with
`cancellation_recorded`, and `tests/worker/cancellation_coverage_test.py`
reads that marker off what the three `WorkerSettings` classes register.
A new worker function fails that test until it is either wrapped or
listed as deliberately unwrapped with its reason. Left unwrapped today:
`ping`, the queue-stats and `inventory_census` gauges, the reapers, and
the four maintenance dispatchers, whose stranded rows go to the
matching reaper's orphan sweep.

The cleanup never raises. If recording a cancel fails (the database is
unreachable, say), the helper logs the traceback as
`Failed to record the queue job's cancellation` and still re-raises
the cancel, and the row is left to the reaper.

## The no-progress guard

A slice that reached its budget without visiting a single edition
beyond the cursor does not continue. It completes
`completed_with_errors`, records `continued` as `false` and `reason`
as `no_progress` on its `progress`, and logs a warning:
`Keeper-sync slice made no progress within its budget; not continuing`.
A continuation would only stop at the same place, so without the guard
a chain could loop forever.

The budget is checked before each edition, so a slice visits nothing
only when the work before its first edition took the whole budget:
fetching the product, listing and fetching every edition after the
cursor, and the lifecycle pass. That work grows with the project (for
the first slice of a `pipelines` chain it is close to 3,000 LTD calls),
so this is what an explicit budget set too small for the project runs
into. The parent run finalises `partial_failure` once nothing else is
pending. The next chain, whether a tier cron or an operator starts it,
runs into the same wall until the slice has more time (see
[The timeout escape hatch](#the-timeout-escape-hatch)).

### The timeout escape hatch

Two cases need more time than a slice has:

- **The work before the first edition outgrows the budget.** The guard
  above ends the chain each time. Raise `keeper_sync_slice_budget_seconds`
  if it was set explicitly, or remove the override; otherwise raise
  `keeper_sync_job_timeout_seconds`, which raises the derived budget
  with it.
- **One edition takes longer to copy than the margin.** An edition
  started near the end of a slice runs past the timeout and is
  cancelled as a `job_timeout`. The project's next chain starts from
  the top and short-circuits through the editions already synced, so
  it reaches that edition earlier in its first slice, with more of the
  timeout left. If the copy takes longer than the whole timeout, every
  attempt is cancelled the same way: raise
  `keeper_sync_job_timeout_seconds`. The budget and the reaper threshold
  follow it, so the ladder keeps its shape.

There is no per-project timeout: raising the timeout raises it for every
keeper-sync job on the pool.

## The reaper cap

The effective `keeper_sync_reaper_threshold_seconds` is the smaller of
the configured value and `keeper_sync_job_timeout_seconds` +
`KEEPER_SYNC_REAPER_MARGIN_SECONDS`. arq has cancelled any keeper-sync
job by its timeout, and the margin is one full `keeper_sync_reaper` cron
gap (the reaper runs at :00 and :30), so a row still `in_progress` past
the cap is dead, and waiting longer only keeps the project behind its
active-job slot and the organization's next run 409-blocked.

At the stock 3600 s timeout, the Phalanx pin of `21600` that held
roundtable-prod for six hours becomes `5400`. A value at or below the
cap, such as `4000`, is left alone, so an environment can still drive
the reaper down to seconds for testing. When the cap rewrote a value,
the sync worker logs one warning at startup,
`Keeper-sync reaper threshold capped at the job timeout plus margin`,
with `requested_seconds`, `effective_seconds`, `job_timeout_seconds`
and `margin_seconds`. Seeing it means the deployment's value no longer
applies and should be dropped. The API and the other pools apply the
same cap without logging it.

With the cap, a keeper-sync row orphaned by an OOM kill is reaped no
later than 120 minutes after it started at the stock timeout (the
first reaper tick past 90 minutes), however high the deployment sets
the threshold. The cap covers only the keeper-sync threshold; the other
reapers keep their own.

## Log lines

The lines this page refers to, from the slice budget, the chain, the
cap, the cancellation helpers and the tier crons. Every line a `keeper_sync_project`
slice writes, `Queue job cancelled` included, also carries the
`slice_index` the job binds on its logger, along with `org`, `run_id`
and `ltd_slug`.

| Message | Level | Fields |
| --- | --- | --- |
| `Keeper-sync time ladder` | info | `slice_budget_seconds`, `job_timeout_seconds`, `reaper_threshold_seconds` |
| `Keeper-sync reaper threshold capped at the job timeout plus margin` | warning | `requested_seconds`, `effective_seconds`, `job_timeout_seconds`, `margin_seconds` |
| `Project sync stopped at its slice budget` | info | `editions_total`, `editions_visited`, `editions_remaining`, `last_visited_ltd_edition_id`, `project_id`, `project`, `ltd_slug` |
| `Keeper-sync slice budget reached; continuing in a new job` | info | `editions_visited`, `editions_remaining`, `continuation_job_id`, `restamped_edition_count`, `edition_failure_count` |
| `Keeper-sync slice made no progress within its budget; not continuing` | warning | `editions_total`, `editions_remaining`, `resume_after_ltd_edition_id`, `slice_budget_seconds` |
| `Keeper-sync project completed` | info | `restamped_edition_count` |
| `Keeper-sync project completed with edition failures` | warning | `restamped_edition_count`, `edition_failure_count`, `failed_ltd_edition_slugs` |
| `Resume cursor is no longer in LTD's edition list; walking from the top` | info | `resume_after_ltd_edition_id`, `ltd_slug` |
| `Skipping keeper_sync_project enqueue: an active job for this project already exists` | info | `org`, `ltd_slug`, `tier` |
| `Skipping keeper_sync_project enqueue: an active job for this project already exists` | info | `org`, `ltd_slug`, `source` |
| `Queue job cancelled` | warning | `queue_job_id`, `queue_job_kind`, `reason`, `elapsed_seconds`, `timeout_seconds`, `progress` |
| `Queue job cancelled` | warning | `job_function`, `reason`, `elapsed_seconds`, `timeout_seconds`, `orphaned_queue_job_ids`, `orphaned_queue_job_kinds` |
| `Failed to record the queue job's cancellation` | error | `exception` |
| `Keeper-sync tier pass cancelled` | warning | `tier`, `reason`, `elapsed_seconds`, `timeout_seconds`, `orgs_completed`, `orgs_total`, `org`, `projects_visited`, `projects_total`, `ltd_slug` |

- `Keeper-sync time ladder` and the cap warning are logged once, at the
  sync worker's startup.
- `Project sync stopped at its slice budget` comes from the sync
  service as the walk stops; the worker's "slice budget reached" line
  follows once the continuation is enqueued. Only the slice that walks
  to the end of LTD's list logs one of the two
  `Keeper-sync project completed` lines.
- The two `Skipping keeper_sync_project enqueue` rows are a tier cron
  (with `tier`) and a run's discovery (with `source`) finding the
  project's slot taken, which is what they see for the whole of a
  chain.
- The two `Queue job cancelled` rows are the two helpers: the first
  fails the job's own row, the second fails rows a cancel stranded on
  their way to arq (see [Cancellation](#cancellation)).
- `exception` is the traceback that Safir's production log profile
  renders from `logger.exception`.
- `Keeper-sync tier pass cancelled` is a tier cron's whole record of a
  cancel (see [Tier-cron timeouts](#tier-cron-timeouts)). `orgs_total`
  and `projects_total` are null if the cancel landed before the pass
  listed its orgs, or resolved the in-flight org's scope, and `org` and
  `ltd_slug` are null between orgs.

## What the slicing deliberately does not do

- **Split a project into per-edition jobs, or resume a copy mid-build.**
  A slice is a span of whole editions, and an edition's copy always
  runs to the end inside one job.
- **Continue from a cancel.** A cancelled slice fails its row and nothing
  more; the tier crons re-drive the project from the top.
- **Add a job status for slices.** A slice that handed off is
  `completed`, and `progress` says it continued.
- **Set a timeout per project.** The no-progress guard and the pool-wide
  timeout cover the one-edition-too-big case.
- **Recover OOM-orphaned rows faster than the reaper.** The capped
  threshold bounds how long that takes; the sync worker's memory growth
  itself is #751 (see [Memory diagnostics](memory-diagnostics.md)).

## Related

- [Scoping the keeper sync](keeper-sync-scope.md), which covers the
  runs and waves whose project syncs this page slices.
- [Keeper-sync transport resilience](keeper-sync-transport.md), which
  covers what a slow copy looks like before it reaches the timeout.
- `src/docverse_server/config.py`: the three ladder settings and both
  margins.
- `src/docverse_server/services/keeper_sync/budget.py`: `SliceBudget`
  and `SliceProgress`.
- `src/docverse_server/services/keeper_sync/service.py`:
  `sync_project`'s budget and cursor, and how a resumed walk is planned.
- `src/docverse_server/services/keeper_sync/scheduler.py`: the tier
  cron intervals and `tier_cron_timeout`.
- `src/docverse_server/worker/functions/keeper_sync.py`:
  `keeper_sync_project`, the continuation hand-off and the no-progress
  guard.
- `src/docverse_server/worker/functions/_cancellation.py`: both
  cancellation helpers and the `cancellation_recorded` marker.
- `src/docverse_server/worker/main.py`: the startup ladder line and the
  cap warning.
- `tests/docs_test.py`: fails when this page stops matching the ladder's
  settings and defaults, the tier-cron timeouts, the progress keys a
  slice records, the errors payload, the functions that record their
  cancellation, or the log lines tabulated above.
- SQR-112, PRD #765 and #699.
