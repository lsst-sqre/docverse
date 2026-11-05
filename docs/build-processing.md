# Build processing time budget

`build_processing` is the default-pool job that turns an upload into a
build: it downloads the uploaded tarball from the staging store,
unpacks every file into the organization's object store, marks the
build completed, and enqueues the publishes for the editions that track
it. Its wall clock grows with the size of the build, so a large
documentation site (thousands of files and hundreds of megabytes) is
the case its time budget has to cover. Two settings bound that budget,
and they move together.

## Settings

| Setting | Environment variable | Default | Phalanx value |
| --- | --- | --- | --- |
| `build_processing_job_timeout_seconds` | `DOCVERSE_BUILD_PROCESSING_JOB_TIMEOUT_SECONDS` | `300` | `config.buildProcessing.jobTimeoutSeconds` |
| `build_processing_reaper_threshold_seconds` | `DOCVERSE_BUILD_PROCESSING_REAPER_THRESHOLD_SECONDS` | `28800` | `config.reaperThresholds.buildProcessingSeconds` |

- **The job timeout** wraps `build_processing` on the default pool's
  `WorkerSettings`: arq cancels a job that runs past it. The default is
  arq's own implicit 300 s, which the job ran under before it was a
  setting, so a deployment that sets nothing behaves as it did. It is
  the one to raise when a deployment's largest builds outgrow it (see
  [Sizing the timeout](#sizing-the-timeout)).
- **The reaper threshold** is how long a `build_processing` row may sit
  `in_progress` before `build_processing_reaper`, on the maintenance
  pool at `minute={12, 42}`, fails it, along with the build left in
  `processing` under it. Since every cancel records itself, the reaper
  is left with the rows nothing else can fail: a worker pod OOM-killed
  mid-unpack, or a job arq lost.

## The reaper floor

The threshold must be at least the timeout plus
`BUILD_PROCESSING_REAPER_MARGIN_SECONDS` (1800). arq has not cancelled
a job until its timeout elapses, so a reaper that fired any earlier
would fail a row whose job is still unpacking, and its stranded-build
sweep would then fail the build under it while the upload carried on.
The margin is one full `build_processing_reaper` cron gap, the same
size as the keeper-sync and `edition_reconcile` reaper margins.

At the stock values the floor is `2100`, so the stock 8 h threshold
admits any `build_processing_job_timeout_seconds` up to `27000`.
Raising the timeout past that means raising
`build_processing_reaper_threshold_seconds` with it. Unlike the
keeper-sync threshold the reaper threshold does not derive from the
timeout: it keeps its literal default, and configuration fails at
startup, with an error naming the threshold, the timeout, and the
margin, when either value breaks the floor.

To watch the reaper fire in a test environment, drive the timeout down
with the threshold, for example a 60 s timeout and a 1860 s threshold,
and put them back afterwards.

## When the timeout fires

A job that reaches the timeout is cancelled by arq, and
`record_cancellation` fails its row and its build at once, as an error
would, rather than leaving both for `build_processing_reaper`. The
row's `errors` carry `reason` `job_timeout` and `timeout_seconds` equal
to the configured `build_processing_job_timeout_seconds`, so a raised
timeout reads back as the value arq actually enforced; the timeout is
also captured in Sentry. A rolling deploy that interrupts the job
records `worker_shutdown` against the same timeout instead. The full
payload, and how the reason is told apart, is described under
[Cancellation](keeper-sync-budget.md#cancellation) on the keeper-sync
time budget page. Either way the build ends `failed`, and uploading it
again creates a new build.

## Sizing the timeout

Size the timeout from a measured upload of the largest build the
deployment takes, not from an estimate. Upload it to a development
environment, read the `build_processing` job's `date_started` and
`date_completed` from `GET /orgs/{org}/jobs/{job}`, and set the timeout
to about twice that wall clock. Its `phase` moves through `unpacking`,
`edition_tracking`, and `complete`; nearly all of the time is in
`unpacking`. Check the result against
[the reaper floor](#the-reaper-floor) before setting it.
