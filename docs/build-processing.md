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
  [Sizing the timeout](#sizing-the-timeout)). How long a build takes
  to unpack also depends on the worker-wide upload cap its uploads
  share with keeper-sync copies, `keeper_sync_upload_concurrency` (see
  [Upload transport](#upload-transport)).
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
`unpacking`. Measure with the development environment's worker on the
same `keeper_sync_upload_concurrency` as the target's, since the upload
cap sets how fast `unpacking` goes (see
[Upload transport](#upload-transport)). Check the result against
[the reaper floor](#the-reaper-floor) before setting it.

## Upload transport

`build_processing` uploads every file it unpacks with a presigned PUT
over the worker's copy client, the HTTP client keeper-sync's build
copies upload over, and each PUT holds a slot of the same worker-wide
upload cap, `keeper_sync_upload_concurrency`
(`DOCVERSE_KEEPER_SYNC_UPLOAD_CONCURRENCY`, default `32`). Each job
also fans its own uploads out no wider than the cap. The cap is per
worker process: every job in the process that uploads build content
shares it, so concurrent `build_processing` jobs on one default-pool
worker split its slots, as concurrent keeper-sync jobs on the sync
worker do. The copy client keeps a connection alive for every slot and
gives each attempt a 10 s connect window, so a large build reuses its
connections to R2 instead of re-dialling them through the burst.
[The copy client](keeper-sync-transport.md#the-copy-client), on the
keeper-sync transport page, describes the client, the cap, and their
constants.

Before this, each job PUT 50 files at a time over the worker's shared
client, whose 5 s connect timeout and 20 kept-alive connections could
not carry the burst: a pipelines daily of about 8,600 files failed in
40 s, every one of its first 50 PUTs timing out connecting to R2.

Uploads keep the shared per-object retry budget rather than
`keeper_sync_upload_max_attempts`: the job uploads inside the database
transaction that then completes the build, and the keeper-sync budget's
longer waits are meant for a copy that holds none.

The cap moves the job's wall clock, so a timeout sized from a measured
upload holds only at the cap it was measured at. Re-measure after
changing `keeper_sync_upload_concurrency`, which also has to stay
inside the node's NAT port allocation (see the Phalanx note under
[Configuration](keeper-sync-transport.md#configuration) on the
keeper-sync transport page).
