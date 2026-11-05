# Dashboard build time budget

`dashboard_build` is the default-pool job that renders a project's
dashboard: the landing page (`__dashboard.html`), the 404 page
(`__404.html`), the version switcher (`__switcher.json`), and one
metadata file per live edition (`__editions/{slug}.json`), all uploaded
to the organization's object store. Every edition costs one upload, so
the job's wall clock grows with the number of editions, and a project
with over a thousand of them, such as `ts-xml` (about 1,200), is the
case its time budget has to cover. Two settings bound that budget, and
they move together.

## Settings

| Setting | Environment variable | Default | Phalanx value |
| --- | --- | --- | --- |
| `dashboard_build_job_timeout_seconds` | `DOCVERSE_DASHBOARD_BUILD_JOB_TIMEOUT_SECONDS` | `900` | Not rendered by the chart yet: `config.dashboardBuild.jobTimeoutSeconds` lands with #873 |
| `dashboard_build_reaper_threshold_seconds` | `DOCVERSE_DASHBOARD_BUILD_REAPER_THRESHOLD_SECONDS` | `1800` | `config.reaperThresholds.dashboardBuildSeconds` |

- **The job timeout** wraps `dashboard_build` on the default pool's
  `WorkerSettings`: arq cancels a job that runs past it. Before it was
  a setting the job ran under arq's implicit 300 s default and uploaded
  its files one at a time, about 1.5 to 2 s each from prod, so `ts-xml`
  and `ts-observatory-control` (472 editions) were cancelled on every
  rebuild (#863). The edition files now upload concurrently (see
  [Upload order and transport](#upload-order-and-transport)), so the
  largest projects finish in a minute or two. The 900 s default leaves
  room for a slow object store and sits exactly on
  [the reaper floor](#the-reaper-floor) of the stock threshold.
- **The reaper threshold** is how long a `dashboard_build` row may sit
  `in_progress` before `dashboard_build_reaper`, on the maintenance
  pool at `minute={3, 18, 33, 48}`, fails it. Failing the row frees the
  project's rebuild slot, so
  `POST /orgs/{org}/projects/{project}/dashboard/rebuild` stops
  answering 409. Since every cancel records itself, the reaper is left
  with the rows nothing else can fail: a worker pod OOM-killed
  mid-render, or a job arq lost.

## The reaper floor

The threshold must be at least the timeout plus
`DASHBOARD_BUILD_REAPER_MARGIN_SECONDS` (900). arq has not cancelled a
job until its timeout elapses, so a reaper that fired any earlier would
fail a row whose render is still uploading and free the project's
rebuild slot, letting a second render start uploading over the first.

The margin is one full `dashboard_build_reaper` cron gap, 15 min, the
same rule that sizes the keeper-sync, `edition_reconcile` and
`build_processing` margins at their own reapers' 30 min gaps. It is not
`BUILD_PROCESSING_REAPER_MARGIN_SECONDS`: the dashboard reaper runs
twice as often, and a 1800 s margin would leave no timeout at all under
the 1800 s threshold every deployment runs.

At the stock values the floor is `1800`, exactly the stock threshold,
so the stock threshold admits any `dashboard_build_job_timeout_seconds`
up to `900`. Raising the timeout means raising
`dashboard_build_reaper_threshold_seconds` with it. The threshold keeps
its literal default rather than deriving from the timeout, and
configuration fails at startup, with an error naming the threshold, the
timeout, and the margin, when either value breaks the floor.

To watch the reaper fire in a test environment, drive the timeout down
with the threshold, for example a 60 s timeout and a 960 s threshold,
and put them back afterwards.

## When the timeout fires

A job that reaches the timeout is cancelled by arq, and
`record_cancellation` fails its row at once rather than leaving it for
`dashboard_build_reaper`. The row's `errors` carry `reason`
`job_timeout` and `timeout_seconds` equal to the configured
`dashboard_build_job_timeout_seconds`, so a raised timeout reads back
as the value arq actually enforced; the timeout is also captured in
Sentry. A rolling deploy that interrupts the job records
`worker_shutdown` against the same timeout instead. The full payload,
and how the reason is told apart, is described under
[Cancellation](keeper-sync-budget.md#cancellation) on the keeper-sync
time budget page.

A cancelled render leaves in place whatever it had already uploaded.
The landing page, 404 page and switcher land before any edition file,
so they are current, and only some edition files can be left from the
previous render. The project's next rebuild rewrites every file.

## Upload order and transport

The job renders every artifact before its first upload, then uploads
them in two steps:

1. The landing page, the 404 page and the switcher, one at a time and
   in that order, so all three have landed before any edition file.
2. The edition files, concurrently, with at most
   `keeper_sync_upload_concurrency`
   (`DOCVERSE_KEEPER_SYNC_UPLOAD_CONCURRENCY`, default `32`) in flight.
   The first upload to fail stops the rest, cancels those in flight,
   and fails the job with the object store's own error.

Every upload is a presigned PUT over the worker's copy client, the HTTP
client that keeper-sync's build copies and `build_processing`'s unpacks
upload over, and each PUT holds a slot of the same worker-wide upload
cap. The cap is per worker process, so concurrent `dashboard_build` and
`build_processing` jobs on one default-pool worker split its slots, and
there is no separate dashboard setting to move. The copy client keeps a
connection alive for every slot and gives each attempt a
10 s connect window. [Upload transport](build-processing.md#upload-transport), on
the build-processing page, and
[The copy client](keeper-sync-transport.md#the-copy-client), on the
keeper-sync transport page, describe the client and the cap. Uploads
keep the shared per-object retry budget rather than
`keeper_sync_upload_max_attempts`.

## Sizing the timeout

A render takes about (editions ÷ cap) × 2 s, plus a few seconds for
the three project-level files: about 75 s for `ts-xml` at the stock cap
of 32, where one upload at a time took about 40 min. A render that
overlaps a large `build_processing` unpack on the same worker shares
the cap with it and goes slower. To measure one, request a rebuild,
then read the `dashboard_build` job's `date_started` and
`date_completed` from `GET /orgs/{org}/jobs/{job}`. Check a raised
timeout against [the reaper floor](#the-reaper-floor) before setting
it.
