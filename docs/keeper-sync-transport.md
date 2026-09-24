# Keeper-sync transport resilience

A keeper-sync build copy reads every object of one LTD build from the
`lsst-the-docs` S3 bucket and PUTs it into the organization's R2 bucket
through a presigned URL: thousands of downloads and PUTs for a large
project, and up to 80 in flight at once on one sync worker at the stock
settings. This page covers how a copy survives either end, R2 or the
LTD bucket, being briefly unreachable, what each layer of retry costs,
and how to read the `build_content_copied` metrics event that reports
every copy.

It is for operators running a sync campaign, the waves
[Scoping the keeper sync](keeper-sync-scope.md) describes, and for
anyone reading a copy failure in Sentry or on a job's `progress`.

## Why this exists

During the `sqr-` wave on roundtable-prod on 2026-09-22, an R2 connect
outage of about 40 s (19:51:13 to 19:51:53 UTC) failed 38 of 208
keeper-sync jobs on `httpx.ConnectTimeout`. Every one had spent the
shared four-attempt upload budget, which rode out only 18.5 s of outage
behind the shared HTTP client's 5 s connect timeout, and nothing above
the object retried the build. PRD #685 put four layers between an R2
outage and a failed edition.

## Transport resilience

The layers, innermost first:

| Layer | Where it lives | What it absorbs | Tuned by |
| --- | --- | --- | --- |
| Per-object upload retry | `S3ObjectStore`'s presigned PUT, through the shared `retry_request` loop | A connect stall, a dropped connection, or a `429`/`5xx`, for about 65 s per object | `keeper_sync_upload_max_attempts`, `keeper_sync_upload_max_backoff_seconds` |
| Dedicated copy client | `worker/main.py` | Pool waits that used to surface as upload timeouts; gives each attempt a 10 s connect window | Constants, sized from `keeper_sync_upload_concurrency` |
| Build-level retry | `KeeperSyncService.sync_build` | A transport outage on either end of the copy: an R2 outage that outlasts one object's whole budget, or the LTD bucket timing out or dropping a connection during a download. One re-run of the copy | `keeper_sync_copy_retry_delay_seconds` |
| Later syncs | The tier crons, `POST .../refresh`, a backfill run | Anything longer (see [When both retries fail](#when-both-retries-fail)) | None |

Only the keeper-sync worker's copy path uses the first three. The API
process, and every other object store a worker opens, keep the shared
retry defaults and the shared HTTP client.

The first two layers are on the R2 side only. The per-object budget
applies to uploads, and LTD downloads go through aiobotocore rather
than either HTTP client. An LTD download has no per-object budget of
Docverse's: beneath the build-level retry there is only botocore's own
retry of the request, in its default `legacy` mode (up to five
attempts, with a randomized backoff of a few seconds), and a
connection that drops while the body is streaming is not retried at
that level at all. The build-level retry is the one layer that covers
both ends.

## Configuration

| Setting | Environment variable | Default | Allowed |
| --- | --- | --- | --- |
| `keeper_sync_upload_max_attempts` | `DOCVERSE_KEEPER_SYNC_UPLOAD_MAX_ATTEMPTS` | `6` | at least 1 |
| `keeper_sync_upload_max_backoff_seconds` | `DOCVERSE_KEEPER_SYNC_UPLOAD_MAX_BACKOFF_SECONDS` | `30.0` | at least 0 |
| `keeper_sync_upload_concurrency` | `DOCVERSE_KEEPER_SYNC_UPLOAD_CONCURRENCY` | `32` | at least 1 |
| `keeper_sync_copy_retry_delay_seconds` | `DOCVERSE_KEEPER_SYNC_COPY_RETRY_DELAY_SECONDS` | `30.0` | at least 0 |

- `keeper_sync_upload_max_attempts` counts the first attempt, so `1`
  means "upload once, never retry". A value below the minimum fails
  configuration at startup rather than degrading silently.
- `keeper_sync_upload_max_backoff_seconds` is the ceiling on any single
  wait between attempts, including a wait R2 asks for with
  `Retry-After`. At six attempts the exponential backoff peaks at 8 s
  and never reaches it, so it matters only when R2 sends a
  `Retry-After` longer than the shared 10 s ceiling (honoured up to
  30 s here, where the shared budget would clamp it to 10 s), or when
  `keeper_sync_upload_max_attempts` is raised past seven. At `0.0`,
  retries go out back to back. The base of the backoff, 0.5 s, is not
  configurable.
- `keeper_sync_upload_concurrency` bounds the presigned PUTs in flight
  at once across every keeper-sync job in the worker process, and sizes
  the copy client's connection pool (see
  [Connection pool](#connection-pool)). A value above
  `keeper_sync_max_jobs` x `keeper_sync_copy_concurrency` (80 at the
  defaults) never throttles an upload.
- `keeper_sync_copy_retry_delay_seconds` is how long the build-level
  retry waits before it re-runs a failed copy. At `0.0` the copy is
  re-run at once. It is a wait, not a count: the copy is re-run at most
  once whatever the value.

Two neighbouring settings are not on this table because they are not
transport knobs: `keeper_sync_max_jobs` (default 10) and
`keeper_sync_copy_concurrency` (default 8). Their product bounds the
object bodies the sync worker buffers at once, which is what its memory
limit is sized against, so raising either one means raising that limit
too. It no longer sizes the copy client's connection pool.

**Phalanx values for these settings are optional.** The defaults
above are the intended production values, chosen against the
2026-09-22 outage, so no environment needs to set them. Add chart
values for them, in a Phalanx change of their own, only to move one
environment away from the defaults: for example, a shorter
`keeper_sync_copy_retry_delay_seconds` in a test environment so a
forced failure resolves quickly.

## The per-object budget

Every presigned PUT of a copied object runs through `retry_request`,
the retry loop the storage clients share. A transport failure (any
`httpx.TimeoutException`, an `httpx.NetworkError` such as a connection
refused or reset mid-body, or an `httpx.RemoteProtocolError`) and a
retryable status (`429`, `500`, `502`, `503`, `504`) draw on the same
budget. Any other status, such as a `403` from a revoked credential or
a redirect, fails on its first attempt, because another attempt would
fail the same way. Each attempt signs a fresh URL.

### Ride-out arithmetic

An object rides out an R2 connect outage if its last attempt starts
after the outage has cleared. Until then, every earlier attempt burns
the copy client's 10 s connect timeout, and every gap between attempts
is a backoff sleep that starts at 0.5 s and doubles:

```
backoff sleeps     0.5 + 1 + 2 + 4 + 8   = 15.5 s   (between 6 attempts)
connect timeouts   5 x 10 s              = 50 s     (attempts 1 to 5)
ride-out           15.5 + 50             = 65.5 s   (attempt 6 starts)
given up           65.5 + 10             = 75.5 s   (attempt 6 times out)
```

So at the defaults an object survives a connect outage of about 65 s,
counted from its own first failed attempt. The shared budget it
replaced, four attempts behind a 5 s connect timeout, rode out
0.5 + 1 + 2 + 3 x 5 = 18.5 s, less than half of the 2026-09-22 outage.

Each extra attempt adds its backoff plus one more connect timeout: 7
attempts ride out 65.5 + 16 + 10 = 91.5 s, and 8 ride out
91.5 + 30 + 10 = 131.5 s. From the eighth attempt on, the 30 s
backoff ceiling applies.

A **status** outage, where R2 answers `503` rather than not answering,
rides out less. A status comes back in milliseconds, so the budget is
only the sleeps: 15.5 s at the defaults, or longer where R2 sends
`Retry-After`. The build-level retry below does not re-run a copy that
failed this way.

## The copy client

Presigned PUTs of copied objects go over a dedicated
`httpx.AsyncClient`, and nothing else does. Every worker process opens
it at startup next to the shared client (`ctx["copy_http_client"]`
next to `ctx["http_client"]`) and `shutdown` closes both. The shared
client keeps httpx's defaults (5 s timeouts, 100 connections, 20 kept
alive) and carries run discovery, the LTD API, GitHub, Cloudflare KV
and CDN purges. LTD object downloads go through aiobotocore, so neither
client carries them.

Before the split, the stock 10 x 8 = 80 concurrent copies ran the
shared 100-connection pool close to its ceiling. An upload waiting for
a pooled connection then timed out like an upload that could not reach
R2, and spent a retry doing it.

### Timeouts

`COPY_HTTP_TIMEOUT`, fixed in code:

| Timeout | Value | Why |
| --- | --- | --- |
| `connect` | 10 s | Gives each attempt room to reach R2 through a connect stall. It is the per-attempt term in the ride-out arithmetic above. |
| `read` | 60 s | Waiting for R2 to acknowledge a whole object body. |
| `write` | 60 s | Sending one whole object body; a large build asset can take longer than 5 s to send from a busy pod. |
| `pool` | 30 s | Waiting for a free connection. The pool is sized so that this wait should not happen; if it does, a long wait beats failing an attempt that never reached R2. |

Every one of them raises a subclass of `httpx.TimeoutException`, so
each expiry is a retryable transport failure that spends one attempt of
the per-object budget.

### Connection pool

`copy_http_limits` derives the pool from the upload cap:

| Limit | Formula | At the defaults |
| --- | --- | --- |
| `max_connections` | `keeper_sync_upload_concurrency` + `COPY_HTTP_CONNECTION_HEADROOM` (10) | 42 |
| `max_keepalive_connections` | Same as `max_connections` | 42 |
| `keepalive_expiry` | `COPY_HTTP_KEEPALIVE_EXPIRY_SECONDS` | 60 s |

- `max_connections` budgets one connection for every presigned PUT that
  `keeper_sync_upload_concurrency` lets through at once, across every
  `keeper_sync_project` job in the process. No PUT waits on the pool at
  the cap. The headroom covers brief overlaps, such as a connection
  being torn down after a failed attempt while its retry opens a
  replacement.
- `max_keepalive_connections` equals `max_connections`, so every
  connection the pool opens is kept alive. httpcore closes an idle
  connection whenever the pool holds more connections than this, so a
  lower value tears a connection down after almost every PUT of a burst
  and the next object re-dials R2.
- `keepalive_expiry` keeps an idle connection open for a minute rather
  than httpx's default 5 s, so the pool stays warm between builds.

The timeouts, the headroom and the keepalive expiry are constants, not
configuration: they bound transport behaviour an operator has no reason
to tune, while the retry budget on top of them is configurable. The
pool follows `keeper_sync_upload_concurrency` automatically. Every
worker pool opens a copy client sized from that setting, because the
pools share one startup, but only keeper-sync jobs copy and the client
opens no connection until a copy uses it.

## The build-level retry

When a copy fails, `KeeperSyncService.sync_build` looks at the
exception the copier raised. That is the first failing object's own
exception, with its cause chain intact. An exception that was only
being handled when another one was raised (its implicit `__context__`)
does not count. If the exception, or anything on its explicit
`__cause__` chain, is a retryable transport error from either end of
the copy, the service re-runs it:

- **R2 upload:** `httpx.TimeoutException`, `httpx.NetworkError` or
  `httpx.RemoteProtocolError`, once the object has spent its whole
  per-object budget.
- **LTD download:** `botocore.exceptions.ConnectionError` (the parent
  of `EndpointConnectionError`, `ConnectTimeoutError`,
  `ProxyConnectionError` and `SSLError`) or
  `botocore.exceptions.HTTPClientError` (the parent of
  `ReadTimeoutError`, `ConnectionClosedError`, `ResponseStreamingError`
  and aiobotocore's wrapper for any other aiohttp client error). These
  come from listing the build prefix or downloading an object, after
  botocore's own retries, if any, have given up. The service reads
  them from `RETRYABLE_SOURCE_TRANSPORT_ERRORS` next to the LTD source,
  so it does not import botocore itself.

For either kind, the service:

1. logs `Retrying build copy after transport error` at warning,
2. waits `keeper_sync_copy_retry_delay_seconds`,
3. re-runs the whole copy **once**, into the same placeholder build.

Re-running into the same build is safe because a copy is
content-hashed and idempotent: objects that landed on the first pass
are rewritten with the same bytes. The wait holds no database
transaction and no advisory lock, because `sync_build` copies between
its transactions.

These are **not** re-run, and fail the edition exactly as they did
before the retry existed:

- a second failure, of any kind;
- `LtdSourceAccessDeniedError`, an LTD build prefix that anonymous
  reads are denied on;
- a botocore `ClientError` from the LTD side of the copy, **including**
  a throttling `SlowDown` or other `503`, and a `NoSuchKey`;
- an `httpx.HTTPStatusError`, **including** an upload that spent its
  whole budget on `429`/`5xx`.

The build-level retry is for either end being unreachable, not for it
answering with errors. A `ClientError` is S3 answering, so a throttled
LTD download is the source-side twin of a throttled R2 upload and is
left to the next sync in the same way.

Because those propagate unchanged, `sync_project`'s per-edition failure
accounting (the job's `edition_failures`, the consecutive-failure
breaker) and the reclaim of orphaned placeholder builds see exactly
what they saw before.

### Ride-out with the build-level retry

For an R2 connect outage, the first pass gives up when its first
object's last attempt times out. The re-run then gives every object a
fresh per-object budget:

```
first pass gives up          75.5 s
build-level retry wait     + 30 s
re-run's ride-out          + 65.5 s
                           = 171 s
```

A copy therefore survives an R2 connect outage of just under three
minutes, counted from its first failed connect. Treat that as a floor:
the re-run lists and downloads before its first PUT, which only
lengthens it.

An LTD outage is ridden out for much less, because a download has no
per-object budget. The first pass fails as soon as botocore's own
retries give up, and the re-run lands only if LTD is answering again
when it lists and downloads, `keeper_sync_copy_retry_delay_seconds`
(30 s) later plus botocore's retries on the re-run. Raising that delay
is the one knob that lengthens it.

The cost of that resilience is time. A copy that fails both passes
takes about three minutes, so a sustained R2 outage costs that much
per edition. A project with many editions can reach
`keeper_sync_job_timeout_seconds` (3600 s by default), about twenty
failing editions in, before the consecutive-failure breaker trips at
75. arq then cancels the job and `keeper_sync_reaper` fails its
`queue_jobs` row.

## When both retries fail

The edition fails, as it always has. `sync_project` logs
`Edition sync failed; skipping edition and continuing`, sends the
exception to Sentry, records the edition in the job's
`progress.edition_failures`, and carries on with the next edition, so
the job ends `completed_with_errors` (or `failed`, if the
consecutive-failure breaker trips). For an R2 connect timeout, the
entry's `error_type` is `ConnectTimeout` and its `error_message` is
often empty; the `Presigned upload failed` line described below has
the detail. For an LTD download failure, `error_type` is the botocore
class, such as `ReadTimeoutError` or `EndpointConnectionError`, and
`error_message` names the endpoint; no `Presigned upload failed` line
precedes it. The failed copy leaves its placeholder build `pending`,
and a later sync of the same git ref fails placeholders that are more
than an hour old.

The failed copy records no build sync state, so the next sync of the
edition cannot short-circuit on it and copies it again. What starts
that next sync depends on the project:

- **A project with at least one non-`main` edition** is re-synced
  whole by `keeper_sync_tier_other` once any non-`main` edition's state
  is an hour old: hourly while the project is hot, and at the dormant
  cadence (about daily) otherwise. That re-sync covers every edition,
  including a failed `main`.
- **A project whose only edition is `main`** is not re-driven by the
  tier crons until LTD rebuilds `main`. `sync_edition` records LTD's
  `date_rebuilt` on the edition's state before it copies, so
  `keeper_sync_tier_main` sees nothing new to fetch, and
  `keeper_sync_tier_other` looks only at non-`main` editions.

There is no "retry failed jobs" endpoint. To catch a project up
without waiting, `POST /orgs/{org}/keeper-sync/projects/{ltd_slug}/refresh`
enqueues a sync of it now, and a new backfill run,
`POST /orgs/{org}/keeper-sync/runs`, re-drives a whole wave. For a
`main`-only project, one of these is the only recovery path short of
an LTD rebuild. Check a campaign's failed editions after any R2 or LTD
outage.

## Reading a copy

### The metrics event

Every build-content copy publishes one project-scoped
`build_content_copied` event (`BuildContentCopiedEvent`) from the
`keeper_sync_project` worker, after the copy succeeds and after it
fails. A copy the build-level retry re-ran is still **one** copy and
one event. `organization` is the org slug and `project` the Docverse
project slug. They come from the shared payload base, like every
Docverse event.

| Field | Meaning |
| --- | --- |
| `ltd_slug` | The LTD product the build was copied from. It differs from `project` when the organization rewrites slugs. |
| `object_count` | Objects stored by the copy's last pass: the build's whole object count on success, or what the last pass stored before it stopped. |
| `total_size_bytes` | Bytes stored by the last pass, counted the same way. |
| `duration_seconds` | Wall-clock seconds from the first pass starting to the last pass ending, including the build-level retry's wait. It is a float in seconds, the unit the retry log lines' `elapsed_seconds` uses. |
| `peak_concurrent_copies` | Most objects in flight at once, over both passes. At most `keeper_sync_copy_concurrency`. |
| `retried_object_count` | Objects that landed only after at least one upload retry, summed over both passes. |
| `exhausted_object_count` | Objects whose upload spent its whole budget, summed over both passes. Only failures a retry could have fixed count: a transport failure or a `429`/`5xx` on every attempt. A `403` or a failed LTD download does not. |
| `build_retry_used` | Whether the build-level retry re-ran the copy, after a transport error on either end. |
| `succeeded` | Whether the copy, after any build-level retry, stored every object. |

A build whose content Docverse already holds (the manifest-hash dedupe)
copies nothing and publishes no event, and neither does a copy
cancelled by the arq job timeout.

Two fields carry the campaign-level signal:

- **`retried_object_count` is the early warning.** A rising count
  across a campaign is R2 getting flaky while the per-object budget
  still absorbs it: no edition has failed yet.
- **`exhausted_object_count` above zero means an object outlasted its
  whole budget,** so R2 was unreachable or erroring for over a minute.
  Read it with `succeeded` and `build_retry_used`. It is not a measure
  of how many objects the outage touched: the first object to exhaust
  its budget fails the pass and cancels the objects still in flight, so
  a failed pass usually contributes exactly one. For the breadth of an
  outage, use `retried_object_count` and the retry log lines.

Both counters describe R2 uploads only. An LTD outage moves neither:
it shows as `build_retry_used` with `exhausted_object_count` at zero,
and the retry log line's `error_type` names the botocore class.

| `succeeded` | `build_retry_used` | `exhausted_object_count` | What happened | What to do |
| --- | --- | --- | --- | --- |
| true | false | 0 | A clean copy. A non-zero `retried_object_count` means the per-object budget absorbed some blips. | Nothing. Watch the trend. |
| true | true | 1 or more | An R2 connect outage outlasted one object's whole budget, and the build-level retry re-ran the copy successfully. The edition synced. The first pass's `Presigned upload failed` error still reached Sentry. | Nothing for the edition. Many of these in one campaign means outages longer than a minute are routine; consider raising `keeper_sync_upload_max_attempts`. |
| true | true | 0 | The first pass lost the LTD bucket mid-download (a botocore transport error, which has no per-object budget to exhaust), and the re-run landed. The edition synced. | Nothing for the edition. The retry log line's `error_type` names the botocore class. Many of these in one campaign means LTD S3 is flaky; consider a longer `keeper_sync_copy_retry_delay_seconds`. |
| false | true | 1 or more | Both passes failed, and at least one pass was an R2 outage: the outage outlasted about three minutes, or the re-run hit a different failure. The edition failed. | See [When both retries fail](#when-both-retries-fail). |
| false | true | 0 | Both passes failed without an R2 upload exhausting its budget: most often LTD was still unreachable on the re-run. The edition failed. | Read the edition failure's `error_type`, then see [When both retries fail](#when-both-retries-fail). |
| false | false | 1 or more | An upload spent its budget on a retryable *status* (`429`/`5xx`) rather than a transport error, which the build-level retry does not re-run. The edition failed. | R2 was answering with errors rather than unreachable. Check R2's status, then refresh the project. |
| false | false | 0 | A failure the build-level retry does not re-run: an LTD `AccessDenied`, a botocore `ClientError` such as an LTD `SlowDown`, or a non-retryable R2 status such as `403`. | Not a transport problem. Read the edition failure's `error_type` on the job. |

### Logs

| Message | Level | When | Fields |
| --- | --- | --- | --- |
| `Retrying presigned upload after transport error` | warning | An upload attempt failed on a transport error and the budget has attempts left | `key`, `error` (the exception's `repr`, never empty), `error_type`, `attempt`, `max_attempts`, `retry_delay`, `elapsed_seconds` |
| `Retrying presigned upload` | warning | An upload attempt got a `429`/`5xx` and the budget has attempts left | `key`, `status_code`, `attempt`, `max_attempts`, `retry_delay` |
| `Presigned upload failed` | error | One object's upload failed for good | Transport: `key`, `error`, `error_type`, `attempts`, `elapsed_seconds`, `retryable`. Status: `key`, `status_code`, `response_body`, `attempts`, `elapsed_seconds`, `retryable` |
| `Retrying build copy after transport error` | warning | The build-level retry is about to wait and re-run the copy | `error`, `error_type`, `retry_delay`, `dest_prefix`, `project`, `edition_slug`, `ltd_build_id`, `docverse_build_public_id`, `ltd_source_prefix` |
| `Copied build content` | info | A copy pass stored every object | `object_count`, `total_size_bytes`, `content_hash`, `peak_concurrent_copies`, `retried_object_count`, `exhausted_object_count` |
| `Edition sync failed; skipping edition and continuing` | error | The edition failed, after or without the build-level retry | `ltd_edition_id`, `ltd_edition_slug`, `project` |

`attempts` and `elapsed_seconds` on `Presigned upload failed` say
whether the budget or the outage was too short: a transport failure
with `attempts=6` and `elapsed_seconds` near 75 is an object that tried
for the full ride-out. `error` carries the exception's `repr` because
`str(httpx.ConnectTimeout())` is the empty string. Before PRD #685 that
field was logged as `error=""`.

On `Retrying build copy after transport error`, `error_type` says
which end of the copy failed: an httpx class such as `ConnectTimeout`
or `ReadError` is the R2 upload, and a botocore class such as
`ReadTimeoutError`, `EndpointConnectionError` or
`ConnectionClosedError` is the LTD listing or download.

`Copied build content` is logged once per **pass**. After a re-run it
carries the second pass's counts only. The event above is the one that
sums both passes.

### Sentry

Each exhausted object produces one `Presigned upload failed` error in
Sentry. A copy the build-level retry then rescued still leaves that
event behind, so a `Presigned upload failed` in Sentry does not by
itself mean an edition failed. The failed object's `key` starts with
the build's storage prefix, which is the `dest_prefix` of the
`Retrying build copy after transport error` line that follows it; if
no `Edition sync failed` line follows that for the same edition, the
re-run landed. The `build_content_copied` event's `succeeded` says the
same thing in one field. An edition that did fail also sends its
exception from `sync_project`, alongside the `Edition sync failed`
line.

An LTD download failure sends nothing to Sentry on its own. A re-run
that landed leaves only the warning log line; an edition that failed
both passes sends the second pass's botocore exception from
`sync_project`.

## What the layers deliberately do not do

- **Cap in-flight copies across the process.** The ceiling stays
  `keeper_sync_max_jobs` x `keeper_sync_copy_concurrency`; the copy
  client's pool is sized to it rather than enforcing a lower one.
- **Retry the LTD API differently.** The LTD client already rides out
  up to a 300 s backoff ceiling.
- **Retry an LTD download per object.** A download has only botocore's
  own retries beneath the build-level retry; there is no Docverse
  budget for it like the one R2 uploads get.
- **Re-run a copy on a status error from either end,** such as an R2
  `429`/`5xx` or an LTD `SlowDown`. The build-level retry re-runs
  transport failures only.
- **Make the copy client's timeouts configurable,** or re-run a build
  copy more than once.
- **Change the API process's object store,** or the shared client any
  other caller uses.
- **Deduplicate the per-object Sentry event.** One event per exhausted
  object is accepted; the build-level retry makes exhaustion rare.

## Related

- [Scoping the keeper sync](keeper-sync-scope.md), which covers the
  waves whose copies this page covers.
- `src/docverse_server/storage/_http_retry.py`: `retry_request`, the
  retryable statuses and transport errors, and the backoff.
- `src/docverse_server/storage/objectstore/_s3.py`: the presigned PUT
  and its `Presigned upload failed` lines.
- `src/docverse_server/storage/ltd/s3_source.py`: the LTD download and
  `RETRYABLE_SOURCE_TRANSPORT_ERRORS`, the botocore transport errors
  the build-level retry re-runs.
- `src/docverse_server/worker/main.py`: `COPY_HTTP_TIMEOUT`,
  `copy_http_limits` and the copy client's lifetime.
- `src/docverse_server/services/keeper_sync/service.py`: the
  build-level retry and the report behind the metrics event.
- `src/docverse_server/metrics/payloads.py`: `BuildContentCopiedEvent`.
- `tests/docs_test.py`: fails when this page stops naming a knob, a
  default, a copy-client constant, an event field, or an error class
  the build-level retry re-runs that the code has, or when its ride-out
  arithmetic no longer matches the defaults.
- SQR-112, and PRD #685.
