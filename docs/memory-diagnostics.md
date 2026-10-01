# Memory diagnostics

Every Docverse process (the API and each of the three arq worker pools)
carries an opt-in memory sampler. When it is switched on, the process
logs one `Memory sample` line per interval with its resident size, its
high-water mark, and the garbage collector's counters. With tracemalloc
also switched on, each line adds the traced Python heap and the
allocation sites whose size changed most since the previous line. The
sampler is how a leak is located from pod logs, since nothing can attach
to a running Docverse pod.

This page is for operators chasing a pod's memory growth. It covers
what each field means, the settings and their Phalanx values, how to
turn the sampler on for roundtable-dev, how to tell a Python-heap leak
from fragmentation, how to read `top_sites` from one tick to the next,
and why tracemalloc stays in development environments.

## Why this exists

On 2026-09-29, roundtable-prod's `docverse-sync-worker` was OOM-killed
at its 2 Gi limit during backfill run `1xgf-0pez-y34n-43`. The run
imported 160 products, most of them `ts-*` CSC docs with many small
release builds (#751). The pod had grown from 433 Mi to 1.8 Gi over
two and a half hours. Its replacement went from 577 Mi to 914 Mi in two
minutes on the same queue, and memory taken during a burst was never
given back. Every kill orphans the in-flight `keeper_sync_project` rows
and blocks the organization's next run until the reaper frees them.

The pods run as non-root, with a read-only root filesystem and every
capability dropped. `memray attach`, `py-spy`, or a snapshot taken from
a shell cannot work in such a pod. The diagnostics therefore ship in the
image, gated by configuration, and report through the logs (PRD #753).

The same PRD removed the one allocation churn that code review found.
Each build copy and manifest hash used to build its R2 destination store
from a new aiobotocore session, and each new session parsed botocore's S3
service model and endpoint ruleset again. That happened thousands of
times per backfill. Now every process creates one session at startup,
and every S3 client it opens comes from that session: the LTD source's
client and each copy's destination store. The sampler is how
roundtable-dev checks whether such a fix is enough.

It was not quite enough. The second dev run, with the shared session,
removed the traced growth and the OOM kill, but the sync worker's RSS
still grew by about 2.7 MB per destination client it opened, outside
anything tracemalloc traced. Each worker process now also keeps its
destination clients open between uses; see
[Shared destination clients](#shared-destination-clients).

## Shared destination clients

Opening an object store creates an aiobotocore client, and with it an
aiohttp connector and an `ssl.SSLContext` loaded with the full CA store.
A worker used to open a store per build copy, per manifest hash and per
`build_processing`, `dashboard_build` and `purgatory_cleanup` job, and
close it straight after. On roundtable-dev each of those clients left
about 2.7 MB of RSS behind, growing the sync worker from 194 MB to
1019 MB over about 300 stores while the traced heap stayed flat (#751).

Each worker process now creates one `ObjectStoreCache` at startup
(`ctx["objectstore_cache"]`), and `shutdown` closes every client in it.
Every job's factory resolves its org object stores through the cache, so
the process holds one open client per:

- organization and object-store service label;
- provider and service config;
- upload flavour: the keeper-sync copier's store, built with the
  keeper-sync upload budget, the copy HTTP client, and the worker-wide
  upload limiter, is a separate client from the one every other job
  uses.

A worker therefore holds at most two clients for each organization's
object-store service, however many builds it copies.

Each use still reads the service row and decrypts the credential. When
either one's `date_updated` has changed since the cached client was
opened, the next use closes that client and opens a new one, so an edited
service or a rotated credential takes effect without a restart. A client
that a running copy still holds is closed when that copy releases it.

The API process keeps no cache: a request's store opens a client on
entry and closes it on exit, as before.

A cached store outlives the job that opened it, so its own log lines,
such as `Presigned upload failed`, carry the cache's `org_id` and
`service_label` rather than the job's context. The object `key` on
those lines still names the project and build, and the copier's own
lines that follow carry the job's context.

### Log lines

| Message | Level | Fields |
| --- | --- | --- |
| `Opened shared object store client` | info | `org_id`, `service_label`, `provider`, `open_clients` |
| `Replaced shared object store client` | info | `org_id`, `service_label`, `provider`, `open_clients` |
| `Failed to close shared object store client` | warning | `exception` |

- `open_clients` is the number of clients the process holds after the
  open, counting a replaced client that a running copy still holds. On
  a dev run, these lines give the client count to read next to each
  `Memory sample`: a count that keeps climbing is a cache that is not
  being hit.
- `Replaced shared object store client` follows a service edit or a
  credential rotation, and stands in for the `Opened` line of the new
  client.
- `Failed to close shared object store client` is logged at shutdown or
  on a replacement when the old client fails to close. The remaining
  clients are still closed.

## What the sampler logs

A process whose settings enable the sampler logs `Memory diagnostics
enabled` once at startup, followed by a `Memory sample` right away and
another every `memory_diagnostics_interval_seconds`. The first sample is
the baseline from process start. Each line carries the process's
`component` label, the same label Sentry tags its events with, so the
four pods can be told apart once their logs are merged:

| `component` | Process | Deployment |
| --- | --- | --- |
| `api` | The FastAPI app (its lifespan starts the sampler) | `docverse` |
| `worker` | The default arq pool | `docverse-worker` |
| `worker-keeper-sync` | The keeper-sync arq pool | `docverse-sync-worker` |
| `worker-maintenance` | The maintenance arq pool | `docverse-maintenance-worker` |

The sampler starts before the process opens its database pool, HTTP
clients, and LTD source, so with tracemalloc on, their allocations are
traced as well. It stops first at shutdown. A tick runs in a thread,
off the event loop. A tick that fails logs a warning and the next one
fires on schedule. The sampler never raises into the process and never
holds up its startup.

### Sample fields

Byte counts are integers in bytes. In the JSON logs, tuples are
rendered as lists.

| Field | Present | Meaning |
| --- | --- | --- |
| `component` | always | Which process logged the line (see the table above). |
| `rss_bytes` | always | Resident set size now: `VmRSS` from `/proc/self/status`. `null` on a platform with no `/proc`, such as macOS during local development, where the current size cannot be read. |
| `rss_peak_bytes` | always | Resident high-water mark since the process started: `VmHWM`. It never falls, so once a burst is over, it still records how high the process went. Without `/proc` it is `getrusage`'s `ru_maxrss`. |
| `gc_counts` | always | The collector's per-generation counters from `gc.get_count()`. They rise as container objects are allocated and reset when a generation is collected, so they measure collector activity rather than heap size and are no leak signal on their own. |
| `gc_objects` | with tracemalloc | Objects the collector tracks, from `len(gc.get_objects())`. These are containers and class instances, not `bytes` or `str`, so a leak of buffers does not move it. Only counted while tracing because counting walks the whole heap. |
| `tracemalloc_current_bytes` | with tracemalloc | Size of the traced Python heap now. Only allocations made since tracing started are traced, so modules imported earlier are not counted. The figure includes the previous snapshot that the sampler keeps for its next diff (see [What tracemalloc costs](#what-tracemalloc-costs)). |
| `tracemalloc_peak_bytes` | with tracemalloc | Largest the traced heap has been since tracing started. The sampler never resets it, and its own snapshots can set it. |
| `top_sites` | with tracemalloc | Up to `memory_diagnostics_top_n` allocation sites whose traced size changed most since the previous sample (see [Reading `top_sites` across ticks](#reading-top_sites-across-ticks)). |

A process whose sampler runs without tracemalloc leaves the four
tracemalloc fields off the line entirely rather than logging nulls. If
`PYTHONTRACEMALLOC` already turned tracing on before the sampler started,
the lines carry them anyway, at that traceback depth.

Below is an illustrative traced line from the sync worker. The values
are invented, and only one site is shown:

```json
{"component": "worker-keeper-sync", "rss_bytes": 948051968, "rss_peak_bytes": 958398464, "gc_counts": [1043, 7, 0], "gc_objects": 1210458, "tracemalloc_current_bytes": 311427072, "tracemalloc_peak_bytes": 402653184, "top_sites": ["/app/.venv/lib/python3.14/site-packages/botocore/parsers.py:512 <- /app/.venv/lib/python3.14/site-packages/botocore/parsers.py:390 +1048576 +2211"], "event": "Memory sample", "logger": "docverse_server.worker", "severity": "info"}
```

### Log lines

| Message | Level | Fields |
| --- | --- | --- |
| `Memory diagnostics enabled` | info | `component`, `interval_seconds`, `tracemalloc_enabled`, `tracemalloc_frames`, `top_n` |
| `Memory sample` | info | `component`, plus the [sample fields](#sample-fields) above |
| `Memory sample failed` | warning | `component`, `exception` |
| `Memory diagnostics disabled; ignoring tracemalloc setting` | warning | `component` |
| `Memory diagnostics failed to start` | warning | `component`, `exception` |
| `Memory diagnostics failed to stop` | warning | `component`, `exception` |

- `Memory diagnostics enabled` reports the tracing actually in effect.
  If `PYTHONTRACEMALLOC` started tracing first, `tracemalloc_enabled`
  is `true` and `tracemalloc_frames` is that traceback depth, whatever
  the settings say.
- `Memory sample failed` appears once per failing tick. One cause is an
  unreadable `/proc/self/status`. The loop carries on, so a run of
  these lines shows a tick that keeps failing, not a dead sampler.
- `Memory diagnostics disabled; ignoring tracemalloc setting` means
  `memory_diagnostics_tracemalloc_enabled` was set without
  `memory_diagnostics_enabled`. Nothing is traced or sampled.
- `Memory diagnostics failed to start` and `failed to stop` report a
  sampler that could not start or stop. The process starts, or shuts
  down, anyway.
- `exception` is the traceback that Safir's production log profile
  renders from `exc_info`.

## Configuration

| Setting | Environment variable | Default | Phalanx value |
| --- | --- | --- | --- |
| `memory_diagnostics_enabled` | `DOCVERSE_MEMORY_DIAGNOSTICS_ENABLED` | `false` | `config.memoryDiagnostics.enabled` |
| `memory_diagnostics_interval_seconds` | `DOCVERSE_MEMORY_DIAGNOSTICS_INTERVAL_SECONDS` | `60` | `config.memoryDiagnostics.intervalSeconds` |
| `memory_diagnostics_tracemalloc_enabled` | `DOCVERSE_MEMORY_DIAGNOSTICS_TRACEMALLOC_ENABLED` | `false` | `config.memoryDiagnostics.tracemallocEnabled` |
| `memory_diagnostics_tracemalloc_frames` | `DOCVERSE_MEMORY_DIAGNOSTICS_TRACEMALLOC_FRAMES` | `5` | `config.memoryDiagnostics.tracemallocFrames` |
| `memory_diagnostics_top_n` | `DOCVERSE_MEMORY_DIAGNOSTICS_TOP_N` | `10` | `config.memoryDiagnostics.topN` |

- `memory_diagnostics_enabled` gates everything. When it is off, no
  process starts a sampler task, nothing is traced, and no `Memory
  sample` line is logged. It ships off.
- `memory_diagnostics_interval_seconds` sets the log volume: one line
  per interval per pod, so 1,440 lines a day per pod at 60 s. A traced
  line carries up to `memory_diagnostics_top_n` sites of up to
  `memory_diagnostics_tracemalloc_frames` frames each, which comes to a
  few kilobytes. Each traced tick also snapshots the whole traced heap,
  so an interval much shorter than 60 s adds measurable CPU on a large
  heap.
- `memory_diagnostics_tracemalloc_enabled` adds tracemalloc on top of
  the sampler and does nothing without `memory_diagnostics_enabled`. It
  is a development-environment diagnostic only (see
  [What tracemalloc costs](#what-tracemalloc-costs)).
- `memory_diagnostics_tracemalloc_frames` is the traceback depth that
  tracemalloc records per allocation. It is therefore the depth at
  which `top_sites` tells sites apart.
- `memory_diagnostics_top_n` is how many sites each traced sample
  reports.
- The interval, frames, and top-N settings must be at least 1. A
  smaller value fails configuration at startup.

The Phalanx chart renders each value in the table into the `docverse`
configmap. All four deployments read that configmap, so the values turn
the sampler on in every process at once; there is no switch for a single
pool. The chart change that adds these values (#757) ships separately
from the server release. An environment whose chart lacks them keeps the
sampler off. Every deployment carries a checksum of the configmap, so
changing a value rolls all four pods. The settings are read only at
process start.

## Enabling on roundtable-dev

1. In Phalanx, add the values to
   `applications/docverse/values-roundtable-dev.yaml`:

   ```yaml
   config:
     memoryDiagnostics:
       enabled: true
       tracemallocEnabled: true
   ```

   The other three settings keep their defaults unless a run needs
   something else, such as a deeper `tracemallocFrames` to split a
   shared allocator's callers. Never set these values in `values.yaml`
   or in `values-roundtable-prod.yaml`.

2. Merge the change and run a **full** sync of the `docverse`
   application in Argo CD. The configmap is a `PreSync` hook, and a
   selective sync skips hooks, so it would leave the old configmap in
   place. The configmap checksum then rolls every pod.

3. Confirm that each pod started the sampler with tracing on:

   ```sh
   kubectl logs -n docverse deployment/docverse-sync-worker \
     | jq -cR 'fromjson? | select(.event == "Memory diagnostics enabled")'
   ```

   Repeat for `docverse`, `docverse-worker`, and
   `docverse-maintenance-worker`. `fromjson?` skips any log line that
   is not JSON.

4. Follow the samples, for example the sync worker's sizes during a
   keeper-sync wave:

   ```sh
   kubectl logs -n docverse deployment/docverse-sync-worker --since=1h \
     | jq -cR 'fromjson? | select(.event == "Memory sample")
         | {rss_bytes, rss_peak_bytes, tracemalloc_current_bytes}'
   ```

   Swap in `.top_sites[]` with `jq -rR` to read the sites one per line.

5. Once the investigation is over, turn `tracemallocEnabled` back off
   (and `enabled` too, unless the RSS-only lines are still wanted), with
   another full sync.

## Reading a leak signature

Record samples at four points of a run: before it starts, after its
first 50 or so copies (by then the pools are open and the caches warm),
at its peak, and after it finishes and the process has been idle for a
few ticks. The warm reading is the baseline, not the reading at start.
A process that has settled returns to near that warm value when it is
idle. PRD #753 accepts the shared-session fix only if the idle
`rss_bytes` is within 25% of the warm reading and no site grows by
10 MB or more across the wave. `rss_peak_bytes` shows how high the
burst went. An idle `rss_bytes` that stays near `rss_peak_bytes` means
the burst's memory was never given back. The next step is to find out
whether the Python heap is still holding it or the allocator is.

`kubectl top` and the OOM killer read the container's cgroup memory,
which also counts page cache and kernel memory. Their numbers therefore
sit somewhat above `rss_bytes`, and a comparison should rely on the
trend, not on absolute agreement.

### Python-heap growth

`tracemalloc_current_bytes` rises roughly in step with `rss_bytes`, and
neither comes back once the process is idle. The gap between them stays
roughly constant. The same `top_sites` entries show a positive
`size_diff` tick after tick. `gc_objects` climbs as well when the
retained objects are containers or instances rather than raw buffers.

Something in the process is holding on to Python objects: a cache
without a bound, a list on a long-lived singleton, or a reference kept
past its job. The growing sites name the code. The fix is a code change,
filed as a follow-up that names the site.

### Fragmentation or native memory

`rss_bytes` sits far above `tracemalloc_current_bytes`, and the gap
widens across the wave. Meanwhile, the traced size stays flat or falls
back when the process is idle, and no site grows from tick to tick. The
memory is outside the heap that tracemalloc sees. Two things put it
there:

- **Freed memory the allocator kept.** glibc serves an allocation of
  128 KiB or more with its own `mmap` at first and returns that memory
  on free. Once blocks that large are freed, though, glibc raises the
  threshold (up to 32 MiB), so a burst of whole-object `bytes` bodies
  ends up in malloc's arenas. Left to itself, an arena returns memory to
  the system only from its top, so a heap that churns large buffers
  holds on to space that Python has already freed. Python's own
  small-object allocator behaves the same way: it releases one of its
  arenas only after every object in it has been freed.
- **Native allocations tracemalloc never sees,** such as OpenSSL's
  per-connection buffers or memory a C extension allocates with `malloc`
  directly.

With tracing on, tracemalloc's own bookkeeping also counts in
`rss_bytes` and not in the traced size. It grows with the number of live
blocks, so what matters is whether the gap is widening, not its size at
any one moment.

### The `MALLOC_ARENA_MAX` experiment

glibc lets threads that allocate concurrently take arenas of their own,
up to eight per core on a 64-bit machine. Each arena grows and
fragments independently and rarely shrinks. A Docverse worker has
several such threads: the asyncio default executor (which runs DNS
lookups and `asyncio.to_thread` calls, including the sampler's own
ticks) and, where Sentry is configured, the Sentry SDK's background
worker. `MALLOC_ARENA_MAX=2`
caps glibc at two arenas, accepting a little allocation contention in
exchange for less fragmentation.

Try it only after the shared-session fix is deployed, and only when the
samples show the fragmentation signature above. glibc reads the
variable when the process starts, so it is set as an environment
variable on the container, not as a Docverse setting. The image is
built on Debian, so it uses glibc and the variable applies. Set it on
`docverse-sync-worker` alone, through the chart's sync-worker
extra-environment hook (#757), and let the pod roll. Then re-run the
same wave shape and compare `rss_bytes` at the same four points. If the
idle RSS drops noticeably while `tracemalloc_current_bytes` does not
change, fragmentation was the cause, and the setting should stay on the
sync worker. If nothing changes, remove it.

## Reading `top_sites` across ticks

Each site reads as follows:

```text
file:line <- file:line <- ... size_diff count_diff
```

The allocating frame comes first, followed by its callers, up to
`memory_diagnostics_tracemalloc_frames` frames. `size_diff` and
`count_diff` are the signed changes in bytes and in live blocks since
the previous sample. Sites are sorted by the absolute size change,
largest first. A site whose size did not change is left out, as are the
allocations of tracemalloc's own snapshots.

- **A tick is a diff, not a census.** `top_sites` says what changed
  since the previous tick, not what the heap holds. A site's growth over
  a wave is the sum of its `size_diff` across the wave's ticks. The
  first sample diffs against an empty snapshot, so it lists the largest
  traced sites since tracing began. It is a census of the startup heap
  and not a trend.
- **A leak repeats.** The same site shows a positive `size_diff` in
  tick after tick, including idle ticks when no job is running. A
  positive `count_diff` alongside it means objects are being retained.
  A growing `size_diff` with a `count_diff` near zero means one buffer
  or container keeps growing.
- **Churn cancels.** A site that is positive during a burst and
  negative by a similar amount once it ends is allocation traffic, not
  a leak. It can still matter for fragmentation (see above).
- **Absent does not mean unchanged.** Only the
  `memory_diagnostics_top_n` largest changes are listed, so during a
  burst a slow leak can drop below the cutoff behind heavier churn.
  Idle ticks are the clean reading because churn stops and anything
  still growing is left. Raising `memory_diagnostics_top_n` shows more
  sites per line.
- **Depth decides what counts as one site.** Two allocations are the
  same site only when all of their recorded frames match. A shared
  helper (a botocore parser, a JSON decoder) called from several places
  shows up as several sites, one per calling path. A deeper
  `memory_diagnostics_tracemalloc_frames` splits sites further, at the
  cost of more tracing memory per live block. Changing either setting
  restarts the pods, which starts the diffs over.

## What tracemalloc costs

`memory_diagnostics_tracemalloc_enabled` is for development
environments only. With the sampler already on, turning tracing on adds
the following costs:

- **Memory.** Tracemalloc keeps a record (size and traceback) for every
  live block, which roughly doubles the Python heap's memory overhead.
  That bookkeeping appears in `rss_bytes` but not in
  `tracemalloc_current_bytes`, and deeper frames make it larger. The
  sampler also keeps the previous tick's snapshot for its next diff.
  That snapshot is a copy of every trace, and it is counted in
  `tracemalloc_current_bytes`. For a moment during each tick, two
  snapshots and their diff are alive together, which is why the sampler
  itself can set `tracemalloc_peak_bytes`.
- **CPU.** Every allocation and free goes through tracemalloc's hook,
  which records a traceback, so allocation-heavy work such as a copy
  burst runs noticeably slower. Each tick's snapshot and diff walk every
  trace, and `gc_objects` walks every tracked object. The tick runs in a
  thread, off the event loop, but it still shares the interpreter with
  the jobs, so a large heap's tick slows them while it runs.
- **Log volume.** Every traced line carries `top_sites`, several times
  the size of an untraced line.

These costs are why tracing stays out of production. Its overhead can
push a pod that is near its limit over it, so tracing the OOM being
chased can cause one. The slowdown changes the timing of the very work
being measured, and it can push a long job past its timeout.

The sampler without tracing is cheap: each tick reads
`/proc/self/status` once and logs one short line. Even so, it ships off
everywhere. Whether production should run it is a decision for after
its log volume on roundtable-dev is known.

## What the sampler deliberately does not do

- **Publish metrics.** The samples exist only in the logs. There is no
  Sasquatch event for them and no Chronograf dashboard.
- **Write files or take a heap dump.** The root filesystem is
  read-only, and the logs are the only output.
- **Sample at job boundaries.** Ticks run on a clock. To tie a sample to
  a job, match its timestamp against the job's own log lines.
- **Reset tracemalloc's peak between ticks.**
- **Switch on per pool.** One set of settings applies to all four
  processes. To sample a single deployment, set the
  `DOCVERSE_MEMORY_DIAGNOSTICS_*` environment variables on that
  deployment alone.
- **Pull in a new dependency.** It reads `/proc`, `resource`, `gc`,
  and `tracemalloc` from the standard library, and does not use
  `psutil`.

## Related

- `src/docverse_server/diagnostics/memory.py`: `MemorySample`,
  `read_memory_sample`, and `MemorySampler`.
- `src/docverse_server/config.py`: the `memory_diagnostics_*`
  settings.
- `src/docverse_server/worker/main.py` (`_startup` and `shutdown`) and
  `src/docverse_server/main.py` (`lifespan`): where each process starts
  and stops its sampler, and creates its one aiobotocore session.
- `src/docverse_server/storage/objectstore/_cache.py`:
  `ObjectStoreCache`, the worker's shared destination clients.
- [Keeper-sync transport resilience](keeper-sync-transport.md): the
  bound on the object bodies that the sync worker buffers, which its
  memory limit is sized against, and the shared LTD source client.
- #751, the sync-worker OOM that motivated the sampler, and PRD #753.
- `tests/docs_test.py`: fails when this page stops matching the
  sampler's settings, its log lines, or the fields a `Memory sample`
  line carries, or stops naming a process label, or when the
  shared-client log table stops matching the cache's log lines.
