# Docverse metrics events

Docverse reports what it does as Sasquatch application-metrics events
([SQR-112](https://sqr-112.lsst.io)). This page is the catalog: every
event the application registers, the InfluxDB measurement each one
lands in, every field with its type and meaning, which fields are
InfluxDB tags, and example InfluxQL queries for the questions the
Chronograf dashboards on `roundtable.lsst.cloud` ask of them.

It is standalone Markdown, like the rest of `docs/`.
`tests/docs_test.py` fails when an event the application registers, or
a field one of them carries, is missing from this page; see [Keeping
this page current](#keeping-this-page-current).

## How an event reaches InfluxDB

Every process that publishes, the API and each arq worker, builds one
Safir `EventManager` through `build_event_manager`
(`src/docverse_server/metrics/manager.py`), and `DocverseEvents.initialize`
(`src/docverse_server/metrics/events.py`) registers one publisher per
event type against it. Every event goes to one Kafka topic,
`lsst.square.metrics.events.docverse`, as an Avro record whose schema is
named for the event. Telegraf's app-metrics consumer writes each record
to InfluxDB as one point, in a measurement named for the schema's full
name:

```text
lsst.square.metrics.events.docverse.<event>
```

So `edition_published` lands in
`lsst.square.metrics.events.docverse.edition_published`. The name
contains dots, so InfluxQL needs it double-quoted:
`FROM "lsst.square.metrics.events.docverse.edition_published"`.

Beside the payload fields each event's table lists, Safir adds the same
metadata to every event:

| Field | Meaning |
| --- | --- |
| `id` | A UUID identifying the event. |
| `application` | `docverse`, from `METRICS_APPLICATION`. |
| `timestamp` | When the event was published. For an event that reports a duration, that is when the timed work finished. |
| `timestamp_ns` | The same instant in nanoseconds since the Unix epoch. InfluxDB uses it as the point's time. |

### Types

The Type column of each event's table uses these words:

| Type | Payload annotation | In InfluxDB |
| --- | --- | --- |
| string | `str` | A string field, or a tag. |
| integer | `int` | An integer field. |
| float | `float` | A float field. On this catalog every float is a number of seconds. |
| boolean | `bool` | A boolean field. As a tag, it compares as the string `'true'` or `'false'`. |
| duration | `timedelta` | A float field, in **seconds**. Avro carries a `timedelta` as a double of seconds. |
| enum | a `StrEnum` from `src/docverse_server/metrics/enums.py` | The member's string value. Each event's section lists the values. |

`or null` marks a field that may be absent. InfluxDB cannot store a
null, so an event that carries none has no such field (or no such tag)
on its point. A query aggregating a nullable field, such as
`PERCENTILE("ltd_lag", 95)`, therefore sees only the events that
carried it, and `GROUP BY` a nullable tag puts the events without it in
the group whose tag value is empty.

Every time value in the catalog reads in seconds: the `timedelta`
fields (`elapsed`, `duration`, `ltd_lag`) and the two float fields on
`build_content_copied` that name the unit (`duration_seconds`,
`ltd_lag_seconds`).

## Tags

Which fields become InfluxDB tags is decided in Phalanx, not here.
Docverse publishes every field the same way, and Telegraf promotes the
names listed under `globalAppConfig.docverse.influxTags` in
`applications/sasquatch/charts/app-metrics/values.yaml` to tags when it
writes each point. The list Docverse's events are designed for is:

```yaml
globalAppConfig:
  docverse:
    influxTags:
      - organization
      - project
      - action
      - edition_kind
      - trigger
      - role
      - principal_type
      - ci_platform
      - success
      - github_repository
      - method
      - route
      - status_class
      - authenticated
      - event_type
      - outcome
```

The last six arrived with `api_request` and `github_webhook_received`
(PRD #713), and take effect only once that Phalanx change is synced.
Until then those six are written as ordinary fields. Tagging happens
when a point is written, so points written before the change keep them
as fields, and a `GROUP BY "route"` sees only the points written after
it. A query that must tell a tag from a field of the same name can
write `"route"::tag`.

**A tag name applies to every event.** Telegraf applies the one list to
every measurement on the topic, so a name added for one event also tags
every other event that has a field of that name. Before naming a new
field `success` or `outcome`, check that it would make a sensible tag,
because it will become one. The list tags these events today:

| Tag | Events it tags | Added by |
| --- | --- | --- |
| `organization` | every event | SQR-112 |
| `project` | every event | SQR-112 |
| `action` | `project_lifecycle`, `edition_lifecycle`, `membership_changed`, `lifecycle_action` | SQR-112 |
| `edition_kind` | `edition_published`, `edition_lifecycle` | SQR-112 |
| `trigger` | `edition_published`, `lifecycle_action` | SQR-112 |
| `role` | `membership_changed` | SQR-112 |
| `principal_type` | `membership_changed` | SQR-112 |
| `ci_platform` | `build_uploaded` | SQR-112 |
| `success` | `build_processed`, `dashboard_built`, `keeper_sync_run_completed`, `lifecycle_action`, `purgatory_cleanup_completed` | SQR-112 |
| `github_repository` | `build_uploaded`, `github_webhook_received` | SQR-112 |
| `method` | `api_request` | PRD #713 |
| `route` | `api_request` | PRD #713 |
| `status_class` | `api_request` | PRD #713 |
| `authenticated` | `api_request` | PRD #713 |
| `event_type` | `github_webhook_received` | PRD #713 |
| `outcome` | `conditional_get`, `github_webhook_received` | PRD #713 |

`outcome` is the one PRD #713 tag that reaches an older event: it also
tags `conditional_get`'s `not_modified`/`modified` outcome, which is
what a cache hit-rate panel wants to group by anyway.
`build_content_copied` reports its result as `succeeded`, not
`success`, so it is not tagged by `success`.

### Cardinality rule

Every distinct combination of tag values is a separate InfluxDB series,
and series are what InfluxDB indexes and holds in memory, so a tag
whose values are unbounded grows the index without limit. **Tag only
closed vocabularies and bounded names**: enums, booleans, route
templates, and slugs bounded by what exists in Docverse or GitHub
(`organization`, `project`, `github_repository`). **Never tag a
concrete request path, a username, or a GitHub delivery ID**, nor any
other per-request or per-run identifier such as a commit SHA, a
workflow run ID, or a build ID.

The events are built so that the tag list above keeps to that rule:

- `api_request`'s `route` is the template the request matched, such as
  `/orgs/{org}/projects/{project}`, never the concrete path. A path no
  route matched records `route` as null rather than the path, so
  scanner noise adds request volume but no tag values.
- `api_request` records whether the ingress authenticated a request
  (`authenticated`), never who made it.
- `github_webhook_received` carries no delivery ID, and names an
  `event_type` only for a delivery whose signature verified, so a
  forged `X-GitHub-Event` header cannot mint tag values.
- `status_code` stays a field. Group by `status_class`, which has five
  values, and filter on `status_code` for drill-down.
- The identifiers the rule excludes stay fields: `build_uploaded`'s
  `uploader`, `commit_sha`, `github_run_id`, and `github_actor`, and
  `membership_changed`'s `principal`. Keep them off the tag list.

`api_request`'s `organization` and `project` are the slugs as the
caller wrote them in the path, including on a `404` for a slug that
does not exist. Only a caller that Gafaelfawr has authenticated reaches
a route that declares either one, which is what keeps them bounded.

## Measuring lag behind LTD Keeper

Two fields measure how long a rebuild in LTD Keeper takes to reach
Docverse while keeper-sync mirrors LTD (PRD #713):

- **`edition_published`'s `ltd_lag`** (duration) is the publish's
  success time minus the `date_rebuilt` LTD reported for the edition.
  It spans the whole path: the keeper-sync visit that noticed the
  rebuild, the build-content copy, the wait in the queue for
  `publish_edition`, and the CDN publish.
- **`build_content_copied`'s `ltd_lag_seconds`** (float seconds) is the
  copy's end minus the same `date_rebuilt`: the copy half. It is taken
  when the copy ends, whether it succeeded or failed.

Subtracting the copy half from the whole leaves the queue wait and the
CDN publish. Neither event names the edition or the build, which would
be a join key InfluxQL cannot use and a cardinality hazard as a tag, so
compare the two distributions over the same organization and window
(p50 `ltd_lag` against p50 `ltd_lag_seconds`, say) rather than pairing
events.

**When `ltd_lag` is set.** Only on a publish that a keeper-sync visit
enqueued after importing a fresh LTD rebuild of the edition: the job's
payload then carries the rebuild's `ltd_date_rebuilt`, and
`publish_edition` subtracts it at its success terminal. A semver
aggregate edition (`15`, `15.2`) that the same visit moved reports its
release's lag, because the release's rebuild is what moved it. A
build-level copy retry happens inside the window and counts toward it.

**When it is null.** Whenever there is no fresh rebuild to measure
from:

- publishes from a client build's fan-out, a rollback, or the reconcile
  loop;
- keeper-sync's self-heal publish of an edition or aggregate whose
  earlier publish was lost;
- an aggregate that a visit moved behind a short-circuited build, whose
  content Docverse already held;
- a `publish_edition` job enqueued before the field existed, whose
  payload has no `ltd_date_rebuilt`;
- an LTD edition with no `date_rebuilt`, for which
  `build_content_copied`'s `ltd_lag_seconds` is null too.

**Do not filter on `trigger`.** `edition_published` reports
`trigger=keeper_sync` only when the publish job belongs to a keeper-sync
*run*, the backfill `POST /orgs/{org}/keeper-sync/runs` launches. The
tier crons that poll LTD between runs enqueue their publishes without a
run, so those publishes read `trigger=build` although they carry an
`ltd_lag`. Keeper-sync's self-heal publishes, conversely, can read
`trigger=keeper_sync` with no lag. The presence of `ltd_lag` is the
marker of a publish that measured one, and an aggregate over `ltd_lag`
already sees only those; adding `"trigger" = 'keeper_sync'` narrows it
to backfill runs.

**Negative values are data.** Neither field is clamped. LTD's clock and
Docverse's disagree by some amount, and a lag shorter than that skew
comes out negative rather than as zero, which keeps the skew visible. A
run of negative values is a clock problem, not a fast sync.

## Event catalog

Every event `DocverseEvents.initialize` registers, with the flow that
emits it. "Scope" says which of `organization` and `project` the event
fills: project-scoped events carry both; org-scoped events leave
`project` null.

| Event | Emitted by | Scope |
| --- | --- | --- |
| [`build_uploaded`](#build_uploaded) | builds API handler | project |
| [`build_processed`](#build_processed) | `build_processing` worker | project |
| [`build_content_copied`](#build_content_copied) | `keeper_sync_project` worker | project |
| [`edition_published`](#edition_published) | `publish_edition` worker | project |
| [`dashboard_built`](#dashboard_built) | `dashboard_build` worker | project |
| [`project_lifecycle`](#project_lifecycle) | projects API handler | project |
| [`edition_lifecycle`](#edition_lifecycle) | editions API handler | project |
| [`membership_changed`](#membership_changed) | members API handler | organization |
| [`conditional_get`](#conditional_get) | conditional read endpoints | organization or project |
| [`api_request`](#api_request) | API middleware | optional |
| [`github_webhook_received`](#github_webhook_received) | GitHub webhook handler | none |
| [`keeper_sync_run_completed`](#keeper_sync_run_completed) | keeper-sync run finalisation | organization |
| [`lifecycle_action`](#lifecycle_action) | `lifecycle_eval`, `git_ref_audit`, `purgatory_cleanup` workers | project |
| [`purgatory_cleanup_completed`](#purgatory_cleanup_completed) | `purgatory_cleanup` worker | organization |
| [`edition_reconcile_completed`](#edition_reconcile_completed) | `edition_reconcile` worker | organization |
| [`resource_inventory`](#resource_inventory) | `inventory_census` worker | organization and project |

### `build_uploaded`

Measurement: `lsst.square.metrics.events.docverse.build_uploaded`

A client signalled that a build's upload is complete. Published by
`PATCH /orgs/{org}/projects/{project}/builds/{build}` when the build
moves from `pending` to `processing`. The provenance fields are copied
from the build's annotations, and are null where the uploader did not
annotate them.

| Field | Type | Stored as | Meaning |
| --- | --- | --- | --- |
| `organization` | string | tag | Organization slug. |
| `project` | string or null | tag | Project slug; always set on this event. |
| `uploader` | string | field | The user or bot that uploaded the build. A username, so never a tag. |
| `commit_sha` | string or null | field | The Git commit the build was produced from. |
| `github_repository` | string or null | tag | The `owner/repo` that produced the build. |
| `github_run_id` | string or null | field | The GitHub Actions run ID. |
| `github_actor` | string or null | field | The GitHub user or app that triggered the run. |
| `ci_platform` | string or null | tag | The CI platform that produced the build. |

### `build_processed`

Measurement: `lsst.square.metrics.events.docverse.build_processed`

The `build_processing` worker finished a build, at any of its three
terminal outcomes: a successful unpack and upload, a failure, and a
stale build skipped because a newer build for the same project and Git
ref superseded it.

| Field | Type | Stored as | Meaning |
| --- | --- | --- | --- |
| `organization` | string | tag | Organization slug. |
| `project` | string or null | tag | Project slug; always set on this event. |
| `success` | boolean | tag | Whether processing completed without error. |
| `object_count` | integer or null | field | Objects uploaded; null when nothing was. |
| `total_size_bytes` | integer or null | field | Bytes uploaded; null when nothing was. |
| `editions_updated` | integer | field | Editions repointed at this build. |
| `editions_skipped` | integer | field | Tracking editions left unchanged. |
| `stale_skipped` | boolean | field | Whether the build was skipped as superseded. |
| `elapsed` | duration | field | Time the worker spent on the build. |

### `build_content_copied`

Measurement: `lsst.square.metrics.events.docverse.build_content_copied`

The `keeper_sync_project` worker finished copying one build's content
from LTD into R2. Published once per copy, after it succeeds and after
it fails. A copy the build-level retry re-ran is still one event, with
its counters summed over both passes. [Keeper-sync transport
resilience](keeper-sync-transport.md#the-metrics-event) explains how to
read these events across a sync campaign.

| Field | Type | Stored as | Meaning |
| --- | --- | --- | --- |
| `organization` | string | tag | Organization slug. |
| `project` | string or null | tag | Docverse project slug; always set on this event. |
| `ltd_slug` | string | field | The LTD product the build was copied from. It differs from `project` when the organization rewrites slugs. |
| `object_count` | integer | field | Objects stored by the copy's last pass. |
| `total_size_bytes` | integer | field | Bytes stored by the copy's last pass. |
| `duration_seconds` | float | field | Seconds from the first pass starting to the last ending, including the build-level retry's wait. |
| `peak_concurrent_copies` | integer | field | Most objects in flight at once, over both passes. |
| `retried_object_count` | integer | field | Objects stored only after at least one upload retry. |
| `exhausted_object_count` | integer | field | Objects whose upload ran out of its whole retry budget. |
| `build_retry_used` | boolean | field | Whether the build-level retry re-ran the copy. |
| `succeeded` | boolean | field | Whether the copy, after any build-level retry, stored every object. Not named `success`, so not a tag. |
| `ltd_lag_seconds` | float or null | field | Seconds from LTD's `date_rebuilt` for the edition to the copy's end: the copy half of `ltd_lag`. Null when LTD reports no `date_rebuilt`; negative under clock skew. See [Measuring lag behind LTD Keeper](#measuring-lag-behind-ltd-keeper). |

### `edition_published`

Measurement: `lsst.square.metrics.events.docverse.edition_published`

An edition's current build finished publishing to the CDN. Published
from the `publish_edition` worker's success terminal only.

| Field | Type | Stored as | Meaning |
| --- | --- | --- | --- |
| `organization` | string | tag | Organization slug. |
| `project` | string or null | tag | Project slug; always set on this event. |
| `edition_kind` | enum | tag | The edition's kind: `main`, `release`, `draft`, `major`, `minor`, or `alternate`. |
| `trigger` | enum | tag | What drove the publish: `build` (a client build's fan-out, and keeper-sync's tier crons), `keeper_sync` (a keeper-sync run), `rollback`, or `reconcile` (the `edition_reconcile` loop re-driving a lost publish). |
| `elapsed` | duration | field | Time the worker spent on the publish. |
| `ltd_lag` | duration or null | field | Time from LTD's `date_rebuilt` for the edition to this publish's success, set when a keeper-sync visit imported a fresh rebuild. See [Measuring lag behind LTD Keeper](#measuring-lag-behind-ltd-keeper) for when it is null and why not to filter on `trigger`. |

### `dashboard_built`

Measurement: `lsst.square.metrics.events.docverse.dashboard_built`

The `dashboard_build` worker finished rendering a project's version
dashboard, at either terminal outcome.

| Field | Type | Stored as | Meaning |
| --- | --- | --- | --- |
| `organization` | string | tag | Organization slug. |
| `project` | string or null | tag | Project slug; always set on this event. |
| `success` | boolean | tag | Whether the dashboard build completed without error. |
| `object_count` | integer or null | field | Dashboard artifacts uploaded; null on failure. |
| `total_size_bytes` | integer or null | field | Bytes uploaded; null on failure. |
| `elapsed` | duration | field | Time the worker spent on the build. |

### `project_lifecycle`

Measurement: `lsst.square.metrics.events.docverse.project_lifecycle`

A project was created, updated, or deleted through the projects API.
Published by the handler after the operation's final commit.

| Field | Type | Stored as | Meaning |
| --- | --- | --- | --- |
| `organization` | string | tag | Organization slug. |
| `project` | string or null | tag | Project slug; always set on this event. |
| `action` | enum | tag | `create`, `update`, or `delete`. The enum's fourth value, `rollback`, applies only to editions and never appears here. |

### `edition_lifecycle`

Measurement: `lsst.square.metrics.events.docverse.edition_lifecycle`

An edition was created, updated, deleted, or rolled back through the
editions API. Published by the handler after the operation's final
commit.

| Field | Type | Stored as | Meaning |
| --- | --- | --- | --- |
| `organization` | string | tag | Organization slug. |
| `project` | string or null | tag | Project slug; always set on this event. |
| `action` | enum | tag | `create`, `update`, `delete`, or `rollback`. |
| `edition_kind` | enum | tag | The edition's kind: `main`, `release`, `draft`, `major`, `minor`, or `alternate`. |

### `membership_changed`

Measurement: `lsst.square.metrics.events.docverse.membership_changed`

An organization member was added or removed through the members API.
A `PATCH` that changes a member's role publishes a `remove` of the old
role followed by an `add` of the new one.

| Field | Type | Stored as | Meaning |
| --- | --- | --- | --- |
| `organization` | string | tag | Organization slug. |
| `project` | string or null | tag | Always null: membership is org-scoped. |
| `action` | enum | tag | `add` or `remove`. |
| `role` | enum | tag | The role the membership grants, or granted: `reader`, `uploader`, or `admin`. |
| `principal_type` | enum | tag | `user` or `group`. |
| `principal` | string | field | The username or group name. Never a tag. |

### `conditional_get`

Measurement: `lsst.square.metrics.events.docverse.conditional_get`

A read endpoint evaluated a request's HTTP preconditions. Published
only when the request carried a precondition header, so the stream
counts *conditional* traffic, and the ratio of `not_modified` to
`modified` within it is the cache hit rate. [Conditional
GET](api-conventions.md#conditional-get-the-etag-validator) covers the
validators themselves. `api_request` counts every request, conditional
or not, so the two answer different questions and do not double-count.

| Field | Type | Stored as | Meaning |
| --- | --- | --- | --- |
| `organization` | string | tag | Organization slug. |
| `project` | string or null | tag | Project slug for the single-project endpoint; null for the org-scoped ones. |
| `endpoint` | enum | field | Which endpoint evaluated the preconditions: `projects_list`, `project`, or `organization`. An endpoint identity rather than a route template, so a path change does not break the schema. |
| `outcome` | enum | tag | `not_modified` (answered 304) or `modified` (sent in full). |
| `precondition` | enum | field | Which header decided: `etag`, the only validator Docverse publishes. |

### `api_request`

Measurement: `lsst.square.metrics.events.docverse.api_request`

The API answered one HTTP request. Published once per response by
`ApiRequestMiddleware` (`src/docverse_server/middleware/api_request.py`)
for every request except those to `/` and `/health`, which sit outside
the Gafaelfawr ingress and are polled by Kubernetes probes. Unlike most
events it does not require an organization, because most routes
(`/orgs`, the admin routes, the GitHub webhook) address none.

- A path no route matched records `route` as null. FastAPI answers it
  with a `404`, so it counts as `4xx` volume without adding a `route`
  tag value. The documentation pages FastAPI serves itself
  (`openapi.json`, `docs`, `redoc`) record no template either.
- An exception that escapes the application is recorded as a `500`
  (`5xx`) and re-raised unchanged, so the caller still receives the
  `500` and Sentry still captures it.
- Publishing is best-effort. A publish that fails is logged (`Failed to
  publish api_request metrics event`) and swallowed, so a metrics
  outage never changes or fails a response.
- A GitHub webhook delivery is recorded here too, with `route` set to
  `/webhooks/github` and `authenticated` false, besides its own
  `github_webhook_received` event.

| Field | Type | Stored as | Meaning |
| --- | --- | --- | --- |
| `method` | string | tag | The HTTP method, upper-case (`GET`, `PATCH`). |
| `route` | string or null | tag | The matched route template without the application's path prefix: `/orgs/{org}/projects/{project}` for a request to `/docverse/orgs/rubin/projects/sqr-000`. Null when no route template matched. |
| `status_code` | integer | field | The HTTP status the response started with. |
| `status_class` | string | tag | The class of `status_code`: `1xx`, `2xx`, `3xx`, `4xx`, or `5xx`. A string rather than an Avro enum, because enum symbols cannot begin with a digit; the payload refuses any other value. |
| `duration` | duration | field | Seconds from the request reaching the API to its response starting, on the monotonic clock. It excludes streaming the body. |
| `authenticated` | boolean | tag | Whether Gafaelfawr's ingress set `X-Auth-Request-User`: true for a known user, false for anonymous traffic such as webhook deliveries. Only the header's presence is recorded, never the username in it. |
| `organization` | string or null | tag | The route's `org` path parameter, if it declares one. |
| `project` | string or null | tag | The route's `project` path parameter, if it declares one. |

### `github_webhook_received`

Measurement: `lsst.square.metrics.events.docverse.github_webhook_received`

The API received one GitHub webhook delivery at
`POST /docverse/webhooks/github`. Published once per delivery, whatever
became of it, just before the handler returns or raises. It records
what `api_request` cannot: the GitHub event type, and whether Docverse
acted on the delivery. Like `api_request` it does not require an
organization, because a delivery is recorded before it is resolved to
one. Publishing is best-effort, as for `api_request`.

| Outcome | Response | When |
| --- | --- | --- |
| `dispatched` | 200 | The event type is subscribed, and every callback ran. |
| `ignored` | 200 | The delivery is signed, but no callback subscribes to its event type, such as the `ping` GitHub sends when a webhook is set up. |
| `invalid_signature` | 401 | The delivery is unsigned, or its HMAC does not verify. |
| `not_configured` | 404 | This deployment has no GitHub App configured. |
| `error` | 500 | The delivery raised while being parsed or dispatched. The exception is re-raised unchanged, so Sentry still captures it. |

| Field | Type | Stored as | Meaning |
| --- | --- | --- | --- |
| `event_type` | string or null | tag | GitHub's `X-GitHub-Event` header, such as `push` or `ping`. Null for a delivery whose signature was not verified (`not_configured`, `invalid_signature`, or an `error` while parsing), because the header is caller-supplied. |
| `outcome` | enum | tag | What became of the delivery: `dispatched`, `ignored`, `invalid_signature`, `not_configured`, or `error`. |
| `jobs_enqueued` | integer | field | Background jobs the delivery's callbacks enqueued: the `dashboard_sync` jobs a `push` enqueues, or the `dashboard_build` jobs a `delete` enqueues. Zero for other event types and for a delivery that was not dispatched; on `error`, the jobs enqueued before the failure. |
| `elapsed` | duration | field | Time from the handler receiving the delivery to its outcome. |
| `github_repository` | string or null | tag | The signed payload's `repository.full_name` (`owner/repo`). Null for events that name no repository (`ping`, `installation`) and for an unverified delivery. |
| `organization` | string or null | tag | Reserved; always null. Kept so that resolving a delivery to its organization later is an additive change. |
| `project` | string or null | tag | Reserved; always null, like `organization`. |

### `keeper_sync_run_completed`

Measurement: `lsst.square.metrics.events.docverse.keeper_sync_run_completed`

A keeper-sync run reached a terminal status. Published by whichever
worker path finalises the run: `keeper_sync_project`, `publish_edition`,
or the keeper-sync reaper.

| Field | Type | Stored as | Meaning |
| --- | --- | --- | --- |
| `organization` | string | tag | Organization slug. |
| `project` | string or null | tag | Always null: a run spans many projects. |
| `success` | boolean | tag | Whether every job attributed to the run completed cleanly. |
| `total_count` | integer | field | Jobs attributed to the run. |
| `succeeded_count` | integer | field | Attributed jobs that completed cleanly. |
| `failed_count` | integer | field | Attributed jobs that failed or soft-failed. |
| `elapsed` | duration | field | Time from the run starting to its terminal status. |

### `lifecycle_action`

Measurement: `lsst.square.metrics.events.docverse.lifecycle_action`

A maintenance worker retired a resource: one event per reap. The
`lifecycle_eval` and `git_ref_audit` workers soft-delete rows;
`purgatory_cleanup` permanently reclaims a long-deleted build's
object-store content. One event type spans both, so a consumer can
follow a resource from the rule that retired it to the sweep that freed
its bytes.

| Field | Type | Stored as | Meaning |
| --- | --- | --- | --- |
| `organization` | string | tag | Organization slug. |
| `project` | string or null | tag | Project slug; always set, even when the project itself has been soft-deleted. |
| `action` | enum | tag | What drove the reap: `draft_inactivity`, `build_history_orphan`, or `ref_deleted` (the lifecycle rule that matched), or `retention_expired` (the organization's purgatory retention elapsing). |
| `trigger` | enum | tag | Which worker performed the reap: `lifecycle_eval`, `git_ref_audit`, or `purgatory_cleanup`. |
| `success` | boolean | tag | Whether the reap committed; always true today, since a reap is published only once it is durable. |

### `purgatory_cleanup_completed`

Measurement: `lsst.square.metrics.events.docverse.purgatory_cleanup_completed`

One organization's `purgatory_cleanup` tick finished. The counters are
per-tick deltas; the standing footprint the sweep has yet to reclaim is
`resource_inventory`'s `purgatory_bytes`.

| Field | Type | Stored as | Meaning |
| --- | --- | --- | --- |
| `organization` | string | tag | Organization slug. |
| `project` | string or null | tag | Always null: a tick spans every project in the organization. |
| `success` | boolean | tag | Whether every build the tick attempted was reclaimed. |
| `builds_purged` | integer | field | Builds whose content the tick deleted and whose row it stamped. |
| `builds_failed` | integer | field | Builds the tick attempted and could not complete. |
| `builds_skipped_referenced` | integer | field | Builds held back because a live edition still serves them. |
| `objects_deleted` | integer | field | Objects removed from under the purged builds' prefixes. |
| `bytes_reclaimed` | integer | field | Summed size of the builds the tick purged. |
| `capped` | boolean | field | Whether the per-job cap, not the backlog, ended the work list. |
| `elapsed` | duration | field | Time the sweep spent on the organization. |

### `edition_reconcile_completed`

Measurement: `lsst.square.metrics.events.docverse.edition_reconcile_completed`

One organization's `edition_reconcile` tick finished, including the
ticks that found nothing, so a quiet reconciler is distinguishable from
a stopped one. [Edition reconciliation](edition-reconcile.md#the-metrics-event)
explains each counter against the loop's decision table.

| Field | Type | Stored as | Meaning |
| --- | --- | --- | --- |
| `organization` | string | tag | Organization slug. |
| `project` | string or null | tag | Always null: a tick spans every project in the organization. |
| `editions_scanned` | integer | field | Editions the tick considered. |
| `pointers_read` | integer | field | Keys the organization's edge answered the read-back with; zero when `cdn_checked` is false. |
| `republished` | integer | field | Publishes the tick put back on the queue. |
| `unpublished` | integer | field | Stranded CDN keys the tick deleted. |
| `in_flight_skipped` | integer | field | Pairs a live `publish_edition` job still held. |
| `superseded_skipped` | integer | field | Planned republishes the edition had moved off before the enqueue. |
| `failed_left_alone` | integer | field | Pairs reading `failed`; reported, never re-driven. |
| `unexpected_pointers` | integer | field | Keys for editions with no build to publish; reported, not acted on. |
| `capped` | integer | field | Actions the per-job cap left for the next tick. |
| `cdn_checked` | boolean | field | Whether the tick read the organization's edge back at all. |
| `elapsed` | duration | field | Time the loop spent on the organization. |

### `resource_inventory`

Measurement: `lsst.square.metrics.events.docverse.resource_inventory`

The daily `inventory_census` snapshot: one org-scoped event per
organization (`project` null, `project_count` set) and one per
non-deleted project (`project` set, `project_count` null). Every field
is an absolute-count gauge, so query it with `LAST()`. A build counts
in `build_count` while it is live and in `purgatory_build_count` once
it is soft-deleted but not yet purged, never in both.

| Field | Type | Stored as | Meaning |
| --- | --- | --- | --- |
| `organization` | string | tag | Organization slug. |
| `project` | string or null | tag | Project slug on a project row; null on the organization row. |
| `project_count` | integer or null | field | Active projects in the organization; null on a project row. |
| `edition_count` | integer | field | Active editions in scope. |
| `build_count` | integer | field | Active builds in scope. |
| `total_build_bytes` | integer | field | Summed size of the active builds in scope. |
| `purgatory_build_count` | integer | field | Soft-deleted, not yet purged builds in scope. |
| `purgatory_bytes` | integer | field | Summed size of those purgatory builds. |

## Example queries

One InfluxQL query for each question PRD #713 added events to answer.
They assume the tag list above is in effect; replace `rubin` with the
organization's slug. Every time value comes back in seconds.

### Sync lag: p50 and p95 `ltd_lag` for an organization over 24 hours

```sql
SELECT PERCENTILE("ltd_lag", 50) AS "p50", PERCENTILE("ltd_lag", 95) AS "p95"
FROM "lsst.square.metrics.events.docverse.edition_published"
WHERE "organization" = 'rubin' AND time > now() - 24h
```

Only publishes that measured a lag contribute, which is why there is no
`trigger` filter; see [Measuring lag behind LTD
Keeper](#measuring-lag-behind-ltd-keeper). The copy half over the same
window, to subtract from it:

```sql
SELECT PERCENTILE("ltd_lag_seconds", 50) AS "p50", PERCENTILE("ltd_lag_seconds", 95) AS "p95"
FROM "lsst.square.metrics.events.docverse.build_content_copied"
WHERE "organization" = 'rubin' AND time > now() - 24h
```

### Request volume: requests per minute by `route` and `status_class`

```sql
SELECT COUNT("duration") AS "requests"
FROM "lsst.square.metrics.events.docverse.api_request"
WHERE time > now() - 1h
GROUP BY time(1m), "route", "status_class"
```

`COUNT` needs a field, and every `api_request` carries `duration`, so
counting it counts requests. Requests no route matched group under an
empty `route`.

### Latency: p95 `duration` by `route`

```sql
SELECT PERCENTILE("duration", 95) AS "p95"
FROM "lsst.square.metrics.events.docverse.api_request"
WHERE time > now() - 1h
GROUP BY "route", "status_class"
```

Grouping by `status_class` as well keeps fast answers, such as a `404`
or a `304`, from pulling a route's `2xx` latency down.

### Webhooks: deliveries per hour by `event_type` and `outcome`

```sql
SELECT COUNT("elapsed") AS "deliveries"
FROM "lsst.square.metrics.events.docverse.github_webhook_received"
WHERE time > now() - 24h
GROUP BY time(1h), "event_type", "outcome"
```

`invalid_signature` and `not_configured` deliveries carry no
`event_type`, so they group under an empty one.

## Changing the catalog

- **A new event** is a payload class in
  `src/docverse_server/metrics/payloads.py`, a publisher registered in
  `DocverseEvents.initialize`, and a section on this page. Payloads are
  scalar-only, because InfluxDB stores no nested structures.
- **An existing event** changes only by gaining a nullable field, as
  `edition_published` gained `ltd_lag` and `build_content_copied`
  gained `ltd_lag_seconds`. Every consumer reads the topic through the
  event's registered Avro schema, so renaming, retyping, or removing a
  field breaks them.
- **A new tag** is a Phalanx change to `influxTags`, released alongside
  the Docverse change that needs it and copied into the list above. It
  must follow the [cardinality rule](#cardinality-rule), and it tags
  every event with a field of that name.

## Keeping this page current

`tests/docs_test.py` reads this page and fails when:

- an event `DocverseEvents.initialize` registers has no section here,
  or its section does not name its measurement;
- a payload field is missing from its event's table, or the field's
  Type cell does not match its annotation;
- a Stored-as cell disagrees with the tag list above, or the list names
  a field no event carries;
- the Tags table misstates which events a tag applies to;
- a value an enum field (or `status_class`) can carry is not named in
  its event's section;
- one of the four example queries is missing;
- `index.md`, the transport page's metrics-event section, or the API
  conventions page's conditional GET section stops linking here.

The test cannot read Phalanx, so it checks the tag list as quoted here.
A change to `influxTags` in Phalanx has to be copied into this page by
hand.

## Related

- [SQR-112](https://sqr-112.lsst.io), the design of Docverse's
  metrics, and PRD #713, which added `ltd_lag`, `ltd_lag_seconds`,
  `api_request`, and `github_webhook_received`.
- `src/docverse_server/metrics/`: the payloads, their enums, the
  `DocverseEvents` registry, and `build_event_manager`.
- `src/docverse_server/middleware/api_request.py`: the middleware that
  publishes `api_request`.
- `src/docverse_server/handlers/webhooks/github.py`: the handler that
  publishes `github_webhook_received`.
- [Keeper-sync transport resilience](keeper-sync-transport.md#the-metrics-event):
  reading `build_content_copied` over a sync campaign.
- [Edition reconciliation](edition-reconcile.md#the-metrics-event):
  reading `edition_reconcile_completed`.
- [REST API conventions](api-conventions.md#conditional-get-the-etag-validator):
  the validators `conditional_get` reports on.
