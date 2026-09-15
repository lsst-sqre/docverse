# Docverse REST API conventions

This document records the conventions the Docverse REST API follows so
that new endpoints stay consistent with the ones already shipped. It is
written as a standalone Markdown file because the repository does not yet
have a documentation build; a future Sphinx/documenteer or mkdocs tree can
absorb it unchanged.

The conventions below are grounded in the handlers under
`src/docverse_server/handlers/`. When adding an endpoint, follow the pattern of
the closest existing handler and keep this document in sync.

## Async-action verb rule

Endpoints that trigger background work are named with a verb path segment
chosen by the *source of truth* the action reads from. The three verbs in
use each mean something specific:

- **`rebuild`** — regenerate a derived artifact from Docverse's own
  database state. Example:
  `POST /orgs/{org}/projects/{project}/dashboard/rebuild` and the
  org-wide `POST /orgs/{org}/dashboard/rebuild` regenerate dashboards from
  the projects and editions already recorded in Docverse.
- **`sync`** — pull from an external GitHub source. Example:
  `POST /orgs/{org}/dashboard-template/sync` and
  `POST /orgs/{org}/projects/{project}/dashboard-template/sync` re-fetch
  the bound dashboard template from its GitHub repository.
- **`refresh`** — re-fetch from LTD (the legacy LTD Keeper system).
  Example: `POST /orgs/{org}/keeper-sync/projects/{ltd_slug}/refresh`
  triggers an immediate re-fetch of one LTD product's state.

Pick the verb by asking "where does the fresh data come from?" — Docverse's
database (`rebuild`), GitHub (`sync`), or LTD (`refresh`). Do not
introduce a new async verb without a distinct source to justify it.

## Path parameters

Path parameters are **bare nouns** naming the resource at that position,
not `{noun}_id` or `{noun}_slug`. The parameter's actual type (slug,
Base32 identifier, label) is documented on the `Path(...)` alias in
`src/docverse_server/handlers/params.py`, not encoded in the URL:

```
/orgs/{org}/projects/{project}/builds/{build}
/orgs/{org}/members/{member}
/orgs/{org}/services/{service}
/orgs/{org}/keeper-sync/runs/{run}
```

- `{org}`, `{project}`, `{edition}` — resource slugs.
- `{build}`, `{job}`, `{run}`, `{tombstone}` — Base32-encoded
  identifiers.
- `{member}` — composite `{type}:{principal}` identifier (e.g.
  `user:someuser`).
- `{credential}`, `{service}` — labels.

**Exception:** `{ltd_slug}` keeps its qualified name. It names a slug
belonging to a foreign system (LTD), and the descriptive name signals that
it is not a native Docverse identifier. This exception is deliberate and
should not be "cleaned up" to a bare noun.

## Hypermedia links

Response bodies carry their own URLs so clients can navigate without
constructing paths. Every resource representation includes a **`self_url`**
field pointing at its canonical GET endpoint. Links to related resources
and sub-collections use a **`{relation}_url`** suffix, for example:

- `org_url`, `project_url`, `edition_url`, `build_url` — links to related
  resources.
- `projects_url`, `editions_url`, `members_url`, `services_url`,
  `credentials_url`, `builds_url`, `history_url` — links to
  sub-collections.
- `job_url` — link to the queue job a 202 response enqueued.
- `published_url`, `web_url` — externally-facing (non-API) URLs.

All `*_url` fields are absolute URLs, built from the incoming request so
they honour the deployment's scheme and host.

## Public identifiers: no database IDs on the wire

Response bodies never expose integer database row IDs. Resources that
need a public identifier carry a **Crockford Base32 public ID** (12
characters plus a 2-character checksum, hyphenated every 4, e.g.
`1txq-55pj-1x5m-16`), minted from the `public_id` column via
`docverse_server.domain.base32id`. Fields holding these IDs are named `id` on
the resource itself and `{relation}_id` when referencing another
resource (`job_id`, `build_id`, `keeper_sync_run_id`).

Resources carrying an `id` of their own: **organizations**,
**projects**, builds, queue jobs, keeper-sync runs, and keeper-sync
tombstones. IDs are minted in application code from a time-ordered
sequence rather than by the database, so they sort in creation order
and no integer row id has to reach the wire to get an ordered handle on
a resource. For organizations and projects the ID is informational
today — path parameters are still slugs — but it is stable for the life
of the row, so it stays the traceable identity if a slug is ever
renamed.

Numeric IDs from *foreign* systems are allowed but must be clearly
labelled as such in the field name and description: `ltd_id` (legacy
LTD Keeper), `github_owner_id` / `github_repo_id` /
`github_installation_id` (GitHub). When a response needs to point at
another Docverse resource, prefer a `{relation}_url` HATEOAS link (or
the resource's Base32 public ID) — never its integer row id.

## Example values in the generated OpenAPI schema

Every `*_url`, public-ID, and slug-like field declares
`Field(examples=[...])` so the rendered OpenAPI docs show realistic
values instead of `"string"`. The examples share one vocabulary,
defined as constants in
`client/src/docverse/models/_examples.py`:

- The example deployment is `https://example.org/docverse/api`.
- The example organization is `lsst`, its project `pipelines`, and its
  edition `v1`.
- Example Base32 IDs are genuine minted values (they round-trip through
  the real validators), one distinct constant per resource type so
  cross-references line up — e.g. a build's `self_url` embeds the same
  id shown in the build's `id` field.

When adding a field that fits this vocabulary, reuse the constants
rather than inventing new example values inline.

## Documenting enums

Pydantic emits an enum's **class docstring** as the description of the
enum's component schema in the OpenAPI document, but **member
docstrings are dropped**. Therefore every public (wire-visible) enum
documents its values as a bulleted list in the class docstring:

```python
class KeeperSyncTombstoneReason(StrEnum):
    """Why a ``keeper_sync_state`` row was tombstoned.

    - ``manual_delete`` — an operator soft-deleted ...
    - ``lifecycle_delete`` — an automated process ...
    """
```

Do not put value semantics only in member docstrings — they are
invisible to API consumers. The list format renders as Markdown in
Swagger UI and Redoc.

## OpenAPI tags

Operations are grouped under three tags, declared in
`src/docverse_server/main.py` and applied per sub-router in
`src/docverse_server/handlers/orgs/__init__.py`:

- **`orgs`** — organization-scoped resources: the org itself, members,
  credentials, services, keeper-sync, the org dashboard, and the
  org-scoped jobs collection (`/orgs/{org}/jobs`).
- **`projects`** — projects and their sub-resources (builds, editions,
  project dashboards and template overrides).
- **`admin`** — superuser organization administration.

There is deliberately no separate `jobs` tag: jobs are org-scoped
resources and document under `orgs`.

## Timestamp field naming

Timestamp fields are prefixed with **`date_`** and carry timezone-aware
values, for example `date_created`, `date_updated`, `date_uploaded`,
`date_completed`. This matches the `date_`-prefixed database columns (see
the coding conventions in `CLAUDE.md`).

## HTTP methods: PUT for config singletons, PATCH for resources

The choice between `PUT` and `PATCH` follows the nature of the target:

- **`PUT` for config singletons.** A configuration singleton that is
  naturally set as a whole is replaced with `PUT`. The
  `dashboard-template` bindings are **PUT-only**
  (`PUT /orgs/{org}/dashboard-template`,
  `PUT /orgs/{org}/projects/{project}/dashboard-template`): a binding is
  an atomic pointer, so full replacement is the only meaningful write.
  `PUT /orgs/{org}/keeper-sync` performs a full replacement of the
  keeper-sync configuration.

- **`PATCH` for partial updates with JSON-Merge-Patch semantics.**
  Resources and configs that support field-level edits expose a `PATCH`
  alongside (or instead of) `PUT`:
  - `PATCH /orgs/{org}` — update an organization's mutable fields.
  - `PATCH /orgs/{org}/members/{member}` — update a member's `role` only;
    `principal`/`principal_type` are immutable (changing identity is a
    delete plus re-add).
  - `PATCH /orgs/{org}/keeper-sync` — partial config update.

  **Merge-patch semantics** (house style): omitted fields are left
  untouched; a provided array field replaces the whole array (no append
  semantics). For example, `PATCH /orgs/{org}/keeper-sync` with only
  `{"enabled": false}` leaves `ltd_base_url` and `project_slugs`
  unchanged, while providing `project_slugs` replaces the entire list.

  **Explicit `null` is rejected, not honoured.** RFC 7386 merge-patch gives
  an explicit `null` remove-the-member semantics, but the fields these PATCH
  endpoints expose map onto non-nullable storage (a member's `role`, the
  keeper-sync config's `enabled` / `ltd_base_url` / `project_slugs`), so
  there is nothing to remove. Sending `null` for such a field is a client
  error and returns **422 Unprocessable Entity**; it is *not* silently
  treated as "unset". To leave a field unchanged, omit it from the request
  body. (The request models distinguish the two with a field validator that
  fires on an explicit `null` but is skipped for the unset default — see
  `OrgMembershipUpdate` and `KeeperSyncConfigUpdate` in the client models.)

## Asynchronous operations: 202 with a `Location` job URL

Operations that enqueue background work return **`202 Accepted`**. The
work is represented as a queue job, and the response sets a **`Location`**
header pointing at the job so a client can poll for progress:

- `POST .../dashboard/rebuild` (project-scoped),
  `POST /orgs/{org}/keeper-sync/runs`,
  `POST /orgs/{org}/keeper-sync/projects/{ltd_slug}/refresh`,
  `POST .../dashboard-template/sync` each enqueue a single job and set
  `Location` to that job's `job_url`.
- The batch `POST /orgs/{org}/dashboard/rebuild` enqueues one job per
  project, so there is no single job resource. Per RFC 7231 the 202
  `Location` names a status monitor for the request; here it points at the
  org-scoped jobs collection (`GET /orgs/{org}/jobs`). The response body
  is an object (`{"entries": [...]}`), never a bare array, so it can grow
  fields later.

## `Location` headers on created and enqueued resources

- **201 Created** responses set `Location` to the new resource's
  `self_url`. This applies to every create endpoint, e.g.
  `POST /admin/orgs`, `POST /orgs/{org}/projects`,
  `POST /orgs/{org}/projects/{project}/builds`, `POST /orgs/{org}/members`,
  `POST /orgs/{org}/services`, `POST /orgs/{org}/credentials`, and the
  `PUT` dashboard-template bindings when they create a binding.
- **202 Accepted** responses set `Location` to the job URL (or status
  monitor), as described above.

## Documented error responses

Client-error responses are declared with FastAPI's `responses=` argument
so they appear in the OpenAPI spec with safir's `ErrorModel` body shape,
rather than being left undocumented. The shared helper
`error_responses(*status_codes)` in `src/docverse_server/handlers/responses.py`
builds these declarations for:

- **403 Forbidden** — the caller lacks the role required for the
  operation.
- **404 Not Found** — a resource addressed by the request path does not
  exist.
- **409 Conflict** — the request conflicts with the current state of the
  resource (e.g. a rebuild is already queued).

Each operation declares the subset of these codes it can actually return.

## Pagination: keyset cursors with `Link` and `X-Total-Count`

Unbounded collections are paginated with **keyset (cursor) pagination**,
never offset/limit. The mechanics (see
`src/docverse_server/storage/pagination.py`):

- **Query parameters:** `cursor` (an opaque token copied from a previous
  response — clients must not construct or parse it) and `limit` (default
  `25`, maximum `100`).
- **Ordering:** a listing that offers more than one traversal takes an
  `order` parameter. The project listing accepts `slug` (ascending, the
  default), `date_created`, and `date_updated` (both newest-first). Each
  ordering has its own cursor type, so a cursor is only valid for the
  order that produced it.
- **Fuzzy search:** a listing that supports one takes `q`, which
  replaces the `order` traversal with a relevance ranking and pages
  through the same `Link` header. The project listing's other filters
  apply on the `q` path too.
- **`Link` response header:** carries `rel="next"` / `rel="prev"` URLs
  (RFC 8288) that already embed the correct cursor. Clients follow these
  rather than building their own paginated URLs.
- **`X-Total-Count` response header:** the total number of matching
  entries across all pages.
- Cursors are keyset-based on a stable sort key plus an `id` tiebreaker
  (e.g. `date_created DESC, id DESC` for builds/projects/jobs, `slug ASC`
  for project/edition listings), so pages stay stable under concurrent
  inserts.

Paginated listings include: projects, editions, builds, edition build
history, queue jobs (`GET /orgs/{org}/jobs`), and the keeper-sync
projects, editions, runs, and tombstones collections.

## Deliberately unpaginated listings

Some collections are **bounded by nature** and return a plain JSON array
with no `cursor`/`limit` parameters and no `Link`/`X-Total-Count` headers.
These are intentionally unpaginated because their size is limited by an
organization's team or configuration, not by user-generated content:

- **Members** — `GET /orgs/{org}/members` (`list[OrgMembership]`).
- **Credentials** — `GET /orgs/{org}/credentials`
  (`list[OrganizationCredentialResponse]`).
- **Services** — `GET /orgs/{org}/services`
  (`list[OrganizationServiceResponse]`).
- **Admin orgs** — `GET /admin/orgs` (`list[AdminOrganization]`).
- **Non-admin orgs** — `GET /orgs` (`list[OrganizationSummary]`), which
  lists the organizations where the caller has an effective role. It is
  bounded by the caller's memberships (a superadmin sees all orgs), so it
  is unpaginated to match the other bounded listings; an empty list is a
  valid response.

If one of these collections ever grows unbounded, migrate it to the keyset
pagination pattern above rather than adding offset paging.

## Conditional GET: `ETag` and `Last-Modified` validators

The read endpoints a consumer polls carry HTTP validators
([RFC 7232](https://www.rfc-editor.org/rfc/rfc7232)), so a pass that
finds nothing new is answered with an empty **`304 Not Modified`**
rather than a full representation. Three endpoints participate, each
with its own *watermark* — the instant it reports as `Last-Modified`
and folds into its `ETag`:

| Endpoint | Metrics `endpoint` value | Watermark |
| --- | --- | --- |
| `GET /orgs/{org}/projects` | `projects_list` | newest `date_updated` among the organization's projects, soft-deleted ones **included**, falling back to the organization's own `date_created` when it has none |
| `GET /orgs/{org}/projects/{project}` | `project` | the later of the project's `date_updated` and its default `__main` edition's, because the response embeds that edition |
| `GET /orgs/{org}` | `organization` | the organization's own `date_updated` |

`GET /orgs` is **deliberately excluded.** It is filtered by the caller's
memberships, so granting or revoking one changes what it returns
without moving any organization's clock; a validator there would hand a
poller a 304 over a listing that had in fact changed.

**Weak ETags.** Tags are weak — `W/"<32 hex characters>"`, the leading
half of a SHA-256 over a canonical tuple of validator material —
because they mark *semantic* equivalence rather than byte-for-byte
identity, so a serializer tweak that reorders JSON keys does not retire
one. The material always opens with the endpoint's identity and the
resource's `public_id`, so two resources can never collide on a tag,
and then names whatever else distinguishes the representation:

- The **project listing** hashes the request's whole query string,
  canonicalized by sorting. Every page and every filter combination
  therefore gets its own tag without the endpoint having to enumerate
  its own parameters — a parameter added later is covered the day it is
  added. It also hashes all three parts of the org's listing watermark
  — the newest `date_updated`, the project count, and the sum of every
  project's clock — rather than the newest clock alone, for the
  commit-order reason spelled out under [Commit-order skew and the
  overlap window](#commit-order-skew-and-the-overlap-window).
- The **single project** hashes its parsed `include_deleted` flag, the
  only parameter it takes. Hashing the *parsed* boolean means
  `?include_deleted=false` and the omitted default — the same
  representation — share a tag instead of churning one.

**Second granularity.** An HTTP date carries whole seconds, so
`Last-Modified` is necessarily the watermark truncated to the second,
and the `If-Modified-Since` comparison truncates the watermark the same
way before comparing. Comparing against the untruncated instant would
report "modified" forever, because a client can only ever echo back the
second it was told.

**`If-None-Match` takes precedence.** Per RFC 7232 §6, when
`If-None-Match` is present it is evaluated and `If-Modified-Since` is
not consulted at all — *even when the tags do not match*. The date is a
fallback for a client that holds no tag, never a second opinion. Within
`If-None-Match`, `*` matches any current representation and tags are
compared weakly, so `W/"x"` and `"x"` are the same tag. An
`If-Modified-Since` that cannot be parsed is **ignored** — the request
is answered as though it were unconditional — rather than rejected.

A 304 carries no body and repeats both validators, so a poller can take
them straight into its next request. Handlers evaluate the
preconditions as early as the watermark allows, inside the read
transaction, so the work the 304 skips is real: the project listing
never runs its page query at all, and `GET /orgs/{org}` never loads its
embedded service summaries. The single project is the exception — it
has to read the project row and its default edition to know its own
watermark — so there the saving is the serialized body rather than the
queries behind it.

The semantics live in `src/docverse_server/domain/conditional_get.py`
as pure functions over plain values (no request object, no database);
`src/docverse_server/handlers/conditional.py` is the thin glue that
reads the two request headers, sets `ETag` and `Last-Modified` on the
outgoing response, and hands back the ready-made 304. A new conditional
endpoint computes its watermark and calls that helper rather than
spelling the headers itself.

**Observability.** Every evaluation is logged at debug level, and a
`conditional_get` metrics event is published for each request that
actually carried a precondition header — an unconditional request emits
nothing, so the event stream counts *conditional* traffic and the ratio
within it is the cache hit rate. Beyond the `organization` and
`project` dimensions every Docverse event carries, it records:

- `endpoint` — `projects_list`, `project`, or `organization`, an
  endpoint identity rather than a route template, so a path change does
  not break the Avro contract.
- `outcome` — `not_modified` (answered 304) or `modified` (sent in
  full).
- `precondition` — `etag` when `If-None-Match` decided,
  `last_modified` when the date did.

## Soft-deleted resources and polling the project listing

Deleting a project is a **soft** delete: the row stays, stamped with a
`date_deleted`, and drops out of the default listing. A consumer that
mirrors Docverse's project set therefore needs to be able to see the
deletion itself rather than merely notice that a project stopped
appearing, which is what the parameters below are for.

**`include_deleted`** (boolean, default `false`) is accepted by both
`GET /orgs/{org}/projects` and `GET /orgs/{org}/projects/{project}`:

- On the listing it adds soft-deleted projects to the results and
  counts them in `X-Total-Count`, on the fuzzy-search (`q`) path as
  well as the ordered one.
- On the single project it turns what would otherwise be a 404 into a
  representation. Slugs are never reused after a delete — the
  `uq_projects_org_slug` constraint ignores `date_deleted` — so the
  widened lookup can only ever resolve the project that already owned
  the slug.
- It is **read-only**: `PATCH` and `DELETE` ignore it and still 404 on
  a soft-deleted project.

**`date_deleted`** is always present on the project resource — the
deletion timestamp for a project returned behind `include_deleted`, and
`null` for a live one. Always-present-and-nullable rather than omitted,
so a consumer can tell a deletion from a project it simply did not
receive this pass.

**`updated_since`** (a timezone-aware ISO 8601 timestamp) keeps only
projects whose `date_updated` is at or after the given instant. The
bound is **inclusive**: feeding back the newest timestamp from the
previous pass can then never skip a project written in that same
microsecond, at the cost of re-sending the newest rows once. A naive
timestamp names an ambiguous instant and is rejected with a **422**.

What moves a project's `date_updated` is the other half of that
contract. It advances on a metadata `PATCH`, on a GitHub-binding
resolve, on a soft delete, **and** on a repoint of the project's
default `__main` edition onto a new build — the last of those so that a
consumer watching the listing sees content changes and not only
metadata edits. That touch lives in `EditionStore.set_current_build`,
the single chokepoint every repoint routes through (build tracking,
keeper-sync, rollback), and runs in the same transaction as the
repoint. A repoint of any other edition, a `publish_status` flip, and a
repoint the stale-build or deleted-build guard refuses all leave the
project's clock alone.

Pair `updated_since` with `order=date_updated` for the "what changed?"
traversal, and with the conditional-GET validators above so that a pass
finding nothing new costs one watermark query and no body at all. The
`docverse` client library packages the whole idiom as
`DocverseClient.list_projects`, which follows the `Link` chain and
carries the validators back for the next poll.

### Commit-order skew and the overlap window

`date_updated` is stamped with PostgreSQL's `now()`, which is the
**transaction's start time**, not its commit time — and commit order is
not start order. A write that began before a poller's pass can
therefore become visible *after* that pass while wearing a timestamp
the pass has already gone by. Two consequences, and Docverse handles
them on opposite sides of the wire.

**On the server**, the listing's ETag does not rest on
`max(date_updated)` alone. A late commit below the maximum leaves the
maximum where it was, so a tag built from it would keep answering 304
about a change the poller has never seen. The validator therefore also
covers the org's project count and the sum of every project's
`date_updated`: a row that appears or disappears moves the count, and a
clock that moves anywhere at all — above or below the maximum — moves
the sum. All three are one joinless aggregate over the
`(org_id, date_updated, id)` index, so the cheap path stays cheap.
`Last-Modified` still carries the maximum, because that header has to
name an instant; the ETag is opaque and can say more, which is why
`If-None-Match` is the validator to poll with.

**On the client**, `updated_since` needs an **overlap window**: ask
from slightly earlier than the newest timestamp of the previous pass,
so a write that took a while to commit still falls inside the filter.
`DocverseClient.list_projects` backdates the caller's `updated_since`
by 60 seconds by default (`DEFAULT_UPDATED_SINCE_OVERLAP`, overridable
per call via `updated_since_overlap`; `timedelta(0)` disables it). A
direct HTTP caller should subtract a comparable window itself. The
price is that a filtered pass **re-sends rows** the previous pass
already delivered — the inclusive bound re-sends the boundary row
besides — so treat the listing as a set of upserts keyed on each
project's `id` rather than as a stream of distinct changes.
