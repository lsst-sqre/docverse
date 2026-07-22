# Docverse REST API conventions

This document records the conventions the Docverse REST API follows so
that new endpoints stay consistent with the ones already shipped. It is
written as a standalone Markdown file because the repository does not yet
have a documentation build; a future Sphinx/documenteer or mkdocs tree can
absorb it unchanged.

The conventions below are grounded in the handlers under
`src/docverse/handlers/`. When adding an endpoint, follow the pattern of
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
`src/docverse/handlers/params.py`, not encoded in the URL:

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
`error_responses(*status_codes)` in `src/docverse/handlers/responses.py`
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
`src/docverse/storage/pagination.py`):

- **Query parameters:** `cursor` (an opaque token copied from a previous
  response — clients must not construct or parse it) and `limit` (default
  `25`, maximum `100`).
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
