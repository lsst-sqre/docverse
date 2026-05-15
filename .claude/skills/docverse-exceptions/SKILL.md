---
name: docverse-exceptions
description: Author a new exception class in the lsst-sqre/docverse server. Use when adding a new exception, raising a new error type, creating a new error class, or making code throw a new exception inside the Docverse FastAPI app, workers, services, or storage layer. Codifies the DocverseSlackException base, the API-facing-identifier tag/context recipe, the to_sentry() override pattern, the matching SentryEventInfo test skeleton, and the docverse-client "no safir, no sentry" boundary.
---

# docverse-exceptions

Codifies the exception-authoring conventions established by PRD #338
(DM-54916). Every new server-side exception in `src/docverse/` is written
against this skill so the Sentry / Slack routing decisions don't decay
into ad-hoc `class FooError(Exception):` raises.

If anything in this skill conflicts with what you read in the current
tree, **trust the tree** — the modules cited below are the source of
truth — and update this skill in the same PR that introduces the new
convention.

## 1. Decision tree — which base class?

Pick the base by the error's audience, not by where it's raised:

1. **Is the error a 4xx user error** (malformed request, missing
   resource the caller named, unauthorised tenant)?

   → Subclass `safir.fastapi.ClientRequestError` (re-exported via
   `docverse.exceptions`). Examples already in the tree:
   `BadRequestError`, `NotFoundError`, `ConflictError`,
   `MissingConfigurationError`, `InvalidBase32IdError`,
   `PermissionDeniedError` (`src/docverse/exceptions.py:37-76`).

   These render as HTTP 4xx responses. **They do not go to Sentry or
   Slack** — routing every malformed-request-from-a-tenant into
   Sentry would drown the signal we care about. Do not migrate them
   onto `DocverseSlackException`.

2. **Is the error server-side** (worker job failure, integration
   outage, internal invariant violated, configuration missing in the
   process environment)?

   → Subclass `docverse.exceptions.DocverseSlackException`
   (`src/docverse/exceptions.py:26-34`). This is itself a
   `safir.slack.blockkit.SlackException` subclass, so every subclass
   inherits a sensible default `to_slack()` and `to_sentry()` and is
   merged onto Sentry events by Safir's `before_send_handler` without
   any wiring per raise site.

3. **Is the error in the `client/` package** (anything under
   `client/src/docverse/client/`)?

   → Subclass `docverse.client.DocverseClientError`
   (`client/src/docverse/client/_exceptions.py:16`). **Never**
   subclass `SlackException` or import `safir.slack`, `safir.sentry`,
   or `sentry_sdk` here. See §6 below.

If you can't pick between 1 and 2, ask yourself: *does an on-call
operator need to do something about this, or is this the API telling
the caller they got it wrong?* If the caller is at fault, it's a 4xx;
if the operator is, it's a `DocverseSlackException`.

## 2. Metadata-enrichment recipe — tags vs contexts vs attachments

Sentry exposes three slots for structured data on an event. Pick the
slot by **cardinality**, not by how the field reads:

| Slot | Cardinality | Aggregatable in the Sentry UI? | What to put here |
|---|---|---|---|
| **`tags`** | Low (a few dozen distinct values total) | Yes — index columns. | State names (`"processing"`, `"completed"`), queue names, HTTP status codes, methods, org slug, missing-secret enum values. |
| **`contexts`** | Medium-to-high (per-event structured snapshots) | No — visible per event, not indexed. | The full transition record, request URL + truncated body, lookup snapshot, installation id. |
| **`attachments`** | Unbounded (large blobs) | No. | Full LTD response bodies, large HTML payloads. Truncate at the constructor (see the module-level `_truncate_body` helper, `src/docverse/storage/ltd/client.py:47-65`) rather than relying on Sentry to do it. |

### API-facing identifiers only

Every identifier surfaced to Sentry must be **API-facing** — a slug, a
base32 `public_id`, or another value a triager can paste straight into
a `GET /v1/orgs/{org}/projects/{project}/builds/{build_id}` URL.
**Never** surface a database row id: it makes the Sentry event
untranslatable without DB access.

- Good: `org_slug`, `project_slug`, `edition_slug`, `build_public_id`,
  `job_public_id` (base32), `queue_name`, `job_function`,
  `missing_secret` (a `Literal` enum value).
- Bad: `org_id`, `project_id`, the integer PK of a `builds` row.

### Tags — keep them low-cardinality

Tag values are indexed. If a value is unbounded (request URLs, free-form
messages, full payload snippets, `installation_id` integers that grow
without bound), it belongs in a context, not a tag. Concrete bar from
the merged code:

- `InvalidBuildStateError` tags `org_slug`, `project_slug`,
  `build_current_state`, `build_target_state` —
  `build_public_id` goes into the `build_transition` context
  (`src/docverse/exceptions.py:205-225`).
- `JobNotFoundError` tags only `queue_name` — `job_public_id` and
  `job_function` go into the `queue_job_lookup` context
  (`src/docverse/exceptions.py:284-295`).
- `LtdClientError` tags `ltd_status_code` (cast to `str`) and
  `ltd_method` — URL and (truncated) body go into the `ltd_request`
  context (`src/docverse/storage/ltd/client.py:104-116`).
- `GitHubAppNotConfiguredError` tags `missing_secret` and (when
  known) `org_slug` — `installation_id` and the static app name go
  into the `github_app` context
  (`src/docverse/storage/github/app_client.py:101-112`).

### Omit `None` values from tags

Tag values are strings and a literal `"None"` is worse than absent.
Every existing override gates the assignment on `is not None`:

```python
if self.org_slug is not None:
    info.tags["org_slug"] = self.org_slug
```

Contexts may carry `None` values verbatim — the context's job is to
surface "what did the exception know about", and a missing field is
information. Compare the tag-gating above to the context block from
the same class (`src/docverse/exceptions.py:216-224`), which writes every field
unconditionally.

## 3. Constructor pattern

Every server-side exception that overrides `to_sentry` follows the same
shape. **Copy the template in
[`references/constructor-pattern.md`](references/constructor-pattern.md)**,
rename the class and fields, and verify against `InvalidBuildStateError`
(`src/docverse/exceptions.py:180-204`) when in doubt.

Five things to copy verbatim from that template:

1. **All fields are keyword-only and all default to `None`.** Worker
   raises rarely have every identifier in scope; making them required
   would force `# type: ignore` workarounds at raise sites.
2. **`message` is the last kwarg and defaults to `None`.** The
   `_format_message` staticmethod renders a useful default when the
   caller omits it; explicit `message=` overrides the default (used
   when the caller wants to surface an internal row id in logs
   without leaking it into Sentry tags — see the `JobNotFoundError`
   docstring at `src/docverse/exceptions.py:260-263`).
3. **`_format_message` is a staticmethod.** It lives on the class so
   the formatting logic is colocated with the fields it formats; the
   `@staticmethod` decorator avoids the leading `self` arg.
4. **`to_sentry` calls `super().to_sentry()` first**, then mutates
   `info.tags` and `info.contexts`. Never construct a fresh
   `SentryEventInfo` — Safir's base sets defaults you want to keep.
5. **`@override` from `typing`** decorates `to_sentry`. It's a free
   correctness check that the method actually overrides something.

For exceptions whose identifying field is a `Literal` (e.g.
`MissingGitHubAppSecret = Literal["app_id", "private_key",
"webhook_secret"]`), declare the alias at module scope and reuse it
both as the constructor kwarg type and as the instance attribute type
— see `src/docverse/storage/github/app_client.py:39` and
`src/docverse/storage/github/app_client.py:97`.

## 4. When to add a `to_sentry()` override — and when *not* to

Adding the override is a deliberate choice. The default `SlackException`
behaviour (no extra tags or contexts; the stack trace alone is the
event) is enough for **most** exceptions. An override earns its keep
only when a category of error is **genuinely hard to triage from the
stack trace alone**.

The bar is set by the five overrides that exist in the tree at the
time this skill was written:

| Exception | Why the override exists |
|---|---|
| `InvalidBuildStateError` (`src/docverse/exceptions.py:166`) | A bad build transition can fire from a worker job *or* an API write — the override carries `org_slug` / `project_slug` / `build_public_id` so triage doesn't have to walk the stack to find which build. |
| `InvalidJobStateError` (`src/docverse/exceptions.py:79`) | Same shape for queue jobs and run-state transitions; `queue_name` and `job_public_id` keep the link to `/queue/{job_id}` one click. |
| `JobNotFoundError` (`src/docverse/exceptions.py:254`) | A "row missing" failure tells you nothing without the lookup parameters; the override carries them so you don't need pod logs to know which row was missed. |
| `LtdClientError` (`src/docverse/storage/ltd/client.py:68`) | Tells a 5xx on the LTD side apart from a stale Docverse credential — both surface as the same exception type, the tags and request snapshot are the disambiguator. |
| `GitHubAppNotConfiguredError` (`src/docverse/storage/github/app_client.py:63`) | Routes the alert to the operator who can fix it — `missing_secret` says which secret was unset; `org_slug` says which tenant. |

Counter-example — when *not* to add one:

- `InvalidSlugError` (`src/docverse/domain/slug.py:52-58`) is a
  `DocverseSlackException` with **no** `to_sentry` override. The
  exception carries `slug` and `reason` fields and renders them in
  `str(exc)`, which the default `to_slack()` and `to_sentry()` pick
  up. The slug already names what failed; there is nothing for an
  override to add. Don't add one just for symmetry.

Gafaelfawr's entire codebase has exactly one `to_sentry()` override.
Five is what Docverse needs *today*, given its surface area. The next
override you add should clear the same bar: a category of error
genuinely hard to triage from the stack alone.

If a future raise site has a real exception in hand and the override
already carries everything the site knows — that's the point. The
matching `sentry_sdk.capture_exception(exc)` call site (see the
worker-functions pattern, e.g.
`src/docverse/worker/functions/build_processing.py:218-221`) does **not** need to
re-stamp tags or contexts: the override does it.

## 5. Test skeleton

Tests for `to_sentry` overrides live next to the module the override
enriches (LTD next to `client.py`, GitHub next to `app_client.py`).
The shared `DocverseSlackException` base + the
`exceptions.py`-resident overrides (build state, job state,
job-not-found) live in `tests/exceptions_test.py`. New subclass-specific
overrides should follow that placement rule.

Every test inspects the `SentryEventInfo` returned by
`exception.to_sentry()` directly — **never** call `sentry_sdk.init`
from a unit test. The contract is the override's return value, not
the SDK's transport.

Use the parametrized shape in
[`references/test-skeleton.md`](references/test-skeleton.md), drawn from
the parametrized `to_sentry` tests in `tests/exceptions_test.py:108-179`.
(`tests/storage/ltd/client_test.py:209-243` shows the same
direct-inspection assertions — `assert info.tags == {...}` against the
`to_sentry()` return — but as two plain, non-parametrized tests.)

Three rules:

1. **Assert tag equality, not subset.** `assert info.tags == expected_tags`
   catches a stray tag a future override accidentally adds. The
   second test's `assert "foo_public_id" not in info.tags` is belt-
   and-suspenders against the "promote a context field to a tag"
   mistake.
2. **The factory is a lambda or a `_make_*` function.** Parametrize
   over factories rather than over `InvalidFooStateError(...)`
   instances; instances built at parametrize-collection time hold a
   reference to the class and make pytest reports harder to read.
3. **Add one membership test for the migration shape.** Every new
   `DocverseSlackException` subclass should be added to the
   `_FACTORIES` parametrization in `tests/exceptions_test.py`
   (lines 66-71 at the time of writing). The two tests in that file
   that consume `_FACTORIES`
   (`test_migrated_exception_is_docverse_slack_exception` and
   `test_migrated_exception_renders_default_to_slack`) pin the
   "derives from both bases, default `to_slack()` renders" contract
   for every subclass at once.

The end-to-end Sentry-capture path (a real `sentry_init_fixture` +
`capture_events_fixture` integration test) is exercised once per
worker-function class in `tests/worker/keeper_sync_project_test.py`
(see `test_keeper_sync_project_failure_captures_to_sentry`); a new
exception class does **not** need its own integration test unless it
introduces a new capture *site*, only a new shape.

## 6. The `docverse-client` boundary — do not touch

The `client/` package (`client/src/docverse/client/`) is **not**
Docverse server code. It runs in end-user CI jobs, the GitHub Action
that wraps it, and any other process that pulls
`docverse-client` from PyPI. Telemetry from those processes must not
report to the Docverse Sentry tenant.

**Hard rules** for `client/`:

- Do **not** subclass `safir.slack.blockkit.SlackException` here.
- Do **not** import `safir.slack`, `safir.sentry`, or `sentry_sdk`
  here. As of this writing, a grep for these imports under
  `client/src/` returns zero matches; keep it that way.
- Do **not** add a `to_sentry()` method anywhere in `client/`.
- Do subclass `docverse.client.DocverseClientError` for new
  client-side errors. Its signature accepts a free-form message and
  an optional `status_code` for HTTP-derived errors
  (`client/src/docverse/client/_exceptions.py:16-31`). For
  errors that need richer structure, add fields to the subclass —
  see `BuildProcessingError`
  (`client/src/docverse/client/_exceptions.py:34-47`) for the
  pattern (extra kwarg, `__init__` stores it, no Sentry plumbing).

Why: PRD #338 user story 15 calls out that the client must remain a
pure HTTP client with no Sentry dependency. Pulling in
`safir.slack.blockkit.SlackException` to inherit `to_sentry()` would
re-export `sentry_sdk` from the client wheel and silently send
exceptions raised in user CI to our tenant. The skill's "do not
touch" rule is the durable guard; the test that asserts the bare grep
result above lives at the team's discretion.

## 7. Wiring summary — checklist for a new server-side exception

1. **Pick the base** (§1). For a server-side error, subclass
   `DocverseSlackException`; place the class either in
   `src/docverse/exceptions.py` (cross-cutting transition / lookup
   errors) or in the module that owns the integration (e.g.
   `src/docverse/storage/ltd/client.py` for LTD-specific errors).
2. **Write the structured constructor** (§3). All identifying kwargs
   are keyword-only and default to `None`. Place a `_format_message`
   staticmethod next to it.
3. **Decide whether to override `to_sentry`** (§4). If the
   stack-trace-alone test fails, override; otherwise stop here.
4. **Override `to_sentry`** if you decided to (§3 template). Tag
   low-cardinality API-facing identifiers; context the full
   structured snapshot. Truncate large blobs in the constructor (§2).
5. **Add the class to `tests/exceptions_test.py::_FACTORIES`** for the
   default-rendering / inheritance pins (§5).
6. **Write the `to_sentry` override tests** in the matching `_test.py`
   for the module the override lives in (§5).
7. **Raise it**, and at any worker / service site that catches it and
   converts it to a job-failure row, add
   `sentry_sdk.capture_exception(exc)` immediately above
   `logger.exception(...)` (see `src/docverse/worker/functions/build_processing.py:218-221`
   for the canonical shape). The `to_sentry` override does the rest.
8. **Never** touch `client/src/docverse/client/` in the same PR
   without a separate reason (§6).
