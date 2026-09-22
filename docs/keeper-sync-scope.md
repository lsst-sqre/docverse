# Scoping the keeper sync

An organization's keeper-sync **scope** is the set of LTD Keeper product
slugs Docverse will import and keep in step. It is four fields on the
org's keeper-sync config, and it is what lets a migration the size of
lsst.io — roughly 1640 LTD products — go over in waves by document
series instead of all at once.

This page is for operators: the rule the four fields compose into, what
a pattern does and does not match, what happens to a project that falls
out of scope, and the preview → `PATCH` → backfill workflow a wave
actually uses.

## The four fields

Every field lives on the org's config at `/orgs/{org}/keeper-sync`
(`GET` to read, `PUT` to replace the whole config, `PATCH` to merge).
All four default to empty, so an organization configured before these
fields existed reads back with the same scope it always had.

| Field | Effect | Example |
| --- | --- | --- |
| `project_slugs` | Exact LTD slugs to sync, or `"*"` for every product on the LTD instance | `["sqr-112", "dmtn-001"]` |
| `project_slug_patterns` | Regular expressions that **add** slugs to the scope | `["sqr-\\d+", "dmtn-\\d+"]` |
| `exclude_project_slugs` | Exact slugs **removed** from the scope | `["www"]` |
| `exclude_project_slug_patterns` | Regular expressions that **remove** slugs from the scope | `["test-.*"]` |

`project_slugs` is unchanged from before this feature: it is either a
list or the string `"*"`, never both.

## The rule

A slug is **included** when any of these holds:

- `project_slugs` is `"*"` (every product on the LTD instance), or
- the slug is listed in `project_slugs`, or
- the slug fully matches one of `project_slug_patterns`.

A slug is **in scope** when it is included and *neither*

- listed in `exclude_project_slugs`, *nor*
- fully matched by one of `exclude_project_slug_patterns`.

**Excludes always win.** A slug that is both included and excluded — by
any combination of the four fields — is out of scope. There is no
precedence to reason about beyond that one sentence, and no ordering
among the fields: the scope is a set difference, not a sequence of
rules applied in order.

Combining `"*"` with include patterns is allowed; the patterns are
simply redundant, because the wildcard has already included everything
they could match. The combination worth reaching for is `"*"` plus
excludes:

```json
{"project_slugs": "*", "exclude_project_slugs": ["www"]}
```

which syncs every LTD product except `www`.

The rule is defined once, on `KeeperSyncConfig.resolve_scope` — and its
`is_in_scope` / `filter_in_scope` wrappers — in the client package, and
every consumer (run discovery, the three tier crons, and the
per-project endpoints) calls those methods. Scope resolution preserves
LTD listing order, so successive passes over the same LTD instance fan
their work out deterministically.

## What a pattern matches

Patterns are **Python regular expressions**, matched with
`re.fullmatch` and **case-sensitively**. Glob syntax is not accepted,
and neither is any other pattern language.

`fullmatch` is the part most worth internalising: the pattern has to
consume the *entire* slug, so a pattern is not a prefix filter.

| Pattern | Matches | Does not match |
| --- | --- | --- |
| `sqr-1` | `sqr-1` | `sqr-10`, `sqr-100`, `SQR-1` |
| `sqr-\d+` | `sqr-1`, `sqr-060`, `sqr-112` | `sqr-`, `dmtn-001`, `SQR-112` |
| `sqr-.*` | `sqr-1`, `sqr-112`, `sqr-` | `dmtn-001` |
| `test-.*` | `test-`, `test-fixture` | `mytest-1` |

Because the whole slug has to match, `sqr-1` is a safe way to name
exactly one product — you do not need to anchor it with `^` and `$`,
and adding them changes nothing.

Two caps bound what a pattern field may hold:

- at most **100** entries per pattern field, and
- at most **256** characters per pattern.

These caps, rather than a match timeout or an alternative regex engine,
are deliberately the whole mitigation for a pathological pattern:
patterns are supplied by org admins and are matched against short LTD
slugs.

### Validation

Patterns are validated on the config model, so `PUT`, `PATCH` and the
scope preview all reject the same things identically. A pattern that
does not compile, one that is over-long, or a field with more than 100
entries is a **422** whose message names both the offending field and
the pattern:

```json
{"project_slug_patterns": ["sqr-("]}
```

```
project_slug_patterns pattern 'sqr-(' is not a valid Python regular
expression: missing ), unterminated subpattern at position 4
```

Nothing is stored when validation fails — the stored config is left
exactly as it was.

On `PATCH`, each of the four fields follows `project_slugs`: a field
that is present **replaces the stored list wholesale** (there is no
append semantics — send the full desired list), a field that is omitted
is left unchanged, and an explicit JSON `null` is rejected with a 422.

## When a project falls out of scope

A synced project can leave the scope in several ways — you add it to
`exclude_project_slugs`, you remove the pattern that was including it,
or you narrow `project_slugs`. Whichever way, the outcome is the same
and it is deliberately gentle:

- **It stops being synced.** Run discovery and all three tier crons
  resolve the scope from the stored config on every tick, so the next
  tick simply does not fan out a job for it.
- **Its keeper-sync status endpoints answer 404**, with a message
  reading `is not in the keeper-sync scope`. That covers
  `GET /orgs/{org}/keeper-sync/projects/{ltd_slug}`, its
  `/orgs/{org}/keeper-sync/projects/{ltd_slug}/editions`, and
  `POST /orgs/{org}/keeper-sync/projects/{ltd_slug}/refresh`. A
  `{ltd_slug}` that LTD Keeper could not have issued in the first place
  — anything outside `^[a-z][-a-z0-9]*[a-z0-9]$`, or longer than 255
  characters — is a **422** from those same three routes instead: the
  scope is never consulted about a slug that names no LTD product.
- **Nothing else changes.** The Docverse project, its editions, its
  builds and its keeper-sync state rows are all left alone, and
  `https://<slug>.<base_domain>/` keeps serving exactly what it served
  before. Falling out of scope is **not** a delete and writes **no**
  tombstone.
- **Re-including it resumes syncing.** Put the slug back — by pattern
  or by name — and the next tier-cron tick picks it up again from the
  state rows it never lost. There is no tombstone to clear first and no
  backfill to re-run, though a backfill is the fastest way to catch it
  up.

The collection listing
`GET /orgs/{org}/keeper-sync/projects` is deliberately **not**
scope-filtered: it lists every project-resource state row on the org,
so a project that has fallen out of scope still appears there. That
asymmetry is the feature — the listing is how you find a project whose
detail endpoint has started answering 404.

## The wave workflow

A wave is three steps, in this order:

1. **Preview the candidate scope** —
   `POST /orgs/{org}/keeper-sync/scope-preview`
2. **Save it** — `PATCH /orgs/{org}/keeper-sync`
3. **Launch a backfill** — `POST /orgs/{org}/keeper-sync/runs`

### Why preview first

Because **saving a wider scope is not inert**. The tier crons act on
the stored config at their next tick — `tier_main` every 5 minutes,
`tier_discovery` every 30, `tier_other` hourly — so a `PATCH` that
widens the scope starts importing the newly in-scope products within
minutes, whether or not you ever launch the backfill. A `"*"` saved by
mistake against lsst.io is 1640 products' worth of sync work already
in flight.

The preview is the check that costs nothing. It merges the candidate
body over the stored config with exactly the merge `PATCH` uses,
validates it exactly as `PATCH` validates it, resolves it against the
*live* LTD product listing, and **persists nothing and enqueues
nothing**. An empty body previews the stored config as it stands. The
preview works whether or not sync is `enabled`, so you can stage a
scope before turning sync on at all.

### The preview always uses the stored LTD URL

`ltd_base_url` is the one `PATCH` field the preview body does **not**
accept. The listing is always fetched from the LTD instance named by
the **stored** config — or from the model default,
`https://keeper.lsst.codes`, when the org has no stored config yet.
Sending `ltd_base_url` in a preview body is a **422** naming the
field, and nothing is fetched:

```json
{"project_slugs": "*", "ltd_base_url": "https://keeper.example.org/"}
```

```
ltd_base_url is not accepted by the scope preview; the preview always
resolves the scope against the stored config's LTD instance. Change it
with PUT or PATCH /orgs/{org}/keeper-sync instead.
```

The reason is that a preview resolves a *scope*, and the LTD instance a
scope is resolved against is not part of one. Honouring a candidate
base URL would have the server issue an outbound request to an
arbitrary operator-supplied host from inside the cluster and report the
result back synchronously — which is a probe for whatever the pod can
reach, not a scope check. Repointing the LTD instance stays what it
always was: a `PUT` or `PATCH` you actually save, and then preview.

The preview also holds **no database transaction** while it waits on
LTD — it takes one short transaction to read the config, releases it,
fetches, and takes a second to read the sync state. That is why
retrying a preview against a slow or hanging LTD costs nothing but the
outbound requests: it cannot leave pooled Postgres connections sitting
idle in transaction behind an httpx timeout.

If the live product listing cannot be read, the preview reports a
**502** — never a Docverse 500. That covers every way the listing can
fail, not just the obvious one: LTD unreachable, LTD answering an
error, and LTD answering `200` with something that is not a product
listing, which is what a proxy or maintenance page in front of it
serves. The response names LTD's own status (a `200` here is the
useful surprise) and what could not be read, so you can tell an LTD
outage from a Docverse bug without pod logs.

### Wave 1 — the SQR series

Preview it:

```
POST /orgs/{org}/keeper-sync/scope-preview
```

```json
{"project_slugs": [], "project_slug_patterns": ["sqr-\\d+"]}
```

Note the doubled backslash: `\d` is not a JSON escape, so a regex
reaches the API as `"sqr-\\d+"` and is stored as the pattern `sqr-\d+`.

Read the response (see below). When it says what you expect, save it
with the identical body:

```
PATCH /orgs/{org}/keeper-sync
```

```json
{"project_slugs": [], "project_slug_patterns": ["sqr-\\d+"]}
```

Then launch the import:

```
POST /orgs/{org}/keeper-sync/runs
```

From here on, an `sqr-NNN` product created on LTD is picked up by the
tier crons with no further config change — that is the point of syncing
by pattern rather than by list.

### Wave 2 — add the DMTN series

Each pattern field is replaced wholesale, so a second wave re-sends the
first wave's patterns alongside the new one:

```json
{"project_slug_patterns": ["sqr-\\d+", "dmtn-\\d+"]}
```

Preview it, `PATCH` it, and launch another backfill. What the backfill
then fans out is the preview's `in_scope_count` less its
`tombstoned_slugs` — the identity spelled out under
[Observability](#observability) — and the run's `total_count` is that
plus one, because the discovery job attributes itself to its own run.
A slug whose per-project job is already running is skipped as well, so
`total_count` can come in lower still.

### Wave 3 — everything, minus the strays

The last wave is usually the wildcard with a short exclude list, which
is also how you keep a stale LTD product out without having to delete
it in Keeper first:

```json
{"project_slugs": "*", "exclude_project_slugs": ["www"]}
```

Leaving `project_slug_patterns` set here is harmless — `"*"` has
already included everything those patterns would match — but sending
`{"project_slug_patterns": []}` in the same `PATCH` keeps the stored
config honest about what is actually doing the work.

## Reading a preview

```json
{
  "ltd_count": 1640,
  "in_scope_count": 3,
  "in_scope_slugs": ["sqr-060", "sqr-112", "dmtn-201"],
  "new_slugs": ["dmtn-201"],
  "tombstoned_slugs": ["sqr-060"],
  "unmatched_project_slugs": ["sqr-9999"],
  "unmatched_exclude_project_slugs": ["wwww"]
}
```

| Field | What it tells you |
| --- | --- |
| `ltd_count` | How many distinct product slugs the live LTD instance listed. A sanity check on the upstream, and the denominator for the wave. |
| `in_scope_count` | The size of `in_scope_slugs`: everything the config admits, tombstones included. Subtract `tombstoned_slugs` to get the child jobs a backfill would fan out. |
| `in_scope_slugs` | The resolved scope, in LTD listing order — the order a backfill would work through. Includes slugs a tombstone will make sync skip. |
| `new_slugs` | In-scope slugs with no keeper-sync state row on this org yet: exactly what the next backfill would import **for the first time**. |
| `tombstoned_slugs` | In scope by config, but skipped because the project-resource state row is tombstoned. |
| `unmatched_project_slugs` | `project_slugs` entries the LTD listing does not contain — the typo catcher for the include list. |
| `unmatched_exclude_project_slugs` | `exclude_project_slugs` entries the LTD listing does not contain. |

Four of these deserve more than a row.

**`new_slugs` is the one to size a wave by.** `in_scope_count` counts
everything the config admits, including the projects already synced
from previous waves; `new_slugs` is the delta this wave actually adds.
On a second or third wave the two numbers diverge sharply, and it is
`new_slugs` that predicts the import load.

**`tombstoned_slugs` explains a shortfall before you see it.** A
tombstoned project is in scope by config and will still be skipped by
sync, so a backfill fans out fewer children than `in_scope_count`
suggested — exactly this many fewer, which is what the run then
reports as its `fan_out_count`. If a slug listed here should in fact
sync, clear its
tombstone with
`DELETE /orgs/{org}/keeper-sync/tombstones/{tombstone}` — widening the
scope will not do it, because the tombstone is a veto that outranks the
config.

**`unmatched_project_slugs` catches the silent mistakes.** A misspelled
entry in `project_slugs` admits nothing, and that error shows up
nowhere in the resolved scope — the scope just quietly comes out
smaller than you meant. Anything listed here is a slug you named that
LTD does not have, so it is almost always a typo: fix the spelling and
preview again.

**`unmatched_exclude_project_slugs` is the same check with a different
meaning**, which is why it is a separate list rather than being folded
into the one above. An `exclude_project_slugs` entry LTD does not list
holds nothing back, and that is either:

- a **typo** — the dangerous case. The product you meant to keep out
  has a slug you never actually excluded, so it is in scope and the
  next backfill or tier tick will sync it. Check it against
  `in_scope_slugs`.
- a **stale entry** for a product since deleted from Keeper. Harmless:
  there is nothing left to exclude. Drop the entry from the config
  whenever you next `PATCH` it, so the list keeps telling you the
  truth.

Nothing in the response distinguishes those two, because Docverse
cannot tell them apart — only you know whether the slug was ever meant
to exist. What the split does guarantee is that you are asked the
question about the right field.

Neither list checks the *pattern* fields: a pattern that matches
nothing is a legitimate way to stage a future wave, and `"*"` names no
slugs at all. So two empty unmatched lists with an `in_scope_count` of
zero mean your patterns matched nothing — check `fullmatch`, and check
the case.

## Observability

Both places that resolve a scope log it at info with the same five
counts, so a tier tick and a run's discovery job are directly
comparable — and comparable with a preview, which reports three of
them under the same names:

- `Resolved keeper-sync run scope` (run discovery) and
  `Resolved keeper-sync tier scope` (each tier cron), both carrying
  `ltd_count`, `in_scope_count`, `excluded_count` — how many slugs an
  include rule admitted and an exclude rule then removed —
  `tombstoned_count`, and `fan_out_count`.
- `Previewed keeper-sync scope`, from the preview endpoint, carrying
  the same `ltd_count` and `in_scope_count` plus `new_count`,
  `tombstoned_count`, `unmatched_project_slugs_count`,
  `unmatched_exclude_project_slugs_count`, and a `candidate` flag
  distinguishing a previewed candidate body from the stored config.
  The two unmatched counts are named for the response fields they
  count, and are logged separately for the same reason the response
  carries two lists: a line saying only how many entries were
  unmatched does not say which field to go fix.
- `Updated keeper_sync_config`, on every `PUT` and `PATCH`, recording
  the size of each of the four scope fields as `project_slugs_count`
  (`null` for the `"*"` wildcard, which has no size),
  `project_slug_patterns_count`, `exclude_project_slugs_count` and
  `exclude_project_slug_patterns_count`.

### The counts always add up

The three counts a scope resolution and a preview share mean the same
thing on both sides, and they compose into one identity you can check
by eye:

```
`in_scope_count` - `tombstoned_count` = `fan_out_count`
```

- `in_scope_count` is the **config** resolution — every slug the four
  scope fields admit, before tombstones are considered. It is the
  preview's `in_scope_count` and the length of its `in_scope_slugs`.
- `tombstoned_count` counts only the tombstones **inside that scope**.
  It is the length of the preview's `tombstoned_slugs`, *not* how many
  tombstoned projects the org has: a tombstone on a slug the config
  never admitted is not this scope's shortfall to explain.
- `fan_out_count` is what is left — the `keeper_sync_project` children
  the pass actually enqueues, and on a run, its `total_count` less the
  one discovery job (less, too, any slug skipped because a per-project
  job for it was already running).

So a preview reading `in_scope_count=3` with one entry in
`tombstoned_slugs` predicts a run logging `in_scope_count=3`,
`tombstoned_count=1`, `fan_out_count=2` — and a `total_count` of 3.
If the two disagree, the LTD listing or the tombstones changed between
the preview and the run; the counts themselves do not drift.

An `excluded_count` that jumps unexpectedly is the signal that an
exclude pattern is broader than intended — it is the only counter that
distinguishes "the include rules never matched these" from "an exclude
rule took them back out".

The discovery job's own `queue_jobs` row carries the same two names on
its `progress` when it completes: `in_scope_count` and `fan_out_count`,
alongside `enqueued_count` — which is `fan_out_count` less the slugs
whose per-project job was already running.

## Related

- `client/src/docverse/models/keeper_sync.py` — `KeeperSyncConfig`,
  where the scope rule and its validation are defined once,
  `KeeperSyncScopePreviewRequest` (the preview body, which is a
  `KeeperSyncConfigUpdate` minus `ltd_base_url`), and
  `KeeperSyncScopePreview`.
- `src/docverse_server/services/keeper_sync_scope_preview.py` — the
  side-effect-free preview service.
- `src/docverse_server/worker/functions/keeper_sync.py` — run discovery
  and the tier crons, which resolve the scope through the model.
- `tests/docs_test.py` — fails when this page stops naming every scope
  field, preview field, cap, or endpoint the code has.
- SQR-112, and PRD #667.
