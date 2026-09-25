# Scoping the keeper sync

An organization's keeper-sync **scope** is the set of LTD Keeper product
slugs Docverse will import and keep in step. It is four fields on the
org's keeper-sync config, and it is what lets a migration the size of
lsst.io — roughly 1640 LTD products — go over in waves by document
series instead of all at once.

This page is for operators: the rule the four fields compose into, what
a pattern does and does not match, what happens to a project that falls
out of scope, the preview → `PATCH` → backfill workflow a wave
actually uses, and why the editions and builds a sync imports carry
LTD's timestamps rather than the moment of import
([Timestamps mirror LTD](#timestamps-mirror-ltd)).

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

### Finding the projects that fell out

The collection listing
`GET /orgs/{org}/keeper-sync/projects` is deliberately **not**
scope-filtered: it lists every project-resource state row on the org,
so a project that has fallen out of scope still appears there. That
asymmetry is the feature — the listing is how you find a project whose
detail endpoint has started answering 404.

Each entry carries an `in_scope` boolean saying which side of the scope
it is on, so an excluded project is visibly excluded rather than
indistinguishable from the ones still syncing:

```json
{"ltd_slug": "sqr-112", "in_scope": false, "project_url": "..."}
```

The per-project `GET /orgs/{org}/keeper-sync/projects/{ltd_slug}`
always reports `in_scope: true`, because it 404s every slug that is
not.

Filter the listing on the same flag to ask the question directly:

```
GET /orgs/{org}/keeper-sync/projects?in_scope=false
```

That is the **stale-exclude report** — every project this org once
synced and no longer does. Run it after each wave's `PATCH`: anything
listed is either an exclude you meant (fine) or a pattern that stopped
matching something you did not mean to drop. `?in_scope=true` narrows
the listing the other way, and omitting the parameter returns
everything, as before.

One thing to know about the filter: scope is a regular-expression rule,
not a SQL predicate, so it is applied to each page **after** that
page's rows have been read. `limit` therefore bounds the *rows read*,
not the entries returned — a page can come back shorter than `limit`,
or empty, while its `Link` header still offers a `next` cursor, and
`X-Total-Count` stays the unfiltered row count. **Follow `next` until
it is gone** rather than stopping at the first short page.

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

Neither case says what the excludes that *did* match are holding back.
For that, read the scope from the Docverse side with
`GET /orgs/{org}/keeper-sync/projects?in_scope=false`, which lists the
projects this org has state rows for and has stopped syncing.

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

## Timestamps mirror LTD

A project migrated from LTD Keeper is usually years older than its
import, and its version dashboard (`/v/`) should say so. Keeper-sync
therefore owns the clock of every row it keeps in step with LTD: a
synced edition's dates, and its build's, are the ones LTD recorded,
not the moment Docverse copied them. Before PRD #706 they were the
import's, which is why every edition on a freshly migrated dashboard
read "updated just now".

### Which columns follow LTD

| Docverse column | Set from LTD's |
| --- | --- |
| `editions.date_created` | edition `date_created` |
| `editions.date_updated` | edition `date_rebuilt`, or `date_created` for an edition LTD never rebuilt |
| `builds.date_created` | build `date_created` |
| `builds.date_completed` | build `date_created` |

LTD's `date_rebuilt` is the last time LTD pointed the edition at a new
build — the LTD counterpart of the repoint that moves a native
edition's `date_updated`. The edition mapping is `derive_edition_dates` in
`src/docverse_server/services/keeper_sync/mappers.py`: the stamp and
the proactive lifecycle pass (see
[Lifecycle rules read the same clock](#lifecycle-rules-read-the-same-clock))
both read it, so they cannot disagree.

Three refinements decide *which* rows are stamped:

- **The build is the edition's current one.** The stamp writes the
  Docverse build the edition's LTD build maps to, and only ever moves
  that build's clock *earlier*. `sync_build` converges LTD builds with
  identical bytes onto one Docverse build — a `main` and a tag built
  from the same commit, say — so a shared build carries the earliest
  of those LTD builds' dates, and a native build that LTD's copy of the
  same content converged onto keeps its own upload time when that is
  earlier. Only a *current* build is stamped: a build that had already
  left rotation before the stamp shipped keeps its import-time dates.
- **Semver aggregates follow their release.** The `15` / `15.2`
  editions keeper-sync maintains for a release have no LTD row of their
  own. Each one currently serving the release's build takes the
  release's `editions.date_updated` and keeps its own Docverse
  `editions.date_created`. As with builds, the stamp only moves an
  aggregate's `date_updated` earlier, so when two releases in one
  series share a build the earlier release's date wins. An aggregate
  serving any other build is left alone, and so is an edition on an
  aggregate's slug that does not track the way the aggregate does (an
  operator's own `15`, say).
- **Only visited, live rows.** An edition whose `keeper_sync_state`
  row is tombstoned short-circuits before the stamp, an edition
  soft-deleted mid-visit is skipped (build and aggregates included), and
  a native edition keeper-sync never visits is never stamped.

### Every visit re-asserts the clock

The stamp is the **last** transaction of every keeper-sync visit to an
edition: a fresh import, a visit whose build already matches LTD, and
a visit that converges onto an existing build alike. It has to come
last. Every ORM write to an edition row moves `date_updated` back to
now (the column's `onupdate`), and a visit makes several of them — the
kind convergence, `sync_build`'s repoint, the aggregate backfill. The
stamp names both columns in its `UPDATE`, which is what keeps
`onupdate` out of it, and guards them with `IS DISTINCT FROM`, so a
visit whose rows already carry LTD's values writes nothing at all.

The consequence is that **a Docverse-side write to a synced row drifts
its clock for at most one visit.** A tracking refresh, a kind
convergence, an operator's `PATCH` of the edition's title or
`lifecycle_exempt`, and above all the `publish_status` flips of the
edition's own publish all move `date_updated` to now; the project's
next keeper-sync visit puts LTD's value back. A visit is any
`keeper_sync_project` job for the project:

- a run, `POST /orgs/{org}/keeper-sync/runs`;
- a per-project refresh,
  `POST /orgs/{org}/keeper-sync/projects/{ltd_slug}/refresh`;
- a tier-cron tick. `tier_other` revisits a project with any
  non-`main` edition hourly while the project is hot (its LTD `main`
  rebuilt within 14 days) and every day or two once it is dormant.
  `tier_main` revisits a project only when LTD rebuilds its `main`, so
  a project whose only LTD edition is `main` stays drifted until then,
  or until the next run.

A **freshly imported** edition therefore restamps on two visits, not
one. The import visit stamps it; the publish that visit enqueues flips
`publish_status`, which moves `date_updated` to now; the next visit
puts LTD's value back. It settles there — a third visit writes nothing.
The same holds for a release's aggregates, whose publish flips theirs.

A stamp that fails is logged, with a Sentry event, as
`Edition clock stamp failed; edition sync still succeeded` and does not
fail the edition: it is already imported, and the next visit
re-asserts the dates anyway.

### The dashboard refreshes itself

The version dashboard is rendered when something publishes, so a visit
that only moves dates would leave `/v/` showing the dates it was last
rendered with. Each visit's outcome therefore carries `dates_restamped`
— true when the stamp changed the edition's row, its build's, or one
of its aggregates'. When an outcome is `dates_restamped` and the visit
enqueued no publish (neither the edition's own nor an aggregate's; each
of those cascades its own `dashboard_build`), the `keeper_sync_project`
job enqueues one `dashboard_build` for the project after its edition
loop finishes, logging
`Enqueueing dashboard_build for restamped edition dates`. That is at most one render per project per job,
however many of its editions restamped, and it is enqueued only once
every edition's clock is committed, so the render sees them all. A
failure to enqueue is logged and never fails the job.

One gap is accepted: a job whose sync fails as a whole — a systemic
failure rather than isolated per-edition ones — never reaches that
enqueue. Editions it restamped before failing keep their stale render
until the project's next publish, or the next job that restamps another
of its editions.

### Lifecycle rules read the same clock

`draft_inactivity` matches a `draft` edition whose `date_updated` is
older than the rule's `max_days_inactive`, and for a synced edition
`date_updated` is now LTD's last rebuild rather than the day Docverse
imported it. PRD #706 accepts this deliberately: a draft LTD last
rebuilt a year ago *is* inactive, whenever it was copied. The proactive
lifecycle pass, which tombstones LTD editions a rule would delete
before they are ever imported, already judges them with
`derive_edition_dates`, so a stale LTD draft is not imported in the
first place; the rule now sees the same clock before and after import.

What changes is the drafts imported **before** the stamp existed. Their
`date_updated` recorded the import, so `draft_inactivity` saw them as
fresh. On an organization or project whose lifecycle rules include
`draft_inactivity`, once a visit restamps a draft whose LTD
`date_rebuilt` is older than `max_days_inactive`, the next hourly
`lifecycle_eval` tick matches it and handles it like any other
lifecycle deletion: the edition is soft-deleted, unpublished from the
CDN, and tombstoned `lifecycle_delete`, so keeper-sync does not import
it again. There is no grace period. To keep a particular draft, set
`lifecycle_exempt` on it **before** a visit restamps it: tier-cron
visits start restamping as soon as a release carrying the stamp is
deployed, and the backfill below reaches every project they have not.

`build_history_orphan` reads the build clock the same way: its
`min_age_days` counts from `builds.date_completed`, so a synced build
that later falls out of rotation is aged from LTD's build date rather
than from the copy.

### What the stamp leaves alone

- **`projects.date_updated`.** Restamping never moves the project's
  clock. That clock tells a consumer polling the project listing with
  `updated_since` (Ook) that the content behind a project moved — see
  [Soft-deleted resources and polling the project listing](api-conventions.md#soft-deleted-resources-and-polling-the-project-listing)
  — and re-dating an edition's history moves no content. A full-org
  backfill therefore does not make every project look changed to a
  poller, and the listing's `ETag` does not change. Nor does the
  project clock take LTD's history: it keeps recording Docverse-side
  events, the import among them. The one validator a restamp does
  retire is the single-project `GET`'s `ETag`, and only once: it
  hashes the embedded default `__main` edition's clock as its own part,
  and a restamp moves that clock (usually backwards). See
  [Conditional GET](api-conventions.md#conditional-get-the-etag-validator).
- **`organizations.date_updated`**, for the same reason.
- **`editions.date_deleted`.** LTD's `date_ended` is not carried over;
  tombstoning already handles an edition LTD ended.
- **`edition_build_history` rows**, which keep recording when Docverse
  repointed the edition.

### Backfilling projects synced before the stamp

There is no backfill tool, job, or migration. Every visit re-asserts
LTD's values, so the backfill is simply a full org run:

1. **Decide about stale drafts first** — ideally before the release
   carrying the stamp is deployed, since tier-cron visits start
   restamping on their own. Set `lifecycle_exempt` on any synced draft
   that should survive; see
   [Lifecycle rules read the same clock](#lifecycle-rules-read-the-same-clock).
2. **Launch a run** — `POST /orgs/{org}/keeper-sync/runs`. It fans out
   one `keeper_sync_project` job per in-scope project, and each job
   visits every one of that project's LTD editions. For a single
   project, `POST /orgs/{org}/keeper-sync/projects/{ltd_slug}/refresh`
   does the same.
3. **Let the dashboards land.** Each project with a restamp-only visit
   enqueues its own `dashboard_build` when its job finishes; there is
   nothing to trigger by hand.
4. **Confirm it ran.** Each project's terminal log line —
   `Keeper-sync project completed`, or
   `Keeper-sync project completed with edition failures` for a partial
   sync — carries
   `restamped_edition_count`: how many of its editions' visits reported
   `dates_restamped`. The line is bound to `org`, `run_id` and
   `ltd_slug`, so filtering on the run gives the whole backfill. Expect
   it above zero for every project synced before the stamp was deployed
   that no tier-cron visit has restamped since. On `/v/`, an edition's
   date now reads LTD's `date_rebuilt` (or `date_created`).

A **repeat run** is the check that the backfill converged: it reports
`restamped_edition_count` of `0` for every project whose content did
not move in between. A project the first run imported or republished
reports its second-visit restamp once more (see
[Every visit re-asserts the clock](#every-visit-re-asserts-the-clock))
and `0` after that. The same applies to a wave: the run that imports it
leaves each new edition one visit behind, so a second run straight
after — or the tier crons over the following hours — is what settles
the new dashboards.

To see exactly what moved, the stamp logs one info line per changed
row:

| Message | Fields |
| --- | --- |
| `Restamped edition dates from LTD` | `edition_id`, `edition_slug`, `project_id`, `ltd_edition_id`, `previous_date_updated`, `date_updated` |
| `Restamped build dates from LTD` | `build_id`, `edition_id`, `project_id`, `ltd_edition_id`, `previous_date_created`, `previous_date_completed`, `date_created` |
| `Restamped semver aggregate dates from LTD` | `edition_id`, `edition_slug`, `release_edition_id`, `project_id`, `ltd_edition_id`, `build_id`, `previous_date_updated`, `date_updated` |

## Related

- [Keeper-sync transport resilience](keeper-sync-transport.md) — how
  a wave's build copies ride out an outage on either end, R2 or the
  LTD bucket (with a per-object budget for R2 uploads only), what
  happens to an edition whose copy still fails, and how to read the
  `build_content_copied` event each copy publishes.
- `client/src/docverse/models/keeper_sync.py` — `KeeperSyncConfig`,
  where the scope rule and its validation are defined once,
  `KeeperSyncScopePreviewRequest` (the preview body, which is a
  `KeeperSyncConfigUpdate` minus `ltd_base_url`), and
  `KeeperSyncScopePreview`.
- `src/docverse_server/services/keeper_sync_scope_preview.py` — the
  side-effect-free preview service.
- `src/docverse_server/worker/functions/keeper_sync.py` — run discovery
  and the tier crons, which resolve the scope through the model, and
  the per-project job that logs `restamped_edition_count` and enqueues
  the restamp-only `dashboard_build`.
- `src/docverse_server/services/keeper_sync/service.py` —
  `KeeperSyncService._stamp_ltd_clock`, the end-of-visit clock
  transaction, and `derive_edition_dates` in the sibling `mappers.py`.
- `client/src/docverse/models/lifecycle.py` — `DraftInactivityRule`,
  whose description points back at
  [Timestamps mirror LTD](#timestamps-mirror-ltd).
- `tests/docs_test.py` — fails when this page stops naming every scope
  field, preview field, cap, or endpoint the code has, or a column the
  clock stamp writes.
- SQR-112, PRD #667 (scope), and PRD #706 (timestamps).
