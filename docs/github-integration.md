# GitHub integration

Docverse is a GitHub App. Installed on a repository, it lets Docverse
hear about that repository as it changes — pushes, deleted refs,
renames, transfers, and default-branch changes arrive as webhook
deliveries — and read it through GitHub's REST API with an installation
token. A project is *bound* to a repository when it has a `github`
binding (`github.owner` and `github.repo` on the project resource). A
project whose source is a non-GitHub `source_url` takes no part in
anything on this page.

This page is for operators: what each webhook delivery does, how a
project's `__main` edition follows its repository's default branch
across a rename, the knobs that shape both, how to read what happened,
and what to do when Docverse deliberately leaves `__main` alone.

## Webhook events

GitHub delivers to `POST {path_prefix}/webhooks/github` —
`/docverse/webhooks/github` under Phalanx, on an anonymous ingress that
exists only when `config.githubAppId` is set. The handler verifies the
`X-Hub-Signature-256` HMAC against `github_webhook_secret` and then
dispatches on the event type and its `action`. It answers:

- `404` when the GitHub App is not configured, and `401` when the
  signature is missing or does not verify;
- `200` for every signed delivery, including event types nothing
  handles — such as the `ping` GitHub sends when the webhook is set up —
  so GitHub never retries a delivery Docverse chose to ignore;
- `500` when a handler raises. Each handler does its database work in
  one transaction, so a delivery that fails partway through that work
  leaves nothing half-done — except `repository.edited`, which works
  project by project (see
  [A default-branch delivery that fails](#a-default-branch-delivery-that-fails)).
  A `push` commits its keeper-sync step separately, after the
  dashboard-template work, and absorbs that step's failure rather than
  answering `500` (see
  [When the stamp fails](#when-the-stamp-fails)).

Every delivery, whatever became of it, publishes one
`github_webhook_received` event; see the
[metrics catalog](metrics.md#github_webhook_received).

| Event | What Docverse does |
| --- | --- |
| `push` | Enqueues one `dashboard_sync` for each dashboard-template binding pinned to the pushed repository and ref whose `root_path` the push touched. The changed paths come from the payload, or from GitHub's compare API when the payload's commit list is truncated. Then, in a transaction of its own, stamps the pushed branch or tag onto every LTD-synced project bound to the repository, putting it on keeper-sync's fast path: see [The keeper-sync push hot path](#the-keeper-sync-push-hot-path). That step never calls GitHub, and its failure does not fail the delivery. A push never creates a build or moves an edition: builds arrive through the upload API, or from LTD Keeper through keeper-sync. |
| `delete` | For a deleted branch or tag, soft-deletes and unpublishes every live, non-exempt `draft` edition tracking it, on every project bound to the repository, then enqueues one `dashboard_build` per affected project. Release editions and `__main` are never swept. |
| `repository.renamed` | Rewrites the repository name on bound projects and on dashboard-template bindings and templates, matched by `repository.id` — and, for a template binding that has never synced, by its old name. |
| `repository.transferred` | Rewrites the owner, owner id, and name on the same rows, matched by `repository.id` only. |
| `repository.edited` | When `changes` carries `default_branch`, applies the new default branch to every project bound to the repository, each in a transaction of its own: see [The default branch](#the-default-branch). Any other edit — description, homepage, topics — is logged and ignored. |
| `organization.renamed` | Rewrites the owner login on dashboard-template bindings and templates. Project rows are not rewritten: a project's `github.owner` keeps the old login. |
| `installation.created` | Records the installation, owner, and repository ids on every project bound to a repository the installation covers. |
| `installation.deleted` | Marks every dashboard-template binding under the installation failed, with `installation_deleted`. |
| `installation.suspend` | The same, with `installation_suspended`. |
| `installation.unsuspend` | Clears only the `installation_suspended` failures, so a binding that failed for another reason stays failed. |
| `installation_repositories.added` | Records the ids, as `installation.created` does, for the repositories added. |
| `installation_repositories.removed` | Logged and ignored. The repository still exists, so the ids already recorded are kept. |

The `delete` and `repository.edited` deliveries find their projects by
`repository.id`, falling back to the owner and name only for a project
whose numeric id has not been resolved yet, so a different repository
that later takes a renamed repository's old name never matches.

In the App's settings, *Subscribe to events* must have **Push**,
**Delete**, **Repository**, and **Organization** checked; GitHub offers
each checkbox only once the App holds a permission that covers it.
`installation` and `installation_repositories` deliveries reach every
GitHub App without a subscription. A callback for a new event type does
nothing until its box is checked too.

GitHub does not redeliver a failed delivery by itself. Once the cause
is fixed, redeliver it from the App's *Advanced* settings tab (*Recent
Deliveries*). A default-branch change whose delivery was lost is
converged by the next `git_ref_audit` tick anyway (see
[The audit is the backfill](#the-audit-is-the-backfill)).

### A default-branch delivery that fails

A `repository.edited` delivery reads the projects bound to the
repository first, then converges each one in its own transaction,
committing it before the next project's turn. That is the audit's
shape, and for the same reason: the rule holds each project's `__main`
lock while it writes, and one delivery-wide transaction would wait on a
later project's lock while still holding every earlier project's
uncommitted rows.

So a failure does not roll the whole delivery back. A project whose
convergence raises — a CDN failure unpublishing a retired draft, say —
loses only its own writes, and the projects after it still converge.
Once every project has had its turn, the delivery answers `500` with
the first failure, which is what Sentry records, and its
`github_webhook_received` event reads `error`, with `jobs_enqueued`
counting the jobs the converged projects enqueued. Redelivering it
retries the failed projects and is inert for the ones that converged:
their column already matches and their `__main` already tracks the
branch.

## The default branch

`__main` — the edition served at a project's root — tracks a literal
ref: in `git_ref` mode, `tracking_params.git_ref`, which a new project
gets as `main` unless its default-edition config says otherwise. Builds
match editions by exact string. So when a repository's default branch is
renamed (`master` → `main`), builds start arriving on the new name,
`__main` never matches them, and edition tracking auto-creates a `main`
draft for them instead. Nothing fails; the project root just stops
updating.

Docverse therefore records each bound project's default branch in
`projects.github_default_branch`, shown read-only as
`github.default_branch` on the project resource. It cannot be set
through the API — GitHub is the source of truth — and it reads `null`
until Docverse has learned it, which every consumer treats as `main`.
Recording a new value moves the project's `date_updated`; an unchanged
one does not. A `PATCH` that unbinds the project (`github: null`, or a
non-GitHub `source_url`) or binds it to another repository clears the
column along with the numeric ids, so it never describes a repository
the project is no longer bound to.

### Three triggers, one rule

Three things tell Docverse a repository's default branch, and each
brings its own evidence about whether the ref `__main` tracks still
exists:

| Trigger | When it runs | Its evidence that `__main`'s ref is gone |
| --- | --- | --- |
| `webhook` | A `repository.edited` delivery whose `changes` carry `default_branch` | `changes.default_branch.from`: the branch that just stopped being the default |
| `resolve` | `project_github_resolve`, after a project is created with a `github` binding or a `PATCH` names one | The old repository's default branch, for a project rebound to another repository: the `PATCH` clears the column, so the job's payload carries the value it held as `previous_default_branch`. Without one, the column's current value, if any. While the column is `null` — a first learn — also the live branch and tag names it fetches once for the project (see [A new project](#a-new-project)) |
| `audit` | The daily `git_ref_audit`, at 05:17 UTC | The live branch and tag names it fetched for the project |

All three hand the branch to one rule, `DefaultBranchService.apply`,
which runs in one transaction under `__main`'s `EDITION_UPDATE` lock —
the lock edition tracking, keeper-sync, and `publish_edition` take for
the same edition, so none of them interleaves with it:

1. **Record the branch.** Writes the column, unless it already holds
   that value.
2. **Rewrite `__main` if its ref is gone.** Only when `__main` is in
   `git_ref` mode, tracks something other than the default branch, and
   the trigger's evidence says that ref is gone: it is the old default
   the trigger named, or it is missing from the live ref set the audit,
   or a first-learn resolve, fetched. Only `tracking_params.git_ref`
   changes.
3. **Retire the duplicate draft.** After a rewrite to branch X, every
   live, non-exempt `draft` edition in `git_ref` mode tracking X — the
   one edition tracking auto-created while `__main` ignored X — is
   soft-deleted and unpublished, exactly as a `delete` delivery retires
   a draft. Left alone, it and `__main` would both advance on the next
   push.
4. **Repoint `__main`.** After a rewrite, `__main` is pointed at X's
   newest completed build (newest by `date_created`), under the ordinary
   stale-build guard, so a build older than the one `__main` already
   serves is refused. A repoint records the edition's history row, marks
   its publish `pending`, and enqueues `publish_edition`, and the
   project's `date_updated` moves. If X has no completed build yet,
   `__main` keeps serving what it served until edition tracking advances
   it with X's next build.

Steps 3 and 4 run only after a rewrite, so a redelivered webhook or a
repeat audit tick — the column already matches and `__main` already
tracks the branch — is inert: no rewrite, no jobs, no event.

The webhook and the audit differ in one case, deliberately. Switching a
repository's default branch in its settings while the old branch still
exists sends `repository.edited` with the old branch as
`changes.default_branch.from`, and a `__main` tracking it is rewritten:
the switch itself is the signal. The audit has only the live ref set to
go on, so it never moves `__main` off a branch or tag that still exists.
A switch whose delivery was lost therefore stays with an operator; see
[Pinned `__main` editions](#pinned-__main-editions-are-left-for-operators).

### A new project

A project created with a `github` binding gets a `__main` tracking
`main`, the same fallback every reader of a `null` column uses, because
creation does not wait on GitHub. Its resolve job then learns the real
default branch, and since the column is still `null` it has no old
default to offer as evidence. So a first learn — a new project, or one whose rebind cleared the
column — also fetches the repository's live branches and tags, once,
with the installation token for the ids the job has just recorded (or
anonymously without an installation), and hands them to the rule as the
audit does:

- On a `master` repository with no `main` branch, `__main`'s `main` is
  gone: the resolve rewrites `__main` to `master`, retires the `master`
  draft any push made meanwhile, and repoints `__main` at the newest
  `master` build — within the one job, not at the next audit tick.
- On a repository that has a `main` branch but another default,
  `__main` stays on `main`, a ref that exists. Move it by hand if that
  is not what the project wants; see
  [Pinned `__main` editions](#pinned-__main-editions-are-left-for-operators).
- If the ref fetch fails, the job logs
  `Resolve: GitHub ref fetch failed, recording default branch without live refs`,
  records the branch without the live set, and completes: the ref set
  is extra evidence, not the job's purpose, so it neither retries nor
  fails. The next audit tick is the backstop.

A resolve on a project whose column is already set spends nothing on
the ref set; the column's previous value is its evidence. Keeper-sync
enqueues no resolve, so a keeper-synced project learns its branch from
the audit instead.

### What a rewrite announces

Whichever trigger rewrote `__main` announces it the way a `PATCH` of the
edition would:

- one `edition_lifecycle` event with `action` `update` and
  `edition_kind` `main`;
- one `dashboard_build` for the project, so the `/v/` dashboard drops
  the retired draft;
- when step 4 repointed, the `publish_edition` job, whose
  `edition_published` event reports `trigger` `build`.

On a webhook, the delivery's `github_webhook_received` event counts the
`publish_edition` and `dashboard_build` jobs in `jobs_enqueued`. The
rule has no metrics event of its own.

### What the rule never touches

- A `__main` tracking a branch or tag that still exists, apart from the
  webhook's old default above. A `__main` pinned to a branch or tag on
  purpose stays pinned.
- A `__main` in any tracking mode but `git_ref`. `lsst_doc` has no ref
  to rewrite; it reads the column directly (below).
- `__main`'s `kind`, `kind_source`, `title`, and `lifecycle_exempt`.
- Any edition other than `__main`, beyond step 3's duplicates. Drafts
  still tracking the old branch are retired as deleted refs, by the
  `delete` delivery and the audit's `ref_deleted` pass, once that
  branch is gone.
- A deployment-scoped `alternate_git_ref` draft, or a build with an
  `alternate_name`: neither ever matches alongside `__main`.
- A build still processing: step 4 repoints only at completed builds,
  and edition tracking advances `__main` when the build completes.
- A project with no GitHub binding: unbinding clears its column, it
  stays `null`, and every consumer keeps the `main` fallback.

### `lsst_doc` editions

An `lsst_doc` edition serves the newest `vX.Y` release and, until the
first one, the default branch. That branch is the column (or `main`
while it is `null`), not a literal `main`: a build on the default branch
publishes a fresh `lsst_doc` edition, a `vX.Y` tag upgrades it, a
default-branch build never displaces a published release, and
successive default-branch builds pass the same date guard `main` builds
always have. For a project whose default branch is `master`, a build on
`main` is an ordinary branch.

The branch fallback only applies while the edition serves that same
branch. An `lsst_doc` edition still serving a build of the *former*
default branch is therefore not displaced by builds of the new one
until a release tag lands; repoint it once by hand (see
[Pinned `__main` editions](#pinned-__main-editions-are-left-for-operators))
and the new branch's builds match from then on.

## The audit is the backfill

An environment upgraded to a release with this feature starts with the
column `null` on every project, and the webhook and the resolve fire
only on a change. The daily `git_ref_audit` is what fills it in. Its
per-project pass already fetches each bound repository's live branches
and tags for the `ref_deleted` rule; it also reads
`GET /repos/{owner}/{repo}` for `default_branch` and applies the rule
with the live set as its evidence. The first tick after an upgrade:

- fills the column on every bound project it can read, in every
  organization the audit covers. Keeper-synced projects are included,
  and for them the audit is the usual seed: keeper-sync creates projects
  without enqueueing a resolve;
- rewrites any `__main` tracking a ref that no longer exists — a
  default branch renamed before the upgrade, or a project created before
  it on a `master` repository whose `__main` got the `main` fallback and
  that has no `main` branch. A project created since converges in its
  own resolve (see [A new project](#a-new-project)); the audit is the
  backstop for one whose resolve could not list its refs;
- converges what the webhook cannot reach: repositories without the App
  installed, and deliveries that were lost or failed.

The audit runs only while `git_ref_audit_enabled` is on. The setting
ships off, and Phalanx turns it on per environment with
`config.maintenance.gitRefAuditEnabled`. With it off, columns fill only
as webhooks and resolves happen to arrive, and nothing heals a lost
delivery.

- **Auth.** A project with an installation id is read with that
  installation's token. One without is read anonymously, which works for
  a public repository — within GitHub's unauthenticated rate limit, 60
  requests an hour per source address — and reports a private one as
  not accessible. The audit needs the three GitHub App settings even for
  anonymous projects: without them an organization's pass fails
  outright.
- **Failures.** A project whose ref set cannot be fetched is skipped for
  the pass. One whose ref set was fetched but whose `GET /repos` failed
  keeps its `ref_deleted` reaping and waits a day for its default
  branch. So does one whose rule raises — a CDN failure unpublishing a
  retired draft, say, or a database error: its transaction rolls back
  whole, so none of the rule's writes for it land, and the next tick
  tries again. Either way the rest of the organization's pass carries
  on, and its `git_ref_audit` queue job ends `completed_with_errors`.
- **Order.** The rule runs in its own transaction per project, ahead of
  the pass's `ref_deleted` deletions. One organization-wide transaction
  would hold every earlier project's rows while waiting on a later
  project's `__main` lock.
- **Cost.** One more GitHub request per bound project per day.

There is no manual entrypoint for the audit. To converge one project
without waiting for the next tick, `PATCH /orgs/{org}/projects/{project}`
its `github` binding with the owner and repository it already has. That
re-runs `project_github_resolve`, which records the branch and, when the
column held a different branch that `__main` still tracks, rewrites it.
The `PATCH` clears the three numeric GitHub ids until the resolve reads
them back.

## Keeper-synced projects

A keeper-synced project's `__main` mirrors LTD Keeper's `main` edition,
and keeper-sync realigns its tracking with LTD on every visit. LTD never
hears about a default-branch rename, so its `main` edition goes on
naming the old branch. Mirrored verbatim, every sync visit would undo
what the webhook, the resolve, or the audit converged — and where the
old branch was kept alive, nothing would ever converge it again, since
the audit never moves `__main` off a branch that still exists.

Keeper-sync is the one trigger that defers to the others. When it maps
LTD's `main` edition in `git_refs` mode, and the column is known — not
`null` — and differs from LTD's ref, it reads `__main`'s tracking as
Docverse stores it and decides, in order:

1. **`__main` already tracks the default branch.** `__main` is in
   `git_ref` mode on exactly the column's branch — another trigger, or
   an earlier visit, put it there. It stays, whatever the live ref set
   says, and LTD's ref is not applied.
2. **LTD's ref is gone.** This visit fetched the repository's live ref
   set and LTD's ref is not in it — the rule's own "ref gone" test.
   `__main` moves onto the default branch.

Otherwise LTD's value stands. A ref that still exists is deliberate
non-default tracking, a `null` column has nothing to follow, and a ref
fetch that failed — or a project with no binding — is no evidence that
the ref is gone. A `__main` still tracking LTD's ref after a failed
fetch keeps it for that visit, and the next visit with a live set
converges it; a `__main` already on the default branch is not moved
back by a failed fetch.

When a visit moves `__main` onto the default branch itself (case 2), it
also retires the duplicate drafts, as step 3 of the rule does after a
rewrite: every live, non-exempt `draft` in `git_ref` mode tracking that
branch is soft-deleted and unpublished. A synced draft's
`keeper_sync_state` row is tombstoned with reason `lifecycle_delete`, so
later visits skip its LTD edition. Unlike the rule, a keeper-sync visit
does not repoint `__main` (which build it serves stays LTD's), publish
an `edition_lifecycle` event, or enqueue a `dashboard_build`; the `/v/`
dashboard drops the retired draft at the project's next publish.

A visit writes `__main`'s tracking only when it changes, and then under
`__main`'s `EDITION_UPDATE` lock — the lock the rule holds for its
rewrite — re-reading `__main` inside the lock, so a rewrite that landed
while the visit waited is kept rather than overwritten. A visit that
changes it logs `Realigned keeper-synced __main tracking` with
`previous_git_ref`, `git_ref`, `tracking_source`, and `drafts_retired`;
its `Retired draft duplicating __main` lines carry `trigger`
`keeper_sync`.

The live set is fetched at most once per visit and shared with the
proactive lifecycle pass. A project with neither a `ref_deleted` nor a
`draft_inactivity` rule pays for the fetch only when LTD's `main`
differs from a known default branch.

LTD's own view is kept: `annotations.ltd_tracked_refs` on the edition's
`keeper_sync_state` row still records what LTD said. Which way a visit
went is on the debug line
`Derived keeper-sync edition tracking and kind`, alongside
`ltd_tracked_refs` and the resulting `git_ref`. Its `tracking_source`
is one of:

| `tracking_source` | Meaning |
| --- | --- |
| `ltd` | LTD's tracking, mapped mode for mode |
| `converged` | `__main` already tracks the default branch; LTD's ref is not applied |
| `default_branch` | LTD's ref is gone; the visit moves `__main` onto the default branch |

This governs only the ref a synced `__main` tracks. Which build it
serves is still keeper-sync's business, and follows LTD's `main`
edition. While `__main` tracks the default branch, a change to the ref
LTD's `main` edition tracks is not applied either. To have a synced
`__main` follow some other ref, set that ref in LTD and `PATCH` `__main`
to the same ref (see
[Pinned `__main` editions](#pinned-__main-editions-are-left-for-operators));
from then on it agrees with LTD, and keeper-sync mirrors LTD as before.

## The keeper-sync push hot path

Keeper-sync finds LTD Keeper's changes by polling it on three tier crons
whose cadence follows how recently a project's `main` edition rebuilt,
so a contributor's new branch edition can take half an hour to an hour
to appear, and a dormant project's a day. A `push` cannot sync anything
by itself: the repository's own CI builds the docs and uploads them to
LTD afterwards, and LTD then creates or rebuilds the edition. What a
push can do is say that LTD is about to change. So a `push` delivery
stamps the pushed ref onto each LTD-synced project bound to the
repository, and keeper-sync's tier crons poll that project on their
fast path for a bounded window afterwards (PRD #803). The webhook writes
the stamp; `keeper_sync_tier_main` reads it on its five-minute tick,
checks each pushed ref's edition on LTD directly, and enqueues the
project's sync once LTD has rebuilt it: see
[The `tier_main` check](#the-tier_main-check). For the window, all
three tier crons also treat the project as hot: see
[A push counts as hot](#a-push-counts-as-hot).

### The stamp

The stamp lives in the `github_pushed_refs` annotation on the project's
`keeper_sync_state` row — the project-resource row, keyed by the
project's slug, which is also its LTD product slug. It maps each pushed
ref, normalized to the bare branch or tag name, to the ISO-8601 time
Docverse processed the delivery:

```json
{
  "github_pushed_refs": {
    "tickets/DM-56619": "2026-10-09T14:02:11.204518+00:00",
    "v1.2.0": "2026-10-09T13:41:57.880341+00:00"
  }
}
```

A push is stamped when all of these hold:

- its `ref` is a branch (`refs/heads/…`) or a tag (`refs/tags/…`), and
  `deleted` is not true. A deleted ref is the `delete` event's
  business;
- a project is bound to the repository, found by `repository.id`, or by
  owner and name for a project whose numeric id is not resolved yet, as
  for `delete` and `repository.edited`;
- the project's organization has keeper-sync enabled, and its slug is
  inside the organization's
  [sync scope](keeper-sync-scope.md#the-rule);
- the project has a live project state row: keeper-sync has imported it
  from LTD, and its row is not tombstoned.

A repository bound to several such projects stamps every one of them.
The rest are skipped with an info line naming the `reason`:

| `reason` | Why the project was not stamped |
| --- | --- |
| `sync_disabled` | Its organization has keeper-sync off, or no keeper-sync config |
| `out_of_scope` | Its slug is outside its organization's sync scope |
| `no_state` | It has no live project state row: never imported from LTD, or tombstoned |

Every stamp writes the whole map back:

- **A repeated push overwrites.** A second push to the same ref replaces
  its time, so the window restarts from the latest push.
- **Expired refs are pruned.** Refs whose window has passed are dropped
  on every stamp.
- **The map is capped.** It holds at most 20 refs; a stamp that would
  leave more drops the oldest pushes first, so a repository that pushes
  many tags at once cannot grow the row without bound.
- **Other keys survive.** The row is read `FOR UPDATE`, the map merged
  into its other annotations — the tier crons' own
  `date_main_last_polled` and the like — and the whole column written
  back, so two deliveries stamping the same project, such as a branch
  and a tag pushed together, cannot drop each other's ref.

The stamp needs only the payload: the keeper-sync step never calls
GitHub, not even the compare API the dashboard-template step falls back
to for a truncated push.

### The window

A ref is inside the window while less than
`keeper_sync_push_window_seconds` (default `3600`, one hour) has passed
since its latest push. The window has to cover the repository's CI run
and its upload to LTD; a ref whose window closes without LTD rebuilding
it drops back to its project's ordinary cadence, and nothing retries
it: `tier_main`'s next visit prunes it from the map with the outcome
`expired`. Raising the window keeps a slow CI on the fast path longer,
at the price of more LTD polling per push.

### A push counts as hot

The tier crons gate each project on how recently its LTD `main` rebuilt:
a project rebuilt within 14 days is hot, and polled on every tick of
each tier; a dormant one is polled about once a day per tier. A project
with a ref inside the window is hot too, on all three tiers, whenever
its `main` last rebuilt. So for the window after a contributor pushes to
a project dormant for months:

- `keeper_sync_tier_main` checks its `main` edition every five minutes.
  A push to the default branch is caught by that check, which records
  the rebuild on the project row and so keeps the project hot for the
  14 days after.
- `keeper_sync_tier_discovery` lists its LTD editions every 30 minutes,
  and enqueues its sync for any it has not seen.
- `keeper_sync_tier_other` lists them every hour, and enqueues its sync
  for any non-`main` edition last synced an hour or more ago.

When the window closes, the project drops back to its cohort and nothing
else has to change: the stamp left on the row counts for nothing once
its push is a window old, and each tier's own last-poll time, which its
visits kept fresh, holds the project to the dormant cadence from there.
A ref cleared by an enqueue (see below) stops counting at once, so a
project whose every pushed ref has had its sync enqueued is back in its
cohort on the next tick.

`GET /orgs/{org}/keeper-sync/projects/{ltd_slug}`, and the listing at
`GET /orgs/{org}/keeper-sync/projects`, report the same: while a ref is
inside the window, each `tier_status` entry of the project reads
`"cohort": "hot"`, with `date_next_due` the tier's next cron tick, and
`in_push_window` is true; see [Reading a push](#reading-a-push).

### The `tier_main` check

Every five-minute `keeper_sync_tier_main` tick visits the stamps of each
in-scope project, after its own check of the project's `main` edition.
While a ref is live the push has made the project hot, so that check
runs too; the visit itself runs whatever the check's dormancy gate
decided, so a ref whose window has passed is pruned even from a project
that is dormant again.

Refs whose window has passed come first. Each is `expired`, costs no LTD
call, and is pruned. Then each live ref, oldest push first:

1. **Find the edition the ref feeds.** The Docverse editions of the
   project that track the ref (`git_ref` tracking mode, any kind: the
   `__main` edition for a push to the default branch, a draft for a
   branch, a release for a tag) are mapped to their `keeper_sync_state`
   edition rows and the LTD editions those record. When several LTD
   editions feed the ref, the newest is checked.
2. **Ask LTD.** One `GET /editions/<id>`, skipped when the `main` check
   already fetched that edition this tick. The edition is `rebuilt` when
   its `date_rebuilt` is newer than the `date_rebuilt_seen` its state
   row recorded at the last sync (or, if the row recorded none, its
   `date_last_synced`), and `unchanged` otherwise: the push's CI has not
   uploaded yet.
3. **Fall back to the listing.** When no synced edition tracks the ref,
   or LTD answers `404` for the one that did, the project's edition
   listing (`GET /products/<slug>/editions/`, read once per project per
   tick) is checked the way `keeper_sync_tier_discovery` checks it. An
   edition LTD lists that keeper-sync has no state row for is a
   `new_edition`, most likely the one the push's CI just uploaded;
   none is `not_found`: LTD has not created the ref's edition yet.

| Outcome | What the visit found | The stamp |
| --- | --- | --- |
| `rebuilt` | LTD rebuilt the ref's edition since its last sync | Cleared once the sync is enqueued |
| `new_edition` | No synced edition tracks the ref, and LTD lists one keeper-sync has not seen | Cleared once the sync is enqueued |
| `unchanged` | The ref's edition is as keeper-sync last synced it | Kept |
| `not_found` | No synced edition tracks the ref, and LTD lists nothing new | Kept |
| `error` | LTD failed to answer | Kept |
| `expired` | The ref's window passed | Pruned |

A `rebuilt` or `new_edition` ref enqueues the project's whole
`keeper_sync_project`, with the tier label `main`, through the same
per-project slot as every tier: one job per project per tick, shared
with the `main` check and with the project's other refs, and none when
a sync for the project is already queued or running. The refs that
called for an enqueued job are cleared from the map; when the slot was
taken they keep their stamps, and the next tick checks them again
against what that job synced. An `unchanged` or `not_found` ref keeps
its stamp until LTD rebuilds it or its window passes.

An LTD failure, after the client's own retries, is logged as
`Tier-main: failed to check pushed ref` and sent to Sentry, as the
`main` check's failures are. The ref keeps its stamp, and the project's
remaining live refs wait for the next tick rather than spend more LTD
calls on an LTD that is failing. When the `main` check itself failed on
LTD, the project's refs wait for the next tick too.

The visit writes the map back only when it cleared or pruned a ref,
reading the row `FOR UPDATE` first: a ref pushed again while the tick
ran keeps its newer stamp, since the enqueued job may have missed what
that push uploads. The three tier crons' own writes to the row, which
record when they polled the project, read it `FOR UPDATE` for the same
reason, so none of them can write back a map that predates a stamp.

The check's cost is bounded: at most two LTD calls per stamped ref per
tick (its edition, then the listing), the listing shared by the
project's refs, and at most 20 refs per project. A dormant project the
push woke adds its `main` check, usually one more call per tick for the
window, which a push to `main` itself shares. Editions that follow a
ref by a version rule rather than by name (`lsst_doc` and the `eups`
modes) are not looked up: a tag push to such a project is caught by the
listing when LTD creates an edition for the tag, and otherwise on the
project's ordinary cadence.

### `tier_main` log lines

Every visited ref writes one line; a ref whose sync was enqueued writes
a second, with the time from its push to the enqueue.

| Message | Level | Fields |
| --- | --- | --- |
| `Tier-main: checked pushed ref` | info | `org`, `project`, `github_ref`, `outcome`, `pushed_at`, `enqueued` |
| `Tier-main: enqueued project sync for pushed ref` | info | `org`, `project`, `github_ref`, `outcome`, `push_lag_seconds` |
| `Tier-main: failed to check pushed ref` | error | `org`, `project`, `github_ref`, `exception` |
| `Tier-main: failed to publish pushed ref check` | error | `org`, `project`, `github_ref`, `exception` |

`project` is the project's slug, which is also its LTD product slug;
`pushed_at` is the push time the visit read from the stamp; `enqueued`
is whether the ref's sync was enqueued, and so its stamp cleared, this
tick.

### The metrics event

Each visited ref is also one `keeper_sync_push_check` event, carrying
what its `Tier-main: checked pushed ref` line does: the organization,
the project, the `github_ref`, the `outcome`, whether the visit
`enqueued` the project's sync, and, when it did, the `push_lag` from the
push to that enqueue. The event is published after the tick has settled
the project's stamps, so a dashboard reads the same outcomes as the log.
Publishing is best-effort: a failure is sent to Sentry and logged as
`Tier-main: failed to publish pushed ref check`, and the tick carries on
with the enqueue and the stamps already settled. The event's fields,
tags and an example query are in the [metrics
catalog](metrics.md#keeper_sync_push_check).

### Switching it off

`keeper_sync_push_hot_path_enabled` (default `true`) is the hot path's
switch. Off, a push logs
`Keeper-sync push hot path is disabled, not stamping` and stamps
nothing, `tier_main` neither checks nor prunes stamps, making exactly
the LTD calls it made before the hot path existed, and keeper-sync polls
every project on its ordinary cadence: a stamp makes no project hot,
on any tier or on the status endpoint. The dashboard-template work a
push drives is unaffected either way. Stamps already written stay on
their rows untouched; turned back on, `tier_main` prunes the ones whose
window has passed on its next visit.

### When the stamp fails

The keeper-sync step runs after the dashboard-template step has
committed and handed its jobs to the queue, in a transaction of its
own. A failure there — a database error, say — rolls back only the
stamps, is logged as `Keeper-sync push stamp failed` and sent to Sentry,
and the delivery still answers `200`: a lost stamp costs only that
push's fast path, while a `500` would invite GitHub to redeliver
dashboard work that already happened. The delivery's
`github_webhook_received` event is recorded `dispatched` with
`projects_stamped` null, which is how the failure shows in the metrics.
Redelivering it from the App's settings stamps the projects with the
redelivery's time.

### Reading a push

- **The events.** `github_webhook_received`'s `projects_stamped` counts
  the projects a push stamped, and `keeper_sync_push_check` records
  each of `tier_main`'s visits to a stamped ref, with the push lag of
  the visits that enqueued a sync: see the
  [metrics catalog](metrics.md#github_webhook_received) and
  [The metrics event](#the-metrics-event).
- **The status endpoint.** `GET /orgs/{org}/keeper-sync/projects/{ltd_slug}`
  lists the project's stamps in `pushed_refs`, newest push first, and
  says in `in_push_window` whether any of them holds the project on the
  fast path, when each `tier_status` entry reads `"cohort": "hot"`. Each
  entry gives the `git_ref`, its `date_pushed`, and `in_window`, false
  for a stamp whose window has passed but that `tier_main` has not
  pruned yet. A project no push has stamped, or with no state row,
  reads `"pushed_refs": []` and `"in_push_window": false`. With the hot
  path off, the stamps are still listed, as they are still on the row,
  but none is `in_window`. Each entry of the listing at
  `GET /orgs/{org}/keeper-sync/projects` carries the same two fields:

  ```json
  {
    "in_push_window": true,
    "pushed_refs": [
      {
        "git_ref": "tickets/DM-56619",
        "date_pushed": "2026-10-09T14:02:11.204518Z",
        "in_window": true
      }
    ]
  }
  ```

- **The row.** The same response shows the raw stamp in
  `project_state.annotations`.
- **The tick.** `tier_main`'s lines say what each visit found and when
  a push's sync was enqueued: see
  [`tier_main` log lines](#tier_main-log-lines).
- **The log.** The processor binds `github_owner`, `github_repo`,
  `github_repo_id`, and the payload's `github_ref_raw` onto every line
  it writes, and the normalized `github_ref` once it has one; its
  per-project lines add `org`, `project`, and `project_id`. Every line
  also carries the delivery's `github_event` and `github_delivery_id`.

### Log lines

| Message | Level | Fields |
| --- | --- | --- |
| `Keeper-sync push hot path is disabled, not stamping` | info | — |
| `Ignoring push to a ref keeper-sync does not track` | info | — |
| `Ignoring push that deleted its ref` | info | — |
| `Ignoring push without a repository owner and name` | info | — |
| `No projects match push for keeper-sync` | info | — |
| `Skipped project for keeper-sync push` | info | `reason` |
| `Stamped keeper-sync push hint` | info | `pushed_at`, `pushed_refs` |
| `Processed push for keeper-sync` | info | `projects_matched`, `projects_stamped` |
| `Keeper-sync push stamp failed` | warning | `error`, `error_type` |
| `Processed push webhook` | info | `enqueued`, `projects_stamped` |

`pushed_refs` counts the refs on the project's map after the stamp, and
`pushed_at` is the time stamped. The handler's own
`Processed push webhook` closes every push delivery: `enqueued` counts
its `dashboard_sync` jobs, and `projects_stamped` the projects stamped,
null when the keeper-sync step failed.

## Configuration

| Setting | Environment variable | Default | Phalanx value |
| --- | --- | --- | --- |
| `github_app_id` | `DOCVERSE_GITHUB_APP_ID` | unset | `config.githubAppId` |
| `github_app_private_key` | `DOCVERSE_GITHUB_APP_PRIVATE_KEY` | unset | the `DOCVERSE_GITHUB_APP_PRIVATE_KEY` key of the application's Vault secret |
| `github_webhook_secret` | `DOCVERSE_GITHUB_WEBHOOK_SECRET` | unset | the `DOCVERSE_GITHUB_WEBHOOK_SECRET` key of the application's Vault secret |
| `git_ref_audit_enabled` | `DOCVERSE_GIT_REF_AUDIT_ENABLED` | `false` | `config.maintenance.gitRefAuditEnabled` |
| `keeper_sync_push_hot_path_enabled` | `DOCVERSE_KEEPER_SYNC_PUSH_HOT_PATH_ENABLED` | `true` | `config.keeperSync.pushHotPathEnabled` |
| `keeper_sync_push_window_seconds` | `DOCVERSE_KEEPER_SYNC_PUSH_WINDOW_SECONDS` | `3600` | `config.keeperSync.pushWindowSeconds` |

Notes:

- The three App settings are all or nothing. With any of them unset the
  webhook endpoint answers `404`, no installation token can be minted,
  and the audit's organization passes fail. The webhook secret must be
  the one entered in the App's settings, or every delivery is a `401`.
- `git_ref_audit_enabled` ships false, because the audit spends GitHub
  API budget every day; roundtable-dev and roundtable-prod turn it on.
  Its cron stays registered either way, so flipping the flag needs no
  worker restart.
- The default-branch rule has no switch of its own: it runs wherever
  its triggers do.
- The two `keeper_sync_push_` settings shape
  [the keeper-sync push hot path](#the-keeper-sync-push-hot-path). Their
  defaults are the intended production values, so their Phalanx values
  are optional: set them only to move one environment off the defaults,
  or to switch the hot path off there. The window must be at least one
  second; a smaller value fails configuration at startup.

## Reading an outcome

### The API

`GET /orgs/{org}/projects/{project}` shows the recorded branch as
`github.default_branch`, and
`GET /orgs/{org}/projects/{project}/editions/__main` shows the ref
`__main` tracks, in `tracking_params`, and the build it serves.

### Log fields

The rule binds these fields onto every line it writes, and the
`repository.edited` processor binds the repository's onto every line it
writes. A worker's own context rides along as well — `org` and
`git_ref_audit_run_id` on an audit's lines, for example — and the
webhook handler's own two lines, which the processor does not write,
carry the delivery's `github_event` and `github_delivery_id` instead.

| Field | Bound by | Meaning |
| --- | --- | --- |
| `trigger` | the rule | `webhook`, `resolve`, or `audit`; `keeper_sync` on the draft retirements a keeper-sync visit makes (see [Keeper-synced projects](#keeper-synced-projects)) |
| `project_id` | the rule | The project's internal id |
| `project_slug` | the rule | The project's slug |
| `old_default_branch` | the rule | The old default the trigger named: the webhook's `changes.default_branch.from`, or the column's previous value on a resolve. Null on the audit, whose evidence is the live ref set instead. |
| `new_default_branch` | the rule | The default branch being applied |
| `github_owner` | the processor | The repository's owner login |
| `github_repo` | the processor | The repository's name |
| `github_repo_id` | the processor | The repository's numeric id, `repository.id` |

### Log lines

Start from `Applied repository default branch`: the rule writes it once
per project each time a trigger applies it, whatever happened, so it is
the line to count. The rule's other lines say why it did what it did.

| Message | Level | Fields |
| --- | --- | --- |
| `Matched projects for repository.edited default branch change` | info | `old_default_branch`, `new_default_branch`, `projects_matched` |
| `Default branch convergence failed for a project` | warning | `org`, `project`, `project_id`, `error`, `error_type` |
| `Processed repository.edited default branch change` | info | `projects_converged`, `projects_failed`, `main_rewrites` |
| `Ignoring repository.edited without a default branch change` | info | `changed_fields` |
| `No projects match repository.edited` | info | `old_default_branch`, `new_default_branch` |
| `repository.edited payload missing default branch or repo` | warning | `new_default_branch` |
| `Recorded repository default branch` | info | — |
| `Left __main tracking a ref that still exists` | info | `edition_id`, `main_git_ref` |
| `Rewrote __main to track the default branch` | info | `edition_id`, `main_rewritten_from` |
| `Retired draft duplicating __main` | info | `edition_id`, `edition_slug`, `github_ref` |
| `Repointed __main at the default branch` | info | `edition_id`, `github_ref`, `repointed_build_id`, `previous_build_id` |
| `No completed build to repoint __main at` | info | `edition_id`, `github_ref` |
| `Kept __main on a build at least as new` | info | `edition_id`, `github_ref`, `build_id`, `current_build_id` |
| `Applied repository default branch` | info | `column_changed`, `main_rewritten`, `main_rewritten_from`, `drafts_retired`, `repointed_build_id` |
| `Resolved project GitHub metadata` | info | `github_installation_id`, `github_owner_id`, `github_repo_id`, `github_default_branch`, `default_branch_changed`, `main_rewritten` |
| `Recorded GitHub ids but skipped default branch: project rebound or deleted after the ids were committed` | info | `github_installation_id`, `github_owner_id`, `github_repo_id`, `github_default_branch` |
| `Resolve: GitHub ref fetch failed, recording default branch without live refs` | warning | `error`, `error_type` |
| `Git ref audit: GitHub repository metadata fetch failed, skipping default branch for this pass` | warning | `owner`, `repo`, `installation_id`, `error`, `error_type` |
| `Git ref audit: default branch convergence failed, skipping project for this pass` | warning | `error`, `error_type` |
| `Git ref audit completed for org` | info | `had_failures`, `default_branch_updates`, `main_rewrites`, `default_branch_errors` |

`drafts_retired` counts the drafts step 3 retired; `main_rewritten_from`
is the ref `__main` tracked before, or null when it was left alone. The
audit's two warnings name the project they skipped with `project` and
`project_id`, and the resolve's lines carry the job's `project_id`,
`github_owner`, and `github_repo`. On the audit's summary line,
`default_branch_updates` counts the projects whose column took a new
value, `main_rewrites` the projects whose `__main` was rewritten, and
`default_branch_errors` the projects whose convergence failed, across
the organization's pass.
Usually that failure is the rule raising and rolling back; when the
rule committed and only the hand-off of its `publish_edition` job to
the queue failed, the project counts toward the other two as well, and
the job's row is left for the reapers' orphan sweep, as after any failed
hand-off.

The resolve commits the numeric ids and then applies the branch in a
second transaction, re-reading the binding in between. When a `PATCH`
has moved the project to another repository (or deleted it) in that
window, the ids are already committed but the old repository's branch
is not recorded against the new binding: the job logs
`Recorded GitHub ids but skipped default branch` with the ids it wrote
and returns `metadata_only`, rather than the `skipped` of a resolve
that wrote nothing, and the rebind's own resolve records the new
repository's branch. A `repository.renamed` or
`repository.transferred` delivery in the same window is not a rebind:
it keeps the repository id the resolve just committed, so the branch
is recorded and the job returns `completed`.

A `repository.edited` delivery that matched any project ends with
`Processed repository.edited default branch change`, which counts the
same way per delivery: `projects_converged` the projects whose
transaction committed, `main_rewrites` those among them whose `__main`
was rewritten, and `projects_failed` the projects that raised, each of
which also logged `Default branch convergence failed for a project`. A
project whose rule committed but whose post-commit hand-off failed
counts as both converged and failed.

## Pinned `__main` editions are left for operators

The rule leaves `__main` alone whenever its ref still exists, because a
`__main` pinned on purpose has to stay pinned — and it cannot tell a
deliberate pin from a leftover. That leaves a few cases for an operator:

- The default branch was switched while the old branch still exists,
  and the `repository.edited` delivery was lost: the audit sees a live
  ref.
- `__main` got the `main` fallback on a repository that has a `main`
  branch but a different default.
- `__main` is in a tracking mode the rule does not rewrite, or is an
  `lsst_doc` edition still serving a build of the former default
  branch.

To move `__main` by hand:

1. Point its tracking at the branch. This needs the `admin` role:

   ```
   PATCH /orgs/{org}/projects/{project}/editions/__main
   {"tracking_mode": "git_ref", "tracking_params": {"git_ref": "main"}}
   ```

   Only `tracking_mode` and `tracking_params` change; `__main` advances
   with the branch's next completed build. (For an `lsst_doc` edition,
   leave its tracking alone and do step 2 alone.)
2. To serve the branch now, add `"build": "<base32 build id>"` to the
   same body, taking the id from
   `GET /orgs/{org}/projects/{project}/builds`. `build` is the edition
   override: it bypasses the stale-build guard and the edition's
   history, and enqueues the publish.
3. Retire the duplicate draft the rule would have retired:

   ```
   DELETE /orgs/{org}/projects/{project}/editions/{edition}
   ```

When the old branch is no longer wanted, deleting it in GitHub does all
of this: the `delete` delivery retires its drafts, and the next audit
tick finds `__main`'s ref gone and applies the whole rule — rewrite,
retire, repoint.

A keeper-synced `__main` keeps a hand-set tracking only when it agrees
with LTD or tracks the project's recorded default branch
(`github.default_branch`): keeper-sync realigns `__main` with LTD's
`main` edition on every visit, so a `PATCH` to any other ref is undone
at the next visit. A `PATCH` onto the default branch sticks, which makes
it the fix for a synced project whose old branch still exists; see
[Keeper-synced projects](#keeper-synced-projects). To pin a synced
`__main` to another ref, set that ref in LTD as well.

## What the integration deliberately does not do

- Rewrite a `__main` whose ref still exists, other than the webhook's
  old default: a pinned `__main` stays pinned.
- Rewrite any edition's tracking but `__main`'s. Drafts on the old
  branch are retired as deleted refs, never moved.
- Interpret a variable in `tracking_params`, such as a default-branch
  placeholder. A ref is a literal, and the one mode that needs the
  default branch, `lsst_doc`, reads the column directly.
- Watch `push` payloads for a changed `repository.default_branch`. A
  lost `repository.edited` waits for the audit, at most a day.
- Seed `__main` from GitHub when a project is created. Creation stays
  synchronous; the resolve and the audit converge the project
  afterwards.
- Offer an admin endpoint or CLI for the backfill: the audit is the
  backfill.
- Change anything in LTD Keeper.

## Related

- `src/docverse_server/handlers/webhooks/github.py` — the webhook
  endpoint and its event router.
- `src/docverse_server/services/default_branch.py` — the rule every
  trigger applies.
- `src/docverse_server/services/default_branch_announce.py` — the
  `edition_lifecycle` event and `dashboard_build` every trigger sends
  after a `__main` rewrite commits.
- `src/docverse_server/services/default_branch_processor.py` — the
  `repository.edited` processor.
- `src/docverse_server/services/ref_deleted_processor.py` and
  `src/docverse_server/services/dashboard_templates/` — the `delete`,
  `push`, rename, transfer, and installation processors.
- `src/docverse_server/worker/functions/git_ref_audit.py` and
  `src/docverse_server/worker/functions/project_github_resolve.py` — the
  audit and resolve triggers.
- `src/docverse_server/services/keeper_sync/mappers.py` —
  `derive_tracking_source`, keeper-sync's version of the rule, and
  `map_edition_tracking`, whose `TrackingDerivation` carries the
  `tracking_source` keeper-sync logs.
- `src/docverse_server/services/keeper_sync_push_processor.py` and
  `src/docverse_server/services/keeper_sync/push_hints.py` — the
  `push` delivery's keeper-sync step and the rules of the
  `github_pushed_refs` map it writes.
- [Metrics events](metrics.md) — `github_webhook_received`,
  `edition_lifecycle`, and `edition_published`.
- `tests/docs_test.py` — fails when this page stops matching the event
  router, the GitHub and push hot-path settings, the rule's triggers
  and log lines, the push step's skip reasons and log lines, or the
  edition endpoints the operators' section relies on.
- SQR-112, the Docverse design.
