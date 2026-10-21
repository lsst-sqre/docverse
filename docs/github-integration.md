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
across a rename, how a draft a lifecycle rule retired comes back when
its branch is active again, the knobs that shape all of it, how to read
what happened, and what to do when Docverse deliberately leaves
`__main` alone.

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
with the installation token for the ids the job has just recorded, and
hands them to the rule as the audit does:

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
- A repository with no commits yet has a default branch but no refs,
  and GitHub answers the ref fetch with `409`. The job goes as after a
  failed fetch, but logs
  `Resolve: GitHub repository is empty, recording default branch without live refs`
  at info instead of the warning: an empty repository is not a failed
  read.
- The fetch is never anonymous. If a rebind clears the installation id
  after the job records it but before the fetch, the job skips the
  fetch without a GitHub request; the rebind's own resolve reads the
  new binding.

A resolve on a project whose column is already set spends nothing on
the ref set; the column's previous value is its evidence. A project
keeper-sync creates gets the same resolve; see
[GitHub ids on creation](#github-ids-on-creation).

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
only on a change. The daily `git_ref_audit` is what fills it in — and,
through `project_github_resolve`, the numeric GitHub ids too (see
[The id backfill](#the-id-backfill)). Its
per-project pass already fetches each bound repository's live branches
and tags for the `ref_deleted` rule; it also reads
`GET /repos/{owner}/{repo}` for `default_branch` and applies the rule
with the live set as its evidence. The first tick after an upgrade:

- fills the column on every bound project it can read — one with an
  installation id (see **Auth** below) — in every
  organization the audit covers. Keeper-synced projects are included,
  and for one synced before keeper-sync enqueued a resolve on creation
  (see [GitHub ids on creation](#github-ids-on-creation)) the audit is
  the usual seed;
- rewrites any `__main` tracking a ref that no longer exists — a
  default branch renamed before the upgrade, or a project created before
  it on a `master` repository whose `__main` got the `main` fallback and
  that has no `main` branch. A project created since converges in its
  own resolve (see [A new project](#a-new-project)); the audit is the
  backstop for one whose resolve could not list its refs;
- converges what the webhook cannot reach: deliveries that were lost or
  failed. A repository without the App installed gets neither, until the
  App is installed on it.

The audit runs only while `git_ref_audit_enabled` is on. The setting
ships off, and Phalanx turns it on per environment with
`config.maintenance.gitRefAuditEnabled`. With it off, columns fill only
as webhooks and resolves happen to arrive, and nothing heals a lost
delivery.

- **Auth.** A project with an installation id is read with that
  installation's token. One without is skipped before any GitHub
  request: the pass reads neither its refs nor its repository, so it
  reaps none of the project's drafts and leaves its column as it is.
  The skip is not a failure and does not turn the queue job
  `completed_with_errors`. The summary line counts the organization's
  skipped projects as `projects_skipped_no_installation`, and one more
  line, `Git ref audit: skipped projects without a GitHub App installation`,
  names up to ten of them (see [Reading an outcome](#reading-an-outcome)),
  so its `org` is an organization that still needs the App. The id
  backfill runs before the pass's reads: once the App is installed on
  the repository, the project's resolve records its installation id,
  and the next pass reads it with the token, with nothing else to do.
  An older release read such a project anonymously instead, and its
  symptom is a `GitHub refs fetch returned non-2xx` warning with
  `status_code` 403 or 429 for project after project, because
  anonymous reads share GitHub's unauthenticated limit of 60 requests an
  hour per source address across every pod behind the cluster's NAT
  address, which an organization with more than about 30 such projects
  exhausts on every pass. The audit needs the three GitHub App
  settings: without them an organization's pass fails outright.
- **Empty repositories.** A project whose repository has no commits
  yet, which GitHub's ref endpoints answer with `409`, is skipped
  rather than failed — it counts toward
  `projects_skipped_empty_repository`, not toward
  `completed_with_errors` — until it gains its first commit, after
  which the next pass reads it with no other change.
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
- **Cost.** One more GitHub request per day per bound project with an
  installation id, plus one `project_github_resolve` job per bound
  project still missing its ids.

There is no manual entrypoint for the audit. To converge one project
without waiting for the next tick, `PATCH /orgs/{org}/projects/{project}`
its `github` binding with the owner and repository it already has. That
re-runs `project_github_resolve`, which records the branch and, when the
column held a different branch that `__main` still tracks, rewrites it.
The `PATCH` clears the three numeric GitHub ids until the resolve reads
them back.

### The id backfill

A project records three numeric ids from GitHub —
`github_installation_id`, `github_owner_id`, and `github_repo_id` —
which webhook deliveries match on (`repository.transferred` matches by
`repository.id` alone) and which give the audit its installation token.
The resolve a project's creation enqueues records them (see
[A new project](#a-new-project)), but a project can miss that resolve:
keeper-sync created projects without one until it began resolving ids
on creation (see [GitHub ids on creation](#github-ids-on-creation)), an
enqueue can fail, and the `installation.created` delivery only reaches
the projects that existed when the App was installed.

So each pass, before its GitHub reads, enqueues one
`project_github_resolve` onto the maintenance pool for every
non-deleted bound project in the organization whose `github_repo_id`
is `null`:

- **Installed.** Where the App is installed on the repository, the
  resolve records the three ids and the default branch, so the next
  pass enqueues nothing for the project and reads it with its
  installation's token.
- **Not installed.** Where it is not, the resolve ends `not_installed`
  and the ids stay `null`, so every pass enqueues the project again, at
  the cost of one GitHub request, until the App is installed and its
  `installation` delivery records them. Meanwhile each pass skips the
  project's reads (see **Auth** above).
- **No cap.** A first pass after an upgrade can enqueue a job for nearly
  every project in an organization. The maintenance pool's concurrency
  bounds how many run at once, and the resolve's own retry and backoff
  absorb GitHub's rate limits. The pool is shared, though: the other
  organizations' audit jobs, queued by the same tick, still run first,
  but a maintenance job queued after the burst — the resolve for a
  project created through the API, the next `lifecycle_eval` tick, a
  `purgatory_cleanup` or `edition_reconcile` pass, a reaper sweep —
  waits behind it, so after an upgrade those can start late until the
  burst drains.
- **Failures.** Each enqueue runs in a transaction of its own. One that
  fails is sent to Sentry and logged as
  `Failed to enqueue project_github_resolve`, with `project` and
  `project_id`. The project is still audited in the same pass, the
  failure does not turn the queue job `completed_with_errors`, and the
  next pass enqueues it again.

The pass's summary line, `Git ref audit completed for org`, counts the
jobs it enqueued as `github_resolves_enqueued`. Once a first pass has
recorded the ids it can, the count is mostly the organization's
projects on repositories the App is not installed on, so a count that
stays above zero pass after pass, on the line's `org`, names an
organization that still needs the App. Its
`projects_skipped_no_installation` says the same from the read side,
and the `skipped projects without a GitHub App installation` line names
up to ten of those projects. With
`git_ref_audit_enabled` off, a project that missed its resolve keeps
`null` ids until an `installation` delivery reaches it.

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
fetch that failed — or a project with no binding, or no installation
id (see below) — is no evidence that the ref is gone. A `__main` still tracking LTD's ref after a failed
fetch keeps it for that visit, and the next visit with a live set
converges it; a `__main` already on the default branch is not moved
back by a failed fetch.

When a visit moves `__main` onto the default branch itself (case 2), it
also retires the duplicate drafts, as step 3 of the rule does after a
rewrite: every live, non-exempt `draft` in `git_ref` mode tracking that
branch is soft-deleted and unpublished. A synced draft's
`keeper_sync_state` row is tombstoned with reason `lifecycle_delete` and
note `ref_deleted`, so later visits skip its LTD edition until LTD
rebuilds it; a visit after a rebuild judges the tombstone again (see
[Lifecycle deletions come back with activity](#lifecycle-deletions-come-back-with-activity)).
Unlike the rule, a keeper-sync visit does not repoint `__main` (which build it serves stays LTD's), publish
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

Sync-time live refs need the GitHub App installed on the repository:
the set is read only with an installation token. A project without a
`github_installation_id` skips the fetch before any GitHub request and
visits as it would after a failed fetch — `ref_deleted` retires none of
its drafts at import, and `__main` keeps LTD's ref unless another
trigger already moved it onto the default branch. The skip is the
debug line
`Keeper sync: project has no GitHub App installation, live refs unavailable for this pass`,
with `owner`, `repo`, `project`, and `project_id`; it does not log the
warning
`Keeper sync: GitHub ref fetch failed, live refs unavailable for this pass`,
which stays for a project with an installation whose read fails. Once
the App is installed, the project's resolve records its installation id
— each audit pass enqueues it meanwhile (see
[The id backfill](#the-id-backfill)) — and the next visit reads the set
with the token, with nothing else to do. An older release read such a
project anonymously instead, and once GitHub's unauthenticated limit of
60 requests an hour, shared by every pod behind the cluster's NAT
address, ran out, logged that warning on every visit.

A project whose repository has no commits yet visits the same way: GitHub
answers its ref endpoints with `409`, so there is no live set to read.
That is not a failed read either, so the visit logs the debug line
`Keeper sync: GitHub repository is empty, live refs unavailable for this pass`,
with `owner`, `repo`, `installation_id`, `project`, and `project_id`,
in place of the warning. The first visit after the repository's first
push reads the set, with nothing else to do.

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

### GitHub ids on creation

When a sync creates the Docverse project for an LTD product whose
`doc_repo` is a github.com URL, it binds the project to that repository
and, once the project's row has committed, enqueues one
`project_github_resolve` onto the maintenance pool — the job the REST
create handler enqueues (see [A new project](#a-new-project)). Within
minutes the project has its `github_installation_id`, `github_owner_id`,
`github_repo_id`, and `github_default_branch`, so deliveries that match
by `repository.id` find it, and the audit reads it with its
installation's token.

- Only the visit that creates the project enqueues. Re-syncing an
  existing project enqueues nothing, so a project synced before this
  behaviour existed — or one whose enqueue failed — gets its resolve
  from the audit instead (see [The id backfill](#the-id-backfill)).
- A product whose `doc_repo` is not on github.com becomes a project
  with a `source_url` and no binding, and enqueues nothing.
- The enqueue is best-effort. A failure is sent to Sentry and logged as
  `Failed to enqueue project_github_resolve`, with the `project_id`, and
  the sync carries on to the project's editions.
- A repository the App is not installed on resolves `not_installed`: the
  ids stay `null` until an `installation` delivery records them, and
  each audit pass enqueues the project's resolve again meanwhile. Until
  then its syncs skip the live ref set (see
  [Keeper-synced projects](#keeper-synced-projects)).

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
- **Expired refs are left for `tier_main`.** A stamp drops no ref
  whose window has passed: `tier_main` prunes each one on its next
  visit and reports it as `expired`, so a push to another ref of a busy
  repository cannot drop that report first.
- **The map is capped.** It holds at most 20 refs; a stamp that would
  leave more drops the oldest pushes first, expired ones before any
  live one, so a repository that pushes many tags at once cannot grow
  the row without bound.
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
  for any non-`main` edition last synced an hour or more ago. An edition
  a lifecycle rule tombstoned counts too, aging from its tombstoning
  when keeper-sync never synced it, so the sync can judge the tombstone
  again should LTD have rebuilt the edition; an edition an operator
  deleted does not.

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
   editions feed the ref, the newest is checked. A draft a lifecycle
   rule retired still counts, since its author pushing again is what
   brings it back: an edition the rule soft-deleted (`delete_reason`
   `lifecycle_delete`, or none on record) and a state row tombstoned
   `lifecycle_delete` or `lifecycle_preemptive` are both looked at. An
   edition an operator deleted (`manual_delete`) is not.
2. **Ask LTD.** One `GET /editions/<id>`, skipped when the `main` check
   already fetched that edition this tick. The edition is `rebuilt` when
   its `date_rebuilt` is newer than the `date_rebuilt_seen` its state
   row recorded at the last sync (or, if the row recorded none, its
   `date_last_synced`, or for a tombstoned row its `date_tombstoned`),
   and `unchanged` otherwise: the push's CI has not uploaded yet. A
   `rebuilt` tombstoned edition enqueues the project's sync, which
   judges the tombstone again against the lifecycle rules: it clears
   the tombstone and imports the edition when no rule matches the
   rebuilt edition any more, and keeps it, stamped with the rebuild it
   judged, when one still does.
3. **Find a tombstone by name.** An edition a lifecycle rule vetoed at
   import (`lifecycle_preemptive`) has no Docverse edition to track the
   ref, and its state row records only LTD's slug for it. So when step
   1 finds nothing, or LTD answers `404` for what it found, the
   project's edition listing (`GET /products/<slug>/editions/`, read
   once per project per tick, with the state rows of the editions it
   lists) is searched for a lifecycle tombstone on the slug LTD gives
   the ref's edition: the ticket key for a `tickets/DM-1234` branch
   (`DM-1234`), and otherwise the ref with its slashes made hyphens
   (`u/jsick/feature` → `u-jsick-feature`). That edition is fetched,
   and when it tracks the pushed ref it is checked as in step 2.
4. **Fall back to the listing.** When no edition keeper-sync has seen
   tracks the ref, the listing is checked the way
   `keeper_sync_tier_discovery` checks it, for editions keeper-sync has
   no state row for. Each of those, newest
   first and at most three, is fetched (`GET /editions/<id>`, also once
   per project per tick) to read the ref it tracks. One that tracks the
   pushed ref is a `new_edition`: LTD created it for the push's CI
   upload. None is `not_found`: LTD has not created the ref's edition
   yet. An unseen edition that tracks some other ref does not count for
   this one: a dormant project can carry one from a branch nobody
   pushed to lately, and syncing it would not bring in the pushed ref's
   edition. The push has put the project on discovery's fast cadence,
   which syncs that edition.

| Outcome | What the visit found | The stamp |
| --- | --- | --- |
| `rebuilt` | LTD rebuilt the ref's edition since its last sync, or since a lifecycle rule tombstoned it | Cleared once the sync is enqueued |
| `new_edition` | No edition keeper-sync has seen tracks the ref, and LTD lists one keeper-sync has not seen that tracks it | Cleared once the sync is enqueued |
| `unchanged` | The ref's edition is as keeper-sync last synced or tombstoned it | Kept |
| `not_found` | No edition keeper-sync has seen tracks the ref, and LTD lists nothing new that tracks it | Kept |
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
tick (its edition, then the edition of the lifecycle tombstone its name
points to), plus the listing and at most three fetches of the unseen
editions it shows, the listing and those fetches shared by the
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

`pushed_refs` counts the refs on the project's map after the stamp,
including any whose window has passed that `tier_main` has yet to
prune, and `pushed_at` is the time stamped. The handler's own
`Processed push webhook` closes every push delivery: `enqueued` counts
its `dashboard_sync` jobs, and `projects_stamped` the projects stamped,
null when the keeper-sync step failed.

## Lifecycle deletions come back with activity

Lifecycle rules retire editions on evidence that can go stale. A draft
`lifecycle_eval` retired under `draft_inactivity` had gone idle; a draft
a `delete` delivery, the audit, or a `__main` rewrite retired under
`ref_deleted` had lost its branch. When its author pushes to that branch
again, the evidence is gone, so the deletion is undone: the edition
comes back in place, with its id, slug, and build history, at its old
URL. An edition an operator deleted is different. Their `DELETE` is a
decision rather than evidence, and only an operator undoes it (PRD
#830).

### Why an edition was deleted

Docverse records the reason in two places:

- **`editions.delete_reason`**, stamped with `date_deleted` on every
  soft delete and cleared when the edition comes back:
  `manual_delete` for the edition `DELETE` and for a project delete's
  cascade, `lifecycle_delete` for a lifecycle rule. A row deleted before
  the column existed, with no tombstone to copy a reason from, reads
  `null`, which every path treats as `lifecycle_delete`. The column is
  server-side provenance; the edition resource does not carry it.
- **The tombstone on a synced edition's `keeper_sync_state` row**, which
  `GET /orgs/{org}/keeper-sync/tombstones` lists. Its
  `tombstone_reason` is `manual_delete`; `lifecycle_delete`, when a rule
  soft-deleted the imported edition; or `lifecycle_preemptive`, when
  keeper-sync vetoed the LTD edition before importing it, so there is
  no Docverse edition at all. On a lifecycle tombstone,
  `tombstone_note` names the rule that matched, `draft_inactivity` or
  `ref_deleted`; one recorded before Docverse began naming the rule has
  no note. A native edition has no state row, so `delete_reason` is its
  only record.

### A build for the ref revives the edition

Edition slugs are never reused: `uq_editions_project_lower_slug` ignores
`date_deleted`, so a deleted edition keeps its slug. When a client build
arrives for a git ref whose edition a soft-deleted row still holds,
edition tracking settles the clash by that row's `delete_reason`:

- **`lifecycle_delete`, or `null`: revived.** The edition comes back in
  place with its id, slug, title, `lifecycle_exempt`, alternate name,
  current build, and build history, and takes the tracking the build's
  ref derives, and the derived kind too unless an operator declared
  one. A keeper-sync tombstone on it is cleared in the same transaction.
  The build then tracks onto it as onto a new edition: the edition
  advances to the build and publishes, and the project's dashboard is
  rebuilt. The `build_processing` worker publishes one
  `edition_lifecycle` event with `action` `restore` per revived
  edition. The semver aggregates (`N` and `N.M`) revive the same way.
- **`manual_delete`: left deleted.** The build is stored and processed,
  but tracks no edition on that slug; edition tracking logs a warning,
  and the build carries on with its other editions. Nothing reaches
  Sentry.

### Keeper-sync judges a rebuilt tombstone again

A lifecycle tombstone on a synced edition once vetoed every later visit.
Now a visit looks at a `lifecycle_delete` or `lifecycle_preemptive`
tombstone again once LTD has rebuilt the edition since the state row
last saw it: LTD's `date_rebuilt` is later than the row's
`date_rebuilt_seen` or, on a row that never saw one, its
`date_tombstoned`. Until then the visit short-circuits on the tombstone
as before, at no cost beyond the edition it already fetched.

The judgement is the one the proactive pass makes at import: the
rebuilt LTD edition, mapped as an import would map it, is evaluated
against the project's resolved `draft_inactivity` and `ref_deleted`
rules and the live ref set the visit already holds (see
[Keeper-synced projects](#keeper-synced-projects)). No LTD request is
added. The verdict is one of:

- **No rule matches: cleared.** The tombstone is cleared exactly as
  `DELETE /orgs/{org}/keeper-sync/tombstones/{tombstone}` clears one —
  a `lifecycle_delete` edition's soft-deleted row is revived in place,
  keeping its id — and the visit goes on to import the edition: its
  build is synced, the edition published, and the `keeper_sync_project`
  worker publishes an `edition_lifecycle` `restore` event for it. The
  tombstone leaves the listing. An LTD edition in `manual` mode, which
  cannot be judged before its build is mapped, is cleared the same way
  and left to the passes that judge imported editions.
- **A rule still matches: kept.** The tombstone stands, with
  `date_tombstoned` and `tombstone_reason` as recorded. Its
  `tombstone_note` is refreshed with the rule that matched this time —
  a draft vetoed as idle may now be vetoed because its branch is gone —
  and the row is stamped with the rebuild it was judged on
  (`date_rebuilt_seen`) and when (`date_last_synced`), so it is not
  judged again until LTD rebuilds the edition once more. This is what
  keeps a trailing LTD build of a merged-and-deleted branch vetoed: its
  ref is still gone, so `ref_deleted` matches again.
- **The live ref set could not be read: kept, unjudged.** A failed
  GitHub read is no evidence that the ref is live, so the tombstone
  stands and nothing is stamped; the next visit judges it again. A
  project with no GitHub binding, no installation id, or no commits has
  no ref set to read at all, and is judged with `ref_deleted` matching
  nothing, as it is everywhere else.

A rebuild alone brings the project in for that visit:
`keeper_sync_tier_other` counts a lifecycle-tombstoned edition when it
looks for a stale one (see [A push counts as hot](#a-push-counts-as-hot)),
and the push hot path finds the tombstoned edition a pushed ref feeds
(see [The `tier_main` check](#the-tier_main-check)). A run or a
per-project refresh judges every rebuilt tombstone it walks past too.
Each `keeper_sync_project` completion line counts the visit's verdicts
as `tombstones_cleared` and `tombstones_kept` (see
[the budget page's log lines](keeper-sync-budget.md#log-lines)).

A visit importing an LTD edition can also meet a soft-deleted Docverse
edition holding its slug with no tombstone of its own in the way — a
native edition on the same slug, say, or one deleted before tombstones
existed. It settles that clash as edition tracking does: a
`lifecycle_delete` or `null` row is revived and the edition imported
onto it, and a `manual_delete` row is left deleted, the visit importing
nothing for it and logging a warning rather than failing the edition.

### An operator's delete is permanent

An edition deleted with

```
DELETE /orgs/{org}/projects/{project}/editions/{edition}
```

or with its project stays deleted: a new build for its ref tracks no
edition, and keeper-sync short-circuits on its `manual_delete` tombstone
however often LTD rebuilds it. The way back is the admin restore:

```
POST /orgs/{org}/projects/{project}/editions/{edition}/restore
```

It revives the edition in place whatever its `delete_reason` — same id,
slug, kind, tracking, and build history — and clears its keeper-sync
tombstone if it has one, so keeper-sync syncs it again. It publishes
the edition's current build again unless that build was itself deleted
or purged, rebuilds the project's dashboard, and answers `200` with the
edition; the editions API handler publishes one `edition_lifecycle`
`restore` event. The URL addresses only deleted editions: a live or
unknown edition answers `404`, and so does `__main`, which cannot be
deleted. The restore works on a lifecycle deletion too, for an edition
that should come back before its author pushes again. A tombstone with
no Docverse edition behind it is lifted on the keeper-sync side instead;
see [Tombstones](keeper-sync-scope.md#tombstones).

### Log lines

| Message | Level | Fields |
| --- | --- | --- |
| `Sync short-circuited: edition tombstoned` | info | `ltd_edition_id`, `ltd_edition_slug`, `tombstone_reason` |
| `Lifecycle tombstone cleared` | info | `revived_docverse_row` |
| `Lifecycle tombstone kept` | info | `rule_type` |
| `Lifecycle tombstone kept: live refs unavailable` | info | — |
| `Edition slug reserved by manual delete; LTD edition not imported` | warning | `ltd_edition_id`, `ltd_edition_slug`, `docverse_slug`, `project_id`, `project` |
| `Revived soft-deleted edition` | info | `edition_id`, `edition_slug`, `project_id`, `previous_delete_reason`, `tombstone_cleared` |
| `Edition slug reserved by manual delete; build not tracked` | warning | `slug`, `project_id` |
| `Restored edition` | info | `org`, `project`, `edition_id`, `edition_slug`, `republished` |
| `Restored edition has no build to republish` | info | `edition_id`, `edition_slug` |
| `Restored edition without republishing; its current build is not live` | warning | `edition_id`, `edition_slug`, `build_id`, `build_deleted`, `build_purged` |

- The three `Lifecycle tombstone` lines are the verdicts, and each also
  carries `ltd_edition_id`, `ltd_edition_slug`, `tombstone_reason`,
  `ltd_date_rebuilt` (the rebuild judged), `project_id`, and `project`.
  `rule_type` is the rule that matched. `revived_docverse_row` is true
  when the clear brought a soft-deleted edition back.
- `Sync short-circuited: edition tombstoned` follows a kept verdict, and
  is the only line for a tombstone the visit did not judge: a
  `manual_delete`, or a lifecycle tombstone LTD has not rebuilt since.
- `Revived soft-deleted edition` comes from every revive of an edition
  row but a tombstone clear's: edition tracking's, keeper-sync's
  import, and the admin restore. `previous_delete_reason` is the
  `delete_reason` it cleared, and `tombstone_cleared` whether a
  keeper-sync tombstone went with it.
- The `Restored edition` lines are the admin restore's; `republished`
  is whether it queued a publish of the edition's current build.

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
| `Resolve: GitHub repository is empty, recording default branch without live refs` | info | `owner`, `repo` |
| `Git ref audit: GitHub repository metadata fetch failed, skipping default branch for this pass` | warning | `owner`, `repo`, `installation_id`, `error`, `error_type` |
| `Git ref audit: default branch convergence failed, skipping project for this pass` | warning | `error`, `error_type` |
| `Git ref audit: GitHub repository is empty, skipping project for this pass` | info | `owner`, `repo`, `installation_id` |
| `Git ref audit: skipped projects without a GitHub App installation` | info | `org`, `count`, `examples` |
| `Git ref audit completed for org` | info | `had_failures`, `default_branch_updates`, `main_rewrites`, `default_branch_errors`, `github_resolves_enqueued`, `projects_skipped_no_installation`, `projects_skipped_empty_repository` |

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
hand-off. `github_resolves_enqueued` counts the `project_github_resolve`
jobs the pass's id backfill enqueued (see
[The id backfill](#the-id-backfill)), and
`projects_skipped_no_installation` the bound projects the pass skipped
unread because they have no installation id (see
[The audit is the backfill](#the-audit-is-the-backfill)). When that
count is not zero, the pass writes one more line before its summary,
`Git ref audit: skipped projects without a GitHub App installation`,
with the same number as `count` and up to ten of the projects' slugs,
in slug order, as `examples`; no line above debug names a tokenless
project on its own.

`projects_skipped_empty_repository` counts the bound projects the pass
skipped because their repository has no commits yet: GitHub answers its
ref endpoints with `409`, so the pass reads neither its refs nor its
default branch. Empty repositories are rare, so each one gets its own
info line,
`Git ref audit: GitHub repository is empty, skipping project for this pass`,
with `project` and `project_id`, and there is no per-organization line
for them. Like the tokenless skip, it is not a
failure: it does not set `had_failures` or turn the queue job
`completed_with_errors`.

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
- Undo an operator's delete. A new build, a push, or an LTD rebuild
  revives only what a lifecycle rule retired; a `manual_delete` waits
  for the admin restore.
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
- `src/docverse_server/storage/edition_store.py` —
  `EditionStore.revive_or_create`, which revives a lifecycle-deleted
  edition for a build or a sync, and `EditionStore.restore`, behind the
  admin restore.
- `src/docverse_server/services/keeper_sync/service.py` —
  `KeeperSyncService._review_lifecycle_tombstone`, keeper-sync's
  judgement of a rebuilt edition's lifecycle tombstone.
- [Metrics events](metrics.md) — `github_webhook_received`,
  `edition_lifecycle`, and `edition_published`.
- `tests/docs_test.py` — fails when this page stops matching the event
  router, the GitHub and push hot-path settings, the rule's triggers
  and log lines, the push step's skip reasons and log lines, the
  edition endpoints the operators' section relies on, or the revival
  section's delete reasons, restore route, and log lines.
- SQR-112, the Docverse design.
