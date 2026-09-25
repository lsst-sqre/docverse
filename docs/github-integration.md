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
  leaves nothing half-done.

Every delivery, whatever became of it, publishes one
`github_webhook_received` event; see the
[metrics catalog](metrics.md#github_webhook_received).

| Event | What Docverse does |
| --- | --- |
| `push` | Enqueues one `dashboard_sync` for each dashboard-template binding pinned to the pushed repository and ref whose `root_path` the push touched. The changed paths come from the payload, or from GitHub's compare API when the payload's commit list is truncated. A push never creates a build or moves an edition: builds arrive through the upload API. |
| `delete` | For a deleted branch or tag, soft-deletes and unpublishes every live, non-exempt `draft` edition tracking it, on every project bound to the repository, then enqueues one `dashboard_build` per affected project. Release editions and `__main` are never swept. |
| `repository.renamed` | Rewrites the repository name on bound projects and on dashboard-template bindings and templates, matched by `repository.id` — and, for a template binding that has never synced, by its old name. |
| `repository.transferred` | Rewrites the owner, owner id, and name on the same rows, matched by `repository.id` only. |
| `repository.edited` | When `changes` carries `default_branch`, applies the new default branch to every project bound to the repository: see [The default branch](#the-default-branch). Any other edit — description, homepage, topics — is logged and ignored. |
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
one does not.

### Three triggers, one rule

Three things tell Docverse a repository's default branch, and each
brings its own evidence about whether the ref `__main` tracks still
exists:

| Trigger | When it runs | Its evidence that `__main`'s ref is gone |
| --- | --- | --- |
| `webhook` | A `repository.edited` delivery whose `changes` carry `default_branch` | `changes.default_branch.from`: the branch that just stopped being the default |
| `resolve` | `project_github_resolve`, after a project is created with a `github` binding or a `PATCH` names one | The branch the column held before, if any — a project rebound to a repository with a different default |
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
   the trigger named, or it is missing from the live ref set the audit
   fetched. Only `tracking_params.git_ref` changes.
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
- A project with no GitHub binding: its column stays `null` and every
  consumer keeps the `main` fallback.

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
  default branch renamed before the upgrade, or a project created on a
  `master` repository whose `__main` got the `main` fallback and that
  has no `main` branch;
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
  branch. Either way the rest of the organization's pass carries on,
  and its `git_ref_audit` queue job ends `completed_with_errors`.
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
what the webhook or the audit converged.

Keeper-sync therefore applies the same "ref gone" test when it maps
LTD's `main` edition in `git_refs` mode, and tracks the project's
default branch instead of LTD's ref when all of these hold:

- the column is known — not `null` — and differs from LTD's ref;
- this visit fetched the repository's live ref set;
- LTD's ref is not in it.

Otherwise LTD's value stands. A ref that still exists is deliberate
non-default tracking, a `null` column has nothing to follow, and a ref
fetch that failed — or a project with no binding — is no evidence that
the ref is gone. A visit whose fetch failed therefore maps LTD's ref
for that visit, and the next visit with a live set converges again.

The live set is fetched at most once per visit and shared with the
proactive lifecycle pass. A project with neither a `ref_deleted` nor a
`draft_inactivity` rule pays for the fetch only when LTD's `main`
differs from a known default branch.

LTD's own view is kept: `annotations.ltd_tracked_refs` on the edition's
`keeper_sync_state` row still records what LTD said. Which way a visit
went is on the debug line
`Derived keeper-sync edition tracking and kind`: `tracking_source` is
`ltd` or `default_branch`, alongside `ltd_tracked_refs` and the
resulting `git_ref`.

This governs only the ref a synced `__main` tracks. Which build it
serves is still keeper-sync's business, and follows LTD's `main`
edition.

## Configuration

| Setting | Environment variable | Default | Phalanx value |
| --- | --- | --- | --- |
| `github_app_id` | `DOCVERSE_GITHUB_APP_ID` | unset | `config.githubAppId` |
| `github_app_private_key` | `DOCVERSE_GITHUB_APP_PRIVATE_KEY` | unset | the `DOCVERSE_GITHUB_APP_PRIVATE_KEY` key of the application's Vault secret |
| `github_webhook_secret` | `DOCVERSE_GITHUB_WEBHOOK_SECRET` | unset | the `DOCVERSE_GITHUB_WEBHOOK_SECRET` key of the application's Vault secret |
| `git_ref_audit_enabled` | `DOCVERSE_GIT_REF_AUDIT_ENABLED` | `false` | `config.maintenance.gitRefAuditEnabled` |

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
`git_ref_audit_run_id` on an audit's lines, for example.

| Field | Bound by | Meaning |
| --- | --- | --- |
| `trigger` | the rule | `webhook`, `resolve`, or `audit` |
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
| `Processed repository.edited default branch change` | info | `old_default_branch`, `new_default_branch`, `projects_matched`, `main_rewrites` |
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
| `Skipping default branch: project binding changed during resolve` | info | — |
| `Git ref audit: GitHub repository metadata fetch failed, skipping default branch for this pass` | warning | `owner`, `repo`, `installation_id`, `error`, `error_type` |
| `Git ref audit completed for org` | info | `had_failures`, `default_branch_updates`, `main_rewrites` |

`drafts_retired` counts the drafts step 3 retired; `main_rewritten_from`
is the ref `__main` tracked before, or null when it was left alone. On
the audit's summary line, `default_branch_updates` counts the projects
whose column took a new value and `main_rewrites` the projects whose
`__main` was rewritten, across the organization's pass.

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

A keeper-synced `__main` keeps a hand-set tracking only while it agrees
with LTD or LTD's ref is gone: keeper-sync realigns `__main` with LTD's
`main` edition on every visit, so a `PATCH` away from a ref LTD tracks
and that still exists is undone at the next visit. Fix those in LTD, or
delete the old branch.

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
- `src/docverse_server/services/default_branch_processor.py` — the
  `repository.edited` processor.
- `src/docverse_server/services/ref_deleted_processor.py` and
  `src/docverse_server/services/dashboard_templates/` — the `delete`,
  `push`, rename, transfer, and installation processors.
- `src/docverse_server/worker/functions/git_ref_audit.py` and
  `src/docverse_server/worker/functions/project_github_resolve.py` — the
  audit and resolve triggers.
- `src/docverse_server/services/keeper_sync/mappers.py` —
  `derive_tracking_source`, keeper-sync's version of the rule.
- [Metrics events](metrics.md) — `github_webhook_received`,
  `edition_lifecycle`, and `edition_published`.
- `tests/docs_test.py` — fails when this page stops matching the event
  router, the GitHub settings, the rule's triggers and log lines, or the
  edition endpoints the operators' section relies on.
- SQR-112, the Docverse design.
