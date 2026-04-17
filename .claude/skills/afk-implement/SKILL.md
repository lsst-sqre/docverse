---
name: afk-implement
description: Drive a single pre-selected task through work-it's TDD cycle and commit/push/PR/close it (or WIP-stuck on failure). Use when invoked from ralph/implement-prompt.md, after the Ralph host has already picked the task and checked out its branch.
---

# afk-implement — drive one pre-selected Ralph task to completion

You are operating inside the **implementation** phase of a two-phase Ralph
iteration driven by `ralph/afk.sh`. By the time this skill is invoked:

- The host has already run a separate selection phase that applied the
  priority rubric and picked exactly one task.
- The host has already checked out the task's branch inside the container via
  `git fetch` + `git checkout` / `checkout -b`.
- The implement-phase prompt you just received carries the picked task's
  issue number, branch, Jira key, full issue body, parent PRD body (if any),
  and current git state.

Your job is to take that one task from here through red/green/refactor, then
commit/push/open-PR/close — or route to the stuck path on failure.

If no other memo or instruction contradicts it, the commit/PR conventions in
this skill are authoritative for this iteration.

## Phase 1: Drive the task through TDD

Follow **Phases 1–4** of `.claude/skills/work-it/SKILL.md`:

- Phase 1 (Understand the task) — read the issue body (provided in the
  prompt), the parent PRD body (if provided), and relevant code.
- Phase 2 (Plan) — only if not already pre-planned by the task body.
- Phase 3 (Red/green/refactor) — one test, one behavior at a time.
- Phase 4 (Final validation) — run the full lint + typing + test suites.

**Do not** follow `work-it` Phase 5 (Commit) — this skill's Phase 2 below owns
the commit message format for AFK work.

If any step in work-it Phase 4 fails and cannot be fixed within this iteration
(e.g. pre-existing test failures, missing upstream dependency), route to the
stuck path (Phase 5).

## Phase 2: Commit (success path)

Stage the relevant files (do not use `git add -A`). Commit with a signed commit
using this template verbatim — both `Key decisions:` and `Next-iteration notes:`
are required sections so the next iteration's selection phase can parse them
from `git log`:

```
<imperative subject, ≤70 chars>

<1–2 sentence paragraph describing what this tracer bullet does and why.>

Key decisions:
- <non-obvious design choice>
- <another, if any>

Next-iteration notes:
- <migrations to run, scaffolding left in place, assumed state — or "None.">

Closes #<task_issue>
```

No Jira prefix on the subject — the branch name and PR title already carry that.
No `Co-authored-by:` trailer (SSH signing is self-identifying). No
files-changed section (`git show` covers it).

**Pre-commit hook retries:** if a pre-commit hook fails, fix the reported issue,
re-stage, and create a **new** commit (never `--amend`, never `--no-verify`).
After **3** consecutive failed commit attempts in this iteration, route to the
stuck path.

## Phase 3: Push

Push the branch:

```
git push -u origin <branch>
```

If the push fails (non-fast-forward, permission, network), do **not** retry or
rebase. Route to the stuck path (Phase 5).

## Phase 4: PR handling

Check whether a PR already exists for the branch:

```
gh pr list --head <branch> --json number,title,body --limit 1
```

- **No existing PR** → invoke the `rubin-create-pr` skill to open one.
  - Task-specific branches (`tickets/DM-XXXXX-...`): PR title uses the task
    issue's title and body includes `Closes #<task_issue>` / `PRD:
    #<parent_prd>` / Jira references per `rubin-create-pr`.
  - Single-branch PRDs (multiple tasks on one branch): fetch the parent PRD
    title with `gh issue view <parent_prd> --json title` and use that as the PR
    title. Body still includes `Closes #<task_issue>`.
- **Existing PR on this branch** → update the body to append a new line
  `Closes #<task_issue>` in the refs section (idempotent — skip if already
  present). Do not rewrite the title.

Then comment on the task issue:

```
✓ Implemented in commit `<sha>` — PR #<pr_number>
```

And close the task issue:

```
gh issue close <task_issue>
```

Closing immediately (rather than waiting for PR merge) lets the next iteration's
host filter see dependent tasks become eligible. If the PR is later rejected,
the issue must be manually reopened — this tradeoff is accepted.

## Phase 5: Stuck path

Use this path when:

- work-it Phase 4 validation cannot be made green in this iteration.
- `git push` fails.
- 3 consecutive pre-commit hook failures.
- Any other blocker in this phase that prevents a clean commit+push+close.

(Branch-resolution failures are handled by the host before this skill is
invoked, so you will not reach this path for a missing or malformed branch.)

Steps:

1. Stage what you have. Commit with the stuck template (signed, no
   `--no-verify`):

   ```
   WIP(stuck) <imperative subject>

   <paragraph: what was attempted.>

   Blocking:
   - <why stuck>

   Refs #<task_issue>
   ```

2. Push the branch.
3. **Do not** create or update a PR.
4. Comment on the task issue describing what was attempted and what's blocking.
5. Add the stuck label:

   ```
   gh issue edit <task_issue> --add-label agent-stuck
   ```

6. **Do not** close the issue.

A human clears the `agent-stuck` label to re-enable the issue in later runs.

After Phase 4 (success) or Phase 5 (stuck), simply end your response. The host
loop owns the iteration sentinel and will continue on its own.
