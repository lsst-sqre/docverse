---
name: afk-iterate
description: Run one iteration of the Ralph AFK loop — pick the next eligible task from a host-prepared shortlist, drive it through work-it's TDD phases, then commit/push/PR/close (or WIP-stuck on failure). Use when invoked from ralph/prompt.md or when the user asks to run an AFK iteration.
---

# afk-iterate — single Ralph loop iteration

You are operating inside a single iteration of the Ralph AFK loop started by
`ralph/afk.sh`. The host has already:

- Pre-fetched the shortlist of eligible `prd-task` issues (Type=AFK, all blockers
  closed, not labeled `agent-stuck`, optionally scoped to one PRD).
- Run `git fetch origin` once at loop start.
- Injected recent commits, `git status`, branch, and the shortlist into the
  prompt you just received.

Your job is to complete **one** of those tasks end-to-end — or, if nothing is
eligible, exit cleanly.

If no other memo or instruction contradicts it, the commit/PR conventions in
this skill are authoritative for this iteration.

## Phase 1: Pick a task

Read the shortlist in the prompt. Each entry includes the issue number, title,
and full body (which contains the Metadata table with `Parent PRD`, `Jira Key`,
`Jira URL`, `Task Order`, `Type`, `Blocked by`, `Parallel with`, and `Branch`).

**If the shortlist is empty**, skip to Phase 8 (emit sentinel and stop).

**If the prompt includes a forced `--issue N` marker**, pick that issue and
skip the ranking.

Otherwise rank the shortlist by:

1. Critical bug fixes (look for `bug`, `regression`, or severity language in the
   body).
2. Developer infrastructure (tooling, CI, local dev, test scaffolding).
3. Tracer-bullet feature slices that unblock future work.
4. Polish / quick wins (small, self-contained improvements).
5. Refactors with no user-visible effect.

Tiebreaker: ascending `Task Order` from the Metadata table.

`Parallel with` is informational and does **not** affect gating.

State the pick in one sentence ("Picking #N: <title> because <reason>") before
moving on.

## Phase 2: Check out the task branch

Read the `Branch:` field from the task's Metadata table. Check it out:

- If it exists locally → `git checkout <branch>`.
- If it exists on `origin` → `git checkout -b <branch> origin/<branch>`.
- Otherwise → `git checkout -b <branch> main`.

Never work on `main`. If the Metadata table is missing a `Branch` field, route
to the stuck path (Phase 7) with "missing Branch metadata" as the blocker.

## Phase 3: Drive the task through TDD

Follow **Phases 1–4** of `.claude/skills/work-it/SKILL.md`:

- Phase 1 (Understand the task) — read the issue body, linked PRD if referenced,
  and relevant code.
- Phase 2 (Plan) — only if not already pre-planned by the task body.
- Phase 3 (Red/green/refactor) — one test, one behavior at a time.
- Phase 4 (Final validation) — run the full lint + typing + test suites.

**Do not** follow `work-it` Phase 5 (Commit) — this skill's Phase 4 below owns
the commit message format for AFK work.

If any step in work-it Phase 4 fails and cannot be fixed within this iteration
(e.g. pre-existing test failures, missing upstream dependency), route to the
stuck path (Phase 7).

## Phase 4: Commit (success path)

Stage the relevant files (do not use `git add -A`). Commit with a signed commit
using this template verbatim — both `Key decisions:` and `Next-iteration notes:`
are required sections so the next iteration can parse them from `git log`:

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

## Phase 5: Push

Push the branch:

```
git push -u origin <branch>
```

If the push fails (non-fast-forward, permission, network), do **not** retry or
rebase. Route to the stuck path (Phase 7).

## Phase 6: PR handling

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

## Phase 7: Stuck path

Use this path when:

- work-it Phase 4 validation cannot be made green in this iteration.
- `git push` fails.
- 3 consecutive pre-commit hook failures.
- Task Metadata is malformed (missing `Branch`, etc.).
- Any other blocker that prevents a clean commit+push+close.

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

## Phase 8: Emit the completion sentinel

After the iteration finishes — whether success, stuck, or empty shortlist —
the last line of your response **must** be exactly:

```
<ralph-status>done</ralph-status>
```

The host greps the final output for this string. If it's missing, the loop
aborts.
