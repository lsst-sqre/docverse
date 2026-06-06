---
name: stoker-implement
description: Drive a single pre-selected task through stoker-work's TDD cycle and commit/push/PR/close it (or stuck-mark it on failure). Use when invoked from the implement-phase prompt — the host has already picked the task and checked out its branch.
---
<!-- stoker-managed: skills:.agents/skills/stoker-implement/SKILL.md:a4de715878f733e0 -->

# stoker-implement — drive one pre-selected stoker task to completion

You are operating inside the **implementation** phase of a two-phase
stoker iteration driven by `stoker run`. By the time this skill is
invoked:

- The host has already run a separate selection phase that picked
  exactly one task.
- The host has already checked out the task's branch inside the
  container.
- The implement-phase prompt you just received carries the picked
  task's issue number, branch, full issue body, parent PRD body (if
  any), and current git state.

Your job is to take that one task from here through red/green/
refactor, then commit/push/open-PR/close — or route to the stuck
path on failure.

If no other memo or instruction contradicts it, the commit / PR
conventions in this skill are authoritative for this iteration.

## Phase 1: Drive the task through TDD

Follow **Phases 0.5–4** of `.claude/skills/stoker-work/SKILL.md`:

- Phase 0.5 (Read project mechanics) — `stoker-work` will load
  `.claude/skills/project-mechanics/SKILL.md` for the repo's command
  names. If that file is missing, route to the stuck path (Phase 4
  below).
- Phase 1 (Understand the task) — read the issue body (provided in
  the prompt), the parent PRD body (if provided), and relevant code.
- Phase 2 (Plan) — only if not already pre-planned by the task body.
- Phase 3 (Red/green/refactor) — one test, one behavior at a time.
- Phase 4 (Final validation) — runs `<lint_all>` + `<typing>` +
  `<complete_test>` against the project-mechanics commands and
  guarantees a clean fixpoint, with the intent-to-add files reset
  so this skill stages with intentionality.

If `stoker-work` Phase 4 cannot be made green within this iteration
(e.g. pre-existing test failures, missing upstream dependency, or
the project-mechanics file is missing), route to the stuck path
(Phase 4 of *this* skill) — do not push broken state.

**Turn-end invariant.** Your turn must not end until you have either
(success) committed, pushed, opened/updated the PR, and closed the
issue, or (failure) taken the stuck path. Never end your turn with a
dirty working tree, with validation still running in the background, or
on a `Monitor` wait for a still-running command. This covers **any**
project-mechanics command in **any** phase — including a Phase 3 Red/Green
`<focused_test>`, not just Phase 4's final validation. Under `stoker run`
the agent is single-shot headless, so ending your turn (or waiting on a
backgrounded test via `Monitor`) kills the run before the work reports
and leaves the tree dirty — the host's next iteration will abort on it.
Run every such command in the foreground and block on it, however long
it takes.

## Phase 2: Commit (success path)

`stoker-work` Phase 4 already left the working tree at a fixpoint with
the original untracked-file state restored. Do not re-run
`<lint_all>` here — that's the inverse of "fixpoint, then stage once
with intentionality." Just stage and commit.

Stage with `git add <paths>` listing every path that should land in
this commit. `git add -A` is allowed only if you've already verified
every working-tree change is intentional.

Commit using this template verbatim — both `Key decisions:` and
`Next-iteration notes:` are required sections so the next
iteration's selection phase can parse them from `git log`:

```
<imperative subject, ≤70 chars>

<1–2 sentence paragraph describing what this slice does and why.>

Key decisions:
- <non-obvious design choice>
- <another, if any>

Next-iteration notes:
- <migrations to run, scaffolding left in place, assumed state — or "None.">

Closes #<task_issue>
```

No `Co-authored-by:` trailer (signing, if configured, is
self-identifying). No files-changed section (`git show` covers it).

**Do not disable signing.** Whether commits are signed is controlled
by `git config commit.gpgsign`, which `stoker run` configures from
the profile's signing secrets before the agent ever runs. Run `git
commit` plainly; never pass `-c commit.gpgsign=false`,
`--no-gpg-sign`, or any other flag that disables signing on a single
invocation. If a commit fails because signing is misconfigured, that
is a sandbox / profile bug to escalate via the stuck path — not
something to paper over per-commit.

**Pre-commit hook retries:** if `git commit`'s hook fails (the
staged-file run catches something `stoker-work` Phase 4's full-tree pass
missed), fix the reported issue, re-stage, and create a **new**
commit (never `--amend`, never `--no-verify`). After **3**
consecutive failed commit attempts in this iteration, route to the
stuck path.

## Phase 3: Push, PR, close

PR creation is delegated to the `stoker-create-pr` skill — it owns title
format, body shape, the `Closes #N` / `PRD:` trailers, and the
"create new vs. update existing OPEN PR" branching. Invoke it now,
passing the picked task issue's number and parent PRD (if any) as
context.

`stoker-create-pr` will:

- Push the branch.
- Detect whether an open PR already exists for the branch.
- Either create a new PR or regenerate the existing OPEN PR's body
  from the branch's cumulative scope.
- Skip handling if the PR is already MERGED / CLOSED.

If `stoker-create-pr` reports a push failure (non-fast-forward, permission,
network), **do not** retry or rebase from here. Route to the stuck
path (Phase 4 of this skill).

After `stoker-create-pr` returns the PR URL (or the merged-PR signal),
proceed with the comment + close steps below.

Comment on the task issue:

```
✓ Implemented in commit `<sha>` — PR #<pr_number>
```

Close the task issue:

```
gh issue close <task_issue>
```

Closing immediately (rather than waiting for PR merge) lets the
next iteration's host filter see dependent tasks become eligible.
If the PR is later rejected, the issue must be manually reopened —
this tradeoff is accepted.

## Phase 4: Stuck path

Use this path when:

- `stoker-work` Phase 4 validation cannot be made green in this iteration.
- `stoker-create-pr` reports a push failure (non-fast-forward, permission,
  network).
- 3 consecutive pre-commit hook failures.
- `.claude/skills/project-mechanics/SKILL.md` is missing and you
  could not safely guess the test/lint commands.
- Any other blocker that prevents a clean commit + push + close.

(Branch-resolution failures are handled by the host before this
skill is invoked, so you will not reach this path for a missing or
malformed branch.)

Steps:

1. Stage what you have. Commit with the stuck template:

   ```
   WIP(stuck) <imperative subject>

   <paragraph: what was attempted.>

   Blocking:
   - <why stuck>

   Refs #<task_issue>
   ```

2. Push the branch.
3. **Do not** create or update a PR.
4. Comment on the task issue describing what was attempted and
   what's blocking.
5. Add the stuck label:

   ```
   gh issue edit <task_issue> --add-label agent-stuck
   ```

6. **Do not** close the issue.

A human clears the `agent-stuck` label to re-enable the issue in
later runs.

After Phase 3 (success) or Phase 4 (stuck), simply end your response.
The host loop owns the iteration sentinel and continues on its own.
