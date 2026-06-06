---
name: stoker-work
description: The universal red/green/refactor TDD methodology building block — drives a unit of work through plan → failing test → minimal implementation → refactor → validation against this repo's project-mechanics. Use when invoked from `stoker-implement` (always delegates here for the dev cycle), from `stoker-fixup` or `stoker-rebase` when a finding or conflict warrants the full TDD discipline rather than an in-place apply-and-validate, or any time the user wants to drive a feature/bug-fix/refactor through plan → test → implement → validate.
---
<!-- stoker-managed: skills:.claude/skills/stoker-work/SKILL.md:d3eafd8629e5f1ba -->

# stoker-work — Development Work Cycle

Execute a complete unit of development work using red/green/refactor
TDD in tracer-bullet slices.

## Phase 0: Branch safety

**Never do work on the `main` branch.** Before any implementation:

1. Check the current branch with `git branch --show-current`.
2. If already on a non-main feature branch, proceed to Phase 0.5.
3. If on `main`:
   - If the task or issue specifies a branch name, check it out
     (`git checkout <branch>`, falling through to
     `git checkout -b <branch> origin/<branch>` or
     `git checkout -b <branch> origin/main` as needed). New branches
     must be rooted at `origin/main`, not local `main`, so a stale
     sandbox `main` never seeds the branch — the host refreshes
     `origin/main` each iteration but doesn't move local `main` for
     us.
   - If no branch name is specified, **ask the user** what branch to
     create or work from before proceeding.

## Phase 0.5: Read project mechanics

Before continuing, read `.claude/skills/project-mechanics/SKILL.md`.
It defines this repo's `focused_test` / `complete_test` /
`lint_touched` / `lint_all` / `typing` commands and any monorepo
selectors. If the file is missing, stop and ask the user how to run
tests/lint in this repo before proceeding — do not guess.

The remaining phases reference these command names verbatim. When you
see `<focused_test>`, substitute the value from the
`## Test commands` section. Same for the other four names.

**Foreground invariant (applies to every phase).** Every command
derived from project-mechanics — `<focused_test>`, `<lint_touched>`,
`<complete_test>`, `<lint_all>`, `<typing>` — runs in the
**foreground**, and you block on it until it finishes. Never launch one
with `run_in_background` and then wait via `Monitor` or end your turn.
Under `stoker run` the agent is a single-shot headless session, so
ending your turn terminates the run and any background work is killed
before it reports — leaving the working tree dirty and aborting the
next iteration. This holds even for a single test: a testcontainer- or
fixture-backed `<focused_test>` can be slow just to start, and the
harness may block a bare `sleep` and funnel you toward `Monitor` — do
not take that path. These commands may take several minutes; that wait
is expected. Block on them.

## Phase 1: Understand the task

- Read any referenced plan or PRD.
- Explore the codebase to understand the relevant files, patterns,
  and conventions.
- If the task is unclear, ask clarifying questions before proceeding.

## Phase 2: Plan the implementation (optional)

If the task has not already been fully planned, create a plan. Break
the work into a sequence of **tracer-bullet slices** — each slice is
the thinnest vertical cut that exercises one new behavior end-to-end
(e.g., route → service → database → response). Order slices so each
one builds on the last.

## Phase 3: Implement with red/green/refactor

Work through slices one at a time. For each slice, follow the TDD
cycle:

1. **Red** — Write ONE failing test that defines the next behavior.
   Run it to confirm it fails using `<focused_test>` scoped to the
   new test. Run `<focused_test>` in the **foreground** per the
   Phase 0.5 foreground invariant — never `run_in_background` + `Monitor`
   or end your turn waiting on it, even when one test is slow to start.
2. **Green** — Write the minimum production code to make that test
   pass. Run the test again to confirm it passes.
3. **Refactor** — Clean up the production code and/or test while
   keeping the test green. After each green→refactor cycle, run
   `<lint_touched>` over the files this slice modified.

Repeat this cycle for each behavior in the slice before moving to
the next slice.

**Do not batch multiple behaviors into one test. One test, one
behavior.**

If `## Monorepo selectors` is present in `project-mechanics` and your
slice is scoped to one workspace package, follow that section's
routing rules so the focused commands stay fast.

## Phase 4: Final validation

Run every command in this phase in the **foreground** and block on it
per the Phase 0.5 foreground invariant — `<lint_all>`, `<typing>`, and
`<complete_test>` are exactly the kind of multi-minute commands that
invariant covers, so never background one and wait via `Monitor` or end
your turn.

After all slices are complete, run the full suite to catch
regressions and reach a clean fixpoint. Stoker's `stoker-implement`
expects the working tree to be in a known good state when this phase
returns.

The discipline (in order):

1. **`git add -N`** any new untracked files so `lint_all` and
   `complete_test` see them.
2. **Run `<lint_all>`.** If it auto-fixes anything, the working tree
   is no longer at a fixpoint — go to step 4.
3. **Run `<typing>`.** Fix any errors before continuing.
4. **Run `<complete_test>`.** Plus any sub-package extras called out
   in the `## Final validation` section of `project-mechanics`. Fix
   any failures.
5. **Lint fixpoint loop.** While `git diff` shows changes (i.e.
   something earlier in this phase auto-fixed code), re-run
   `<lint_touched>` over every modified file until it exits 0 with
   no further changes.
6. **`git reset HEAD`** the intent-to-add files (the `git add -N`
   from step 1). This restores the original untracked-file state so
   `stoker-implement` can stage with intentionality.

A single `<lint_touched>` invocation exiting 0 with no working-tree
modifications is the success condition. Output like "Found N errors
(X fixed, Y remaining)" is a failure — fix the Y remaining manually.

If any step in this phase cannot be made green within this iteration
(e.g. pre-existing test failures, missing upstream dependency), STOP
and signal the failure to `stoker-implement` (your invoker). Do not
push broken state.

## No commits in stoker-work

`stoker-work` does not stage, commit, or push. Those steps live in
`stoker-implement`, which keeps the "fixpoint, then stage once with
intentionality" discipline clean.
