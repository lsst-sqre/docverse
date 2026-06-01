---
name: stoker-rebase
description: Rebase one stale PR branch onto main with intelligent conflict resolution — replay the branch onto a freshly-fetched origin/main, resolve each conflict by understanding both sides, re-validate against this repo's project-mechanics, and leave the branch rebased and clean for the host to force-push. Use when invoked from the stoker rebase-phase prompt, or when the user asks to rebase a PR branch onto main and resolve conflicts — and you are working in a stoker-installed repo. Consumes project-mechanics (Phase 0.5 prelude) and MAY delegate to stoker-work.
---
<!-- stoker-managed: skills:.agents/skills/stoker-rebase/SKILL.md:db027d5d75f9f087 -->

# stoker-rebase — rebase one PR branch onto main

You are the **rebase** primitive of the stoker AFK loop. A selection
phase picked an open PR whose branch is behind `main`, and the host
checked out the branch and tried an automated `git rebase origin/main`
that conflicted. That attempt was aborted, so the branch is back at its
own tip — clean and checked out, **not** mid-rebase. Your job is to redo
the rebase, this time resolving the conflicts intelligently, then
re-validate.

You may read the branch's diff and source freely; you never read issue or
PR **comments** — that is stoker's trust boundary.

## Phase 0: Branch safety

The host has already checked out the PR's head branch — confirm with
`git branch --show-current` and verify it is **not** `main`. If you are
somehow on `main`, stop and report the problem rather than rebasing.

## Phase 0.5: Read project mechanics

Before continuing, read `.claude/skills/project-mechanics/SKILL.md`. It
defines this repo's `focused_test` / `complete_test` / `lint_touched` /
`lint_all` / `typing` commands and any monorepo selectors. If the file is
missing, stop and ask the user how to run tests/lint in this repo before
proceeding — do not guess.

The phases below reference these command names verbatim. When you see
`<lint_all>`, substitute the value from the `## Lint` section, and so on.

## Phase 1: Start the rebase

Freshen the base and replay the branch onto it:

```
git fetch --force origin main
git rebase origin/main
```

If the rebase completes with no conflict, skip to Phase 3 (re-validate).
Otherwise git stops at the first conflicting commit — `git status` lists
the unmerged paths.

## Phase 2: Resolve conflicts intelligently

For each conflicting file, open it and understand **both** sides:

- The base side is `main`'s current content; the branch side is the
  commit being replayed. Read the surrounding code and the commit's intent
  (`git log`, `git show <commit>`) so you keep *both* changes' meaning —
  never resolve by blindly discarding one side.
- If a resolution is mechanical (an import list, adjacent edits to
  different functions), apply it in place.
- If a resolution warrants real reasoning — overlapping logic, a renamed
  symbol both sides touch, a behavior change — treat the merged result as
  new code and lean on the `stoker-work` skill's red/green discipline so
  the resolution lands with a test.

Once a file is resolved, `git add <file>`. When every conflict in the
current step is staged, continue:

```
git rebase --continue
```

Repeat Phase 2 for each conflicting step until the rebase finishes.

**If you cannot resolve the conflicts** — the two sides are genuinely
incompatible, or resolution would change behavior you can't verify — abort
so the working tree is left clean:

```
git rebase --abort
```

Then report what blocked you and stop (the host marks the PR
`agent-stuck`). Do **not** force a resolution you don't understand.

## Phase 3: Re-validate

After the rebase completes, reach a clean validation fixpoint against
`project-mechanics` — a clean textual rebase can still break the build when
`main` moved underneath the branch:

1. `git add -N` any new untracked files so the full-tree tools see them.
2. Run `<lint_all>`, then `<typing>`, then `<complete_test>`. Fix any
   failure before continuing (a fix is a normal commit on top of the
   rebased branch).
3. If anything auto-fixed code, fixpoint-loop on `<lint_touched>` over the
   touched files until it exits clean.
4. `git reset HEAD` the intent-to-add files so the tree is back to a clean,
   fully-committed state.

If validation cannot be made green, abort the rebase (`git rebase
--abort`) and report the blocker rather than leaving broken state.

## Phase 4: Hand back to the host — do not push

Leave the branch **rebased, committed, and clean**. Do **not** run `git
push` and do **not** force-push: the host verifies the branch is now up to
date with `main` and force-pushes it with lease. Pushing here would race
that step.

End your response with a single `<stoker-status>done</stoker-status>`
marker once the rebase is complete and validation is green (or once you
have aborted and reported a blocker), so the host knows the phase ran. Do
not emit any `<stoker-pick>` or `stoker-review` markers — those belong to
other phases.
