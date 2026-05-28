<!-- stoker-managed: prompts:.stoker/prompts/rebase.md:20d07e0890f3ed18 -->
# Stoker rebase phase

Iteration __ITERATION__ of __TOTAL__ in this stoker run.

You are the **rebase** phase of a stoker iteration. A prior selection
phase picked an open pull request whose branch is behind `main`, and the
host has already checked out its head branch and *attempted an automated
rebase onto `origin/main` that hit a conflict*. The automated attempt was
aborted, so the branch is back at its own tip — clean and checked out, not
mid-rebase. Your job is to redo that rebase via the `stoker-rebase` skill,
resolving the conflicts intelligently this time.

## Picked PR

- PR: #__PR_NUMBER__
- Branch: `__BRANCH__`

## Your job

Invoke the `stoker-rebase` skill. It rebases `__BRANCH__` onto a freshly
fetched `origin/main`, resolves each conflict by understanding both sides
(never by blindly taking one), re-validates against this repo's
`project-mechanics` commands (delegating to `stoker-work` when a conflict
resolution warrants the full red/green discipline), and leaves the branch
rebased, committed, and clean.

**Do not push.** Once the rebase is complete and validation is green, the
host verifies the branch is up to date with `main` and force-pushes it with
lease. If you cannot resolve the conflicts, abort the rebase
(`git rebase --abort`) so the working tree is clean and report what blocked
you — the host will mark the PR `agent-stuck` for a human.

End your response with a single `<stoker-status>done</stoker-status>`
marker once the rebase is complete (or once you have aborted and reported a
blocker), so the host knows the phase ran. Do not emit any `<stoker-pick>`
or `stoker-review` markers — those belong to other phases.
