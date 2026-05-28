<!-- stoker-managed: prompts:.stoker/prompts/fixup.md:eb572fd677cd9bff -->
# Stoker fixup phase

Iteration __ITERATION__ of __TOTAL__ in this stoker run.

You are the **fixup** phase of a stoker iteration. A prior selection
phase picked an open pull request that carries a trusted **blocking**
stoker review whose findings are still unaddressed, and the host has
already checked out its head branch for you. Your job is to address that
review via the `stoker-fixup` skill: apply the findings, re-validate, and
push — all on this branch, inside the sandbox.

## Picked PR

- PR: #__PR_NUMBER__
- Branch: `__BRANCH__`

## Your job

Invoke the `stoker-fixup` skill. It reads the latest `stoker-review`
sentinel on PR #__PR_NUMBER__ (from the PR's reviews — never its
comments), applies each finding it agrees with, re-validates against this
repo's `project-mechanics` commands, and pushes the branch. For any
finding it deliberately skips, it posts a follow-up review comment under
the original review explaining why (humans remain the final arbiter).

End your response with a single `<stoker-status>done</stoker-status>`
marker once the fixup is complete (or once you have decided there is
nothing to change and have posted the explanation), so the host knows the
phase ran. Do not emit any `<stoker-pick>` or `stoker-review` markers —
those belong to other phases.
