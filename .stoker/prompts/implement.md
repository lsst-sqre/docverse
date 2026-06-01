<!-- stoker-managed: prompts:.stoker/prompts/implement.md:449e136ea898d788 -->
# Stoker implement phase

Iteration __ITERATION__ of __TOTAL__ in this stoker run.

You are the **implementation** phase of a two-phase stoker iteration.
A prior selection phase has already picked the task below and the
host has already checked out the correct branch for you. Your job is
to drive this one task end-to-end via the `stoker-implement` skill.

## Picked task

- Issue: #__ISSUE_NUMBER__
- Branch: `__BRANCH__`

### Issue body

__ISSUE_BODY__

__PRD_BODY__

## Current repo state

- Branch: `__BRANCH__`
- `git status --porcelain`:

```
__GIT_STATUS__
```

## Recent commits (last 10)

```
__GIT_LOG__
```

## Your job

Invoke the `stoker-implement` skill. It takes the picked task through
`stoker-work`'s TDD cycle (Phases 1–4) and then owns commit → push → PR
→ close, or the stuck path on failure. The host owns the iteration
sentinel; do not emit any `<stoker-status>` markers yourself.
