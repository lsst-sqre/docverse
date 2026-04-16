# Ralph iteration

Iteration __ITERATION__ of __TOTAL__ in this AFK run.

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

## Eligible task issues

The host filter (label=`prd-task`, state=open, Type=AFK, all blockers closed,
not labeled `agent-stuck`) produced this shortlist. Pick one per the priority
rubric in the `afk-iterate` skill and drive it to completion.

__SHORTLIST__

__PRD_CONTEXT__

__FORCED_ISSUE__

## Your job

Invoke the `afk-iterate` skill. After a normal iteration (success or stuck),
end your response without a sentinel — the host loop will continue on its
own. Only emit `<ralph-status>done</ralph-status>` as the last line of your
response when there is no further work to do (empty shortlist or no
actionable tasks remain), per Phase 8 of the skill.
