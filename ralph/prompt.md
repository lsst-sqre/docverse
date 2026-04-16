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

Invoke the `afk-iterate` skill. When the loop's work is done (either this
iteration finishes successfully/stuck, or the shortlist is empty), emit the
sentinel `<ralph-status>done</ralph-status>` as the last thing you say.
