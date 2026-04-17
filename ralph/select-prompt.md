# Ralph select phase

Iteration __ITERATION__ of __TOTAL__ in this AFK run.

You are the **selection** phase of a two-phase Ralph iteration. Your only job
is to pick the single most appropriate task from the shortlist below and emit
a sentinel-wrapped JSON object describing the pick. **Do not** write any code,
run any git commands, invoke any skills, or take any other action. The
separate implementation phase will handle the actual work on whatever you
pick.

## Recent commits (last 10)

Use these as a hint about what logically comes next. Pay special attention to
any `Next-iteration notes:` sections — they're explicit guidance from the
previous iteration about state, migrations, or scaffolding to build on.

```
__GIT_LOG__
```

## Eligible task issues

Each entry is an open `prd-task` issue that the host has already filtered
(Type=AFK, all blockers closed, not labeled `agent-stuck`). The Metadata table
in each body carries `Parent PRD`, `Jira Key`, `Task Order`, `Type`,
`Blocked by`, `Parallel with`, and `Branch`.

__SHORTLIST__

__PRD_CONTEXT__

## Priority rubric

Rank the shortlist by:

1. Critical bug fixes (look for `bug`, `regression`, or severity language in
   the body).
2. Developer infrastructure (tooling, CI, local dev, test scaffolding).
3. Tracer-bullet feature slices that unblock future work.
4. Polish / quick wins (small, self-contained improvements).
5. Refactors with no user-visible effect.

Tiebreaker: ascending `Task Order` from the Metadata table.

`Parallel with` is informational and does **not** affect gating.

## Output format

After ranking, emit **exactly one** of these as the final line of your
response:

- If at least one entry is actionable, emit a `<ralph-pick>` sentinel with a
  JSON object carrying `issue_number`, `branch`, `jira_key`, and a short
  `reason`. Example:

  ```
  <ralph-pick>{"issue_number":57,"branch":"tickets/DM-12345-add-foo","jira_key":"DM-12345","reason":"Critical regression in build ingest; unblocks downstream tasks."}</ralph-pick>
  ```

  The `branch` and `jira_key` must match the Metadata table of the picked
  issue exactly. The `reason` is one sentence explaining the choice.

- If no entry is actionable (every remaining task is blocked by something the
  loop cannot resolve), emit only:

  ```
  <ralph-status>done</ralph-status>
  ```

You may briefly narrate your reasoning before the sentinel, but the sentinel
**must** be on the final line. Do not emit both sentinels. Do not emit any
other sentinel values.
