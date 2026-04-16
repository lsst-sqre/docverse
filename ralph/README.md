# Ralph AFK loop

Ralph is a driver that repeatedly spawns Claude Code inside the docverse
[sandbox](../.devcontainer/README.md) to work through a backlog of `prd-task`
GitHub issues without operator supervision. The name and design are inspired by
the "Ralph Wiggum" loop pattern — each iteration starts with a fresh Claude
session and rediscovers state through `git log`, open issues, and the current
branch.

## Usage

Set the same 1Password / git-identity env vars the sandbox needs (see
[`.devcontainer/README.md`](../.devcontainer/README.md)), then:

```bash
# Run up to 10 iterations against all eligible open AFK tasks
ralph/afk.sh 10

# Scope to the children of PRD issue #42
ralph/afk.sh 10 --prd 42

# Force exactly issue #57 (bypasses eligibility filters, including agent-stuck)
ralph/afk.sh 1 --issue 57
```

Flags:

| Flag | Purpose |
|---|---|
| `--prd N` | Only consider task issues whose Metadata `Parent PRD` field points at #N. |
| `--issue N` | Force this specific issue. Mutually exclusive with `--prd`. Skips the agent-stuck filter so you can retry a stuck task deliberately. |
| `--api` | Use `ANTHROPIC_API_KEY` (from 1Password) instead of Max OAuth. |
| `--docker-context NAME` | Target a non-default docker context (e.g. `agent-sandbox`). |

Environment variables work identically to `run.sh`:
`DOCVERSE_SANDBOX_GH_TOKEN_OP`, `DOCVERSE_SANDBOX_SIGNING_KEY_PRIVATE_OP`,
`DOCVERSE_SANDBOX_SIGNING_KEY_PUBLIC_OP`, `DOCVERSE_SANDBOX_ANTHROPIC_KEY_OP`,
`DOCVERSE_SANDBOX_DOCKER_CONTEXT`, `DOCVERSE_SANDBOX_OP_ACCOUNT`.

Override the per-iteration timeout (default 3600s) with
`DOCVERSE_RALPH_ITER_TIMEOUT=<seconds>`.

## What one iteration does

One iteration = one issue driven to completion. Success path:

1. Pick the highest-priority eligible task from the host-prepared shortlist.
2. Check out the branch named in the task's Metadata table.
3. Red/green/refactor through `work-it` (Phases 1–4).
4. Commit with the structured template (see
   [`.claude/skills/afk-iterate/SKILL.md`](../.claude/skills/afk-iterate/SKILL.md)).
5. Push the branch.
6. Open a PR via `rubin-create-pr` (or append `Closes #N` to the existing PR
   body for single-branch PRDs).
7. Comment on the task issue and close it.

Stuck path (validation can't go green, push fails, 3× pre-commit failures, bad
metadata): commit `WIP(stuck) ...`, push, comment on the issue, add the
`agent-stuck` label. The issue stays open; a human clears the label to let
Ralph try again in a later run.

## Eligibility

A task is eligible when all of these are true:

- Labeled `prd-task`, state `open`.
- Metadata `Type` is `AFK`.
- Every issue number in the Metadata `Blocked by` row is `CLOSED`.
- Not labeled `agent-stuck`.
- If `--prd N` is passed, Metadata `Parent PRD` references `#N`.

The host script applies these mechanically (label + text filters + per-blocker
state lookups). The semantic ranking — critical bugs → infra → tracer bullets
→ polish → refactors, tiebroken by ascending `Task Order` — happens inside the
`afk-iterate` skill.

## Hard-abort conditions

The loop stops immediately (non-zero exit) when:

- The workspace tree is dirty at the start of an iteration.
- `devcontainer up` fails.
- `git fetch origin` fails at loop start.
- Claude exits non-zero (including timeout).

Soft failures inside an iteration (test/lint/typing/push) route through the
stuck path instead.

## How the pieces fit together

```
┌────────────────────────────┐
│ ralph/afk.sh (host)        │  arg parsing, secret loading, container up,
│                            │  fetch, per-iter context pre-fetch, prompt
│                            │  render, devcontainer exec, log capture,
│                            │  sentinel parsing, summary.md
└─────────────┬──────────────┘
              │ sources
              ▼
┌────────────────────────────┐
│ .devcontainer/lib/         │  load_sandbox_secrets + load_host_git_identity
│ secrets.sh                 │  shared with .devcontainer/run.sh
└────────────────────────────┘

┌────────────────────────────┐
│ ralph/prompt.md            │  thin template with __ITERATION__,
│                            │  __BRANCH__, __GIT_LOG__, __SHORTLIST__,
│                            │  __PRD_CONTEXT__, __FORCED_ISSUE__ placeholders
└─────────────┬──────────────┘
              │ piped on stdin to claude -p -
              ▼
┌────────────────────────────┐
│ claude (inside container)  │  runs .claude/skills/afk-iterate/SKILL.md
│                            │  which delegates TDD phases to
│                            │  .claude/skills/work-it/SKILL.md, then
│                            │  uses .claude/skills/rubin-create-pr for
│                            │  PR creation. Emits
│                            │  <ralph-status>done</ralph-status> when done.
└────────────────────────────┘
```

Each file's responsibility:

- **`ralph/afk.sh`** — the only piece that runs on the host. Owns: CLI, secrets,
  container lifecycle, eligibility filtering (label + AFK + blockers), context
  prefetch (last 10 commits, branch, `git status`, optional parent PRD body),
  prompt rendering, streaming log capture (`iter-NN.jsonl` raw + `iter-NN.log`
  jq-rendered), sentinel detection, `summary.md`.

- **`.devcontainer/lib/secrets.sh`** — sourced by `afk.sh` and `run.sh`.
  `load_sandbox_secrets` reads GH token + signing keys (+ optional Anthropic
  key) from 1Password. `load_host_git_identity` reads `user.name` /
  `user.email` from the host's global git config.

- **`ralph/prompt.md`** — substituted once per iteration. Injects recent commits
  and the eligible-task shortlist, then tells Claude to invoke the
  `afk-iterate` skill and emit the completion sentinel.

- **`.claude/skills/afk-iterate/SKILL.md`** — the iteration orchestrator
  running inside Claude. Picks a task from the shortlist, checks out its
  branch, delegates Phases 1–4 of `work-it` for TDD, commits using the
  success/stuck templates, pushes, opens/updates a PR via `rubin-create-pr`,
  comments and closes (success) or labels `agent-stuck` (stuck). Emits
  `<ralph-status>done</ralph-status>` when the work is done or the shortlist
  is empty.

- **`.claude/skills/work-it/SKILL.md`** — unchanged by Ralph. Stays the single
  source of truth for TDD; `afk-iterate` cites its phase numbers directly so
  drift is caught in review.

- **`.claude/skills/rubin-create-pr/SKILL.md`** — unchanged. Called by
  `afk-iterate` for PR creation so the PR body / Jira references stay
  consistent with the interactive flow.

## Logs

Every `afk.sh` invocation creates `ralph/logs/<run-id>/` (run-id is
`YYYYMMDD-HHMMSS`). Each iteration writes three files:

| File | Contents |
|---|---|
| `iter-NN.prompt.md` | Exact prompt piped to Claude (for replay/debugging). |
| `iter-NN.jsonl` | Raw `stream-json` events — the source of truth. |
| `iter-NN.log` | Human-readable transcript (assistant text + tool-use markers). |

At the end of the run, `summary.md` records iteration count, whether the
sentinel was reached, scope flags, any `agent-stuck` issues observed, and the
abort reason (if any). `ralph/logs/` is gitignored.

## Completion sentinel

The loop exits after any iteration whose final `result` event contains the
literal string `<ralph-status>done</ralph-status>`. This happens when the
shortlist becomes empty (no further eligible tasks) or when `afk-iterate`
decides there's no more work to do. Missing the sentinel after a successful
iteration is not itself an error — the loop just continues to the next
iteration until the requested iteration count is reached.
