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

# Force exactly issue #57 (bypasses eligibility filters *and* the select phase)
ralph/afk.sh 1 --issue 57

# Preview the shortlist without making any Claude calls or GitHub mutations
ralph/afk.sh --dry-run
ralph/afk.sh --dry-run --prd 42
```

Flags:

| Flag | Purpose |
|---|---|
| `--prd N` | Only consider task issues whose Metadata `Parent PRD` field points at #N. |
| `--issue N` | Force this specific issue. Mutually exclusive with `--prd`. Skips the agent-stuck filter and the selection phase entirely — the host fabricates a pick from the issue's own metadata. |
| `--dry-run` | Build the shortlist and print it to stdout, then exit 0. Makes no Claude calls and no GitHub mutations. The iterations positional arg is optional with this flag (and ignored if passed, since the shortlist is a pure function of current GitHub state). Compatible with `--prd` and `--issue`. |
| `--api` | Use `ANTHROPIC_API_KEY` (from 1Password) instead of Max OAuth. |
| `--docker-context NAME` | Target a non-default docker context (e.g. `agent-sandbox`). |

Environment variables work identically to `run.sh`:
`DOCVERSE_SANDBOX_GH_TOKEN_OP`, `DOCVERSE_SANDBOX_SIGNING_KEY_PRIVATE_OP`,
`DOCVERSE_SANDBOX_SIGNING_KEY_PUBLIC_OP`, `DOCVERSE_SANDBOX_ANTHROPIC_KEY_OP`,
`DOCVERSE_SANDBOX_DOCKER_CONTEXT`, `DOCVERSE_SANDBOX_OP_ACCOUNT`.

Per-phase tuning:

- `DOCVERSE_RALPH_SELECT_MODEL` — model id for the cheap selection phase
  (default `claude-haiku-4-5`; empty falls back to the container's configured
  default).
- `DOCVERSE_RALPH_SELECT_EFFORT` — Claude Code `--effort` level for the
  selection phase (`low | medium | high | xhigh | max`; empty defers to
  Claude's default).
- `DOCVERSE_RALPH_IMPLEMENT_MODEL` — model id for the implementation phase
  (empty defers to the container's configured default). Set to e.g. `opus`
  when the backlog is gnarly.
- `DOCVERSE_RALPH_IMPLEMENT_EFFORT` — Claude Code `--effort` level for the
  implementation phase (`low | medium | high | xhigh | max`; empty defers to
  Claude's default). Bumping to `xhigh` / `max` may warrant raising
  `DOCVERSE_RALPH_ITER_TIMEOUT` beyond its 3600s default.
- `DOCVERSE_RALPH_SELECT_TIMEOUT` — seconds for the selection phase (default
  `120`).
- `DOCVERSE_RALPH_ITER_TIMEOUT` — seconds for the implementation phase
  (default `3600`).

## What one iteration does

Each iteration is split into **two** host-orchestrated Claude phases so the
rubric, shortlist, and branch-resolution glue stay out of the
implementation-phase context window:

```
┌─ host ────────────────────────────────────────────────────────────────┐
│ build shortlist → JSON index + markdown (iter-NN.shortlist.json)      │
└───────┬───────────────────────────────────────────────────────────────┘
        │                         empty shortlist (and no --issue)
        ├──────────────────────────▶ break loop, no Claude calls
        │
        ▼
┌─ claude: SELECT phase (Haiku) ─────────────────────────────────────────┐
│  sees: shortlist markdown, rubric, recent commits                       │
│  emits: <ralph-pick>{…json…}</ralph-pick>  or  <ralph-status>done</…>   │
└───────┬────────────────────────────────────────────────────────────────┘
        │ (--issue N skips this phase; pick is fabricated from the JSON)
        ▼
┌─ host ────────────────────────────────────────────────────────────────┐
│ parse pick, validate against shortlist JSON                            │
│ fetch parent PRD body (cached across iterations)                       │
│ container-side branch setup via `container_exec git …`:                │
│   fetch --prune origin <branch>  →  checkout / checkout -b             │
│ on all-three-checkout failure: host-side                               │
│   gh issue edit --add-label agent-stuck + comment                      │
└───────┬───────────────────────────────────────────────────────────────┘
        ▼
┌─ claude: IMPLEMENT phase (default model) ──────────────────────────────┐
│  sees: picked issue body + parent PRD body + git state                  │
│  does: work-it phases 1–4 (TDD) → commit → push → PR → close           │
│        or the stuck path (commit WIP, push, label agent-stuck)         │
└────────────────────────────────────────────────────────────────────────┘
```

Success path (implementation phase): red/green/refactor via `work-it`
(phases 1–4) → signed commit with the AFK template → push → PR via
`rubin-create-pr` (or append `Closes #N` to an existing single-branch PR) →
comment & close the task issue.

Stuck path inside the implementation phase (validation can't go green, push
fails, 3× pre-commit failures): commit `WIP(stuck) …`, push, comment, add the
`agent-stuck` label. The issue stays open; a human clears the label to let
Ralph try again in a later run.

Stuck path inside the host (container-side branch-setup failure, or picked
issue missing its Branch metadata): host runs `gh issue edit --add-label
agent-stuck` and posts a comment with the git error text; outer loop
continues.

Successful iterations close the task issue immediately after opening the PR
(rather than waiting for merge) so the next iteration's host filter sees
dependent tasks become eligible. If a PR is later rejected, the closed issue
must be manually reopened — this tradeoff is accepted.

## Eligibility

A task is eligible when all of these are true:

- Labeled `prd-task`, state `open`.
- Metadata `Type` is `AFK`.
- Every issue number in the Metadata `Blocked by` row is resolved: open PRs
  still block, and a closed issue is only resolved once its closing PR is
  merged (or if it has no closing PR).
- Not labeled `agent-stuck`.
- If `--prd N` is passed, Metadata `Parent PRD` references `#N`.

The host script applies these mechanically (label + text filters + per-blocker
state lookups). The semantic ranking — critical bugs → infra → tracer bullets
→ polish → refactors, tiebroken by ascending `Task Order` — happens inside the
selection phase's prompt.

## Hard-abort conditions

The loop stops immediately (non-zero exit) when:

- The workspace tree is dirty at the start of an iteration.
- `devcontainer up` fails.
- `git fetch origin` fails at loop start.
- Any Claude invocation (select or implement) exits non-zero (including
  timeout).
- `--issue N` references an issue that isn't in the filtered shortlist index.

Soft failures route through the stuck path instead:

- Container-side branch checkout failing all three fallbacks → host-side
  stuck label + comment, outer loop continues.
- Malformed select-phase output after one retry → iteration skipped, outer
  loop continues.
- Any TDD / commit / push / PR failure inside the implementation phase →
  `afk-implement` skill self-marks `agent-stuck`.

## How the pieces fit together

```
┌────────────────────────────┐
│ ralph/afk.sh (host)        │  arg parsing, secret loading, container up,
│                            │  fetch, shortlist JSON+markdown, select-phase
│                            │  Claude call, pick validation, parent-PRD
│                            │  fetch (cached), container-side branch setup,
│                            │  implement-phase Claude call, log capture,
│                            │  sentinel parsing, summary.md
└─────────────┬──────────────┘
              │ sources
              ▼
┌────────────────────────────┐
│ .devcontainer/lib/         │  load_sandbox_secrets + load_host_git_identity
│ secrets.sh                 │  shared with .devcontainer/run.sh
└────────────────────────────┘

┌────────────────────────────┐
│ ralph/select-prompt.md     │  select-phase template — shortlist markdown,
│                            │  priority rubric, recent commits, expected
│                            │  <ralph-pick>{…}</ralph-pick> output
└─────────────┬──────────────┘
              │ piped on stdin to claude -p -  (Haiku)
              ▼
┌────────────────────────────┐
│ claude (select phase)      │  ranks shortlist, emits pick JSON sentinel
│                            │  (or <ralph-status>done</ralph-status>).
└────────────────────────────┘

┌────────────────────────────┐
│ ralph/implement-prompt.md  │  implement-phase template — picked issue body,
│                            │  parent PRD body, git state. No shortlist,
│                            │  no rubric.
└─────────────┬──────────────┘
              │ piped on stdin to claude -p -  (default model)
              ▼
┌────────────────────────────┐
│ claude (implement phase)   │  runs .claude/skills/afk-implement/SKILL.md
│                            │  which delegates TDD phases to
│                            │  .claude/skills/work-it/SKILL.md, then uses
│                            │  .claude/skills/rubin-create-pr for PR
│                            │  creation.
└────────────────────────────┘
```

Each file's responsibility:

- **`ralph/afk.sh`** — the only piece that runs on the host. Owns: CLI,
  secrets, container lifecycle, eligibility filtering (label + AFK +
  blockers), shortlist JSON+markdown generation, select-phase orchestration,
  pick parsing/validation, parent-PRD fetch + cache, container-side branch
  setup, host-side stuck labeling when branch setup fails, implement-phase
  orchestration, per-phase streaming log capture, sentinel detection,
  `summary.md`.

- **`.devcontainer/lib/secrets.sh`** — sourced by `afk.sh` and `run.sh`.
  `load_sandbox_secrets` reads GH token + signing keys (+ optional Anthropic
  key) from 1Password. `load_host_git_identity` reads `user.name` /
  `user.email` from the host's global git config.

- **`ralph/select-prompt.md`** — selection-phase prompt. Carries the
  shortlist (rendered from the JSON index), the priority rubric, optional
  parent-PRD context (when `--prd N` is set), and the last 10 commits.
  Instructs the model to emit `<ralph-pick>{…}</ralph-pick>` or
  `<ralph-status>done</ralph-status>`.

- **`ralph/implement-prompt.md`** — implementation-phase prompt. Carries only
  the picked task's metadata (issue number, branch, Jira key) and content
  (issue body, parent PRD body) plus current git state. Never carries the
  shortlist, rubric, or non-picked issues. Instructs the model to invoke the
  `afk-implement` skill.

- **`.claude/skills/afk-implement/SKILL.md`** — the implementation-phase
  orchestrator running inside Claude. Takes the already-checked-out branch
  and the picked task through `work-it` phases 1–4, then commits via the
  AFK template, pushes, opens/updates a PR via `rubin-create-pr`, comments
  and closes (success) or WIP-commits + labels `agent-stuck` (stuck).

- **`.claude/skills/work-it/SKILL.md`** — unchanged by Ralph. Stays the
  single source of truth for TDD; `afk-implement` cites its phase numbers
  directly so drift is caught in review.

- **`.claude/skills/rubin-create-pr/SKILL.md`** — unchanged. Called by
  `afk-implement` for PR creation so the PR body / Jira references stay
  consistent with the interactive flow.

## Logs

Every `afk.sh` invocation creates `ralph/logs/<run-id>/` (run-id is
`YYYYMMDD-HHMMSS`). Each iteration writes up to the following files:

| File | Contents |
|---|---|
| `iter-NN.shortlist.json` | Host-built JSON index keyed by issue number. |
| `iter-NN-select.prompt.md` | Exact select-phase prompt piped to Claude. |
| `iter-NN-select.jsonl` | Raw `stream-json` events from the select phase. |
| `iter-NN-select.log` | Human-readable select-phase transcript. |
| `iter-NN-implement.prompt.md` | Exact implement-phase prompt piped to Claude. |
| `iter-NN-implement.jsonl` | Raw `stream-json` events from the implement phase. |
| `iter-NN-implement.log` | Human-readable implement-phase transcript. |

On a forced `--issue N` iteration, the `iter-NN-select.*` files are absent —
the host fabricates the pick directly from the shortlist JSON without calling
Claude for selection. On a malformed-select retry, an extra
`iter-NN-retry-select.*` triple is written.

At the end of the run, `summary.md` records iteration count, whether the
sentinel was reached, scope flags, per-iteration picks with reasons,
per-phase wall-clock, any `agent-stuck` issues observed (both host-marked
for branch-setup failures and in-container marked by `afk-implement`), and
the abort reason (if any). `ralph/logs/` is gitignored.

## Completion sentinel

The outer loop exits cleanly in two cases:

1. The host-built shortlist is empty (no eligible tasks) and `--issue` is
   not set. No Claude calls are made — the loop breaks immediately.
2. The selection phase emits `<ralph-status>done</ralph-status>` because it
   inspected a non-empty shortlist and concluded nothing is actionable in
   this run.

Missing a sentinel after a successful iteration is not itself an error — the
loop just continues to the next iteration until the requested iteration count
is reached.
