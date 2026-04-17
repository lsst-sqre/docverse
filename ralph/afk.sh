#!/usr/bin/env bash
# Ralph AFK loop driver for docverse.
#
# Repeatedly spawns Claude Code inside the docverse sandbox to work through the
# backlog of open `prd-task` GitHub issues created by `rubin-prd-to-issues`.
# Each iteration: picks one eligible task → drives it through red/green/refactor
# via work-it → commits, pushes, opens/updates a PR, closes the issue (or marks
# it `agent-stuck` on failure).
#
# Usage:
#   ralph/afk.sh <iterations>               # all open AFK prd-tasks
#   ralph/afk.sh <iterations> --prd 42      # only children of PRD #42
#   ralph/afk.sh <iterations> --issue 57    # force task #57 (bypasses filters)
#
# Required env vars (same as .devcontainer/run.sh):
#   DOCVERSE_SANDBOX_GH_TOKEN_OP
#   DOCVERSE_SANDBOX_SIGNING_KEY_PRIVATE_OP
#   DOCVERSE_SANDBOX_SIGNING_KEY_PUBLIC_OP
#
# Optional:
#   DOCVERSE_SANDBOX_ANTHROPIC_KEY_OP         (with --api)
#   DOCVERSE_SANDBOX_DOCKER_CONTEXT           (e.g. agent-sandbox)
#   DOCVERSE_SANDBOX_OP_ACCOUNT               (default: my.1password.com)
#
# Requires bash 4+ (uses `mapfile` and modern array features).
# macOS ships bash 3.2 — install a newer bash via Homebrew (`brew install bash`)
# or run this script in the devcontainer.
if [ "${BASH_VERSINFO[0]:-0}" -lt 4 ]; then
    echo "ralph/afk.sh requires bash 4+ (you have ${BASH_VERSION:-unknown})." >&2
    echo "On macOS: brew install bash, then re-run with /opt/homebrew/bin/bash." >&2
    exit 1
fi
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

EX_INFRA=64
EX_ABORT=65

die() { echo "error: $*" >&2; exit "$EX_INFRA"; }
abort() { echo "abort: $*" >&2; exit "$EX_ABORT"; }

# ---- Defaults ----
iterations=""
prd_filter=""
forced_issue=""
use_api=false
gh_token_uri="${DOCVERSE_SANDBOX_GH_TOKEN_OP:-}"
signing_key_private_uri="${DOCVERSE_SANDBOX_SIGNING_KEY_PRIVATE_OP:-}"
signing_key_public_uri="${DOCVERSE_SANDBOX_SIGNING_KEY_PUBLIC_OP:-}"
anthropic_key_uri="${DOCVERSE_SANDBOX_ANTHROPIC_KEY_OP:-}"
docker_context="${DOCVERSE_SANDBOX_DOCKER_CONTEXT:-}"
op_account="${DOCVERSE_SANDBOX_OP_ACCOUNT:-my.1password.com}"

# Container exec timeout per iteration (seconds). 3600 = 1 hour.
iter_timeout="${DOCVERSE_RALPH_ITER_TIMEOUT:-3600}"

# ---- Flag parsing ----
while [[ $# -gt 0 ]]; do
    case "$1" in
        --prd)            prd_filter="$2"; shift 2 ;;
        --issue)          forced_issue="$2"; shift 2 ;;
        --api)            use_api=true; shift ;;
        --docker-context) docker_context="$2"; shift 2 ;;
        --op-account)     op_account="$2"; shift 2 ;;
        -*)               die "Unknown flag: $1" ;;
        *)
            if [ -z "$iterations" ]; then
                iterations="$1"
            else
                die "Unexpected positional arg: $1"
            fi
            shift ;;
    esac
done

[ -n "$iterations" ] || die "Usage: $0 <iterations> [--prd N | --issue N]"
[[ "$iterations" =~ ^[0-9]+$ ]] || die "iterations must be a positive integer"
[ "$iterations" -gt 0 ] || die "iterations must be > 0"
if [ -n "$prd_filter" ] && [ -n "$forced_issue" ]; then
    die "--prd and --issue are mutually exclusive"
fi

# ---- Validate required URIs ----
[ -n "$gh_token_uri" ] || die "DOCVERSE_SANDBOX_GH_TOKEN_OP required"
[ -n "$signing_key_private_uri" ] || die "DOCVERSE_SANDBOX_SIGNING_KEY_PRIVATE_OP required"
[ -n "$signing_key_public_uri" ] || die "DOCVERSE_SANDBOX_SIGNING_KEY_PUBLIC_OP required"
if $use_api; then
    [ -n "$anthropic_key_uri" ] || die "DOCVERSE_SANDBOX_ANTHROPIC_KEY_OP required with --api"
fi

# ---- Load secrets + git identity (shared with run.sh) ----
# shellcheck source=../.devcontainer/lib/secrets.sh
source "$REPO_DIR/.devcontainer/lib/secrets.sh"
load_sandbox_secrets
load_host_git_identity

# ---- Docker context ----
if [ -n "$docker_context" ]; then
    export DOCKER_CONTEXT="$docker_context"
fi

# ---- Bring container up (idempotent) ----
echo "Ensuring devcontainer is up..."
devcontainer up --workspace-folder "$REPO_DIR" >/dev/null \
    || die "devcontainer up failed"

# ---- Helper: exec a command inside the container with all env wired in ----
# Usage: container_exec [--timeout SECS] <command> [args...]
container_exec() {
    local to=""
    if [ "${1:-}" = "--timeout" ]; then
        to="$2"; shift 2
    fi
    local -a args=(--workspace-folder "$REPO_DIR")
    args+=(--remote-env "GH_TOKEN=$GH_TOKEN")
    args+=(--remote-env "DOCVERSE_SIGNING_KEY_PEM=$SIGNING_KEY_PEM")
    args+=(--remote-env "DOCVERSE_SIGNING_KEY_PUB=$SIGNING_KEY_PUB")
    args+=(--remote-env "DOCVERSE_SANDBOX_GIT_USER_NAME=$GIT_USER_NAME")
    args+=(--remote-env "DOCVERSE_SANDBOX_GIT_USER_EMAIL=$GIT_USER_EMAIL")
    if $use_api; then
        args+=(--remote-env "ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY")
    fi
    if [ -n "$to" ]; then
        devcontainer exec "${args[@]}" timeout "$to" "$@"
    else
        devcontainer exec "${args[@]}" "$@"
    fi
}

# ---- Loop-start fetch so we see other humans' merges ----
echo "Fetching origin inside container..."
container_exec git -C /workspace/docverse fetch --prune origin \
    || die "git fetch origin failed"

# ---- Logging setup ----
run_id="$(date +%Y%m%d-%H%M%S)"
log_dir="$SCRIPT_DIR/logs/$run_id"
mkdir -p "$log_dir"
echo "Logging to $log_dir"

# jq filter: render stream-json events as human-readable text for the terminal
# log. Preserves assistant text with CRLF-normalized newlines and flags tool
# use. Raw stream is separately tee'd to iter-NN.jsonl for replay.
stream_filter='
  if .type == "assistant" then
    (.message.content // [])[]? |
      if .type == "text" then
        (.text // "" | gsub("\n"; "\r\n"))
      elif .type == "tool_use" then
        "[tool] \(.name) \((.input // {}) | tostring | .[0:200])"
      else empty end
  elif .type == "tool_result" then
    empty
  elif .type == "result" then
    "\n--- result ---\n\(.result // "")"
  elif .type == "system" and .subtype == "init" then
    "[session \(.session_id // "?")]"
  else empty end
'

# ---- Helper: fetch the shortlist of eligible task issues ----
# Writes markdown blocks to stdout, one per eligible issue.
build_shortlist() {
    local prd="$1"
    local forced="$2"

    if [ -n "$forced" ]; then
        # --issue N: fetch that one issue, skip all filters
        container_exec gh issue view "$forced" \
            --repo lsst-sqre/docverse \
            --json number,title,body \
            --template '--- ISSUE #{{.number}} ---
{{.title}}

{{.body}}

'
        return
    fi

    # List open prd-task issues, drop those labeled agent-stuck, then filter
    # by Type=AFK and (optional) Parent PRD via body-text matching.
    local raw
    raw=$(container_exec gh issue list \
        --repo lsst-sqre/docverse \
        --label prd-task --state open --limit 100 \
        --json number,title,body,labels) \
        || die "gh issue list failed"

    # First pass: apply label + Type + optional Parent PRD filters in jq.
    local candidates
    candidates=$(echo "$raw" | jq -c --arg prd "$prd" '
        .[]
        | select(any(.labels[]; .name == "agent-stuck") | not)
        | select(.body | test("\\|\\s*Type\\s*\\|\\s*AFK\\s*\\|"; "i"))
        | select(
            ($prd == "") or
            (.body | test("\\|\\s*Parent PRD\\s*\\|\\s*#" + $prd + "\\b"))
          )
    ') || die "jq filter failed"

    [ -z "$candidates" ] && return 0

    # Second pass: for each candidate, check that every `Blocked by: #N` is
    # closed. If any blocker is open, drop the candidate.
    echo "$candidates" | while IFS= read -r issue_json; do
        [ -z "$issue_json" ] && continue
        local num body blockers ok=1
        num=$(echo "$issue_json" | jq -r .number)
        body=$(echo "$issue_json" | jq -r .body)
        # Pull numbers from the line starting with `Blocked by:` or the table
        # row `| Blocked by | #1, #2 |`.
        blockers=$(printf '%s\n' "$body" \
            | grep -iE '^\s*\|?\s*Blocked by' \
            | head -n 1 \
            | grep -oE '#[0-9]+' \
            | tr -d '#' || true)
        for b in $blockers; do
            local state
            state=$(container_exec gh issue view "$b" \
                --repo lsst-sqre/docverse \
                --json state --jq .state 2>/dev/null || echo "OPEN")
            if [ "$state" != "CLOSED" ]; then
                ok=0; break
            fi
        done
        if [ "$ok" = "1" ]; then
            local title
            title=$(echo "$issue_json" | jq -r .title)
            printf -- '--- ISSUE #%s ---\n%s\n\n%s\n\n' "$num" "$title" "$body"
        fi
    done
}

# ---- Helper: pre-fetch per-iteration context ----
prefetch_context() {
    local branch git_status git_log shortlist prd_context forced_marker

    branch=$(container_exec git -C /workspace/docverse branch --show-current \
        | tr -d '\r')
    git_status=$(container_exec git -C /workspace/docverse status --porcelain \
        | tr -d '\r' || true)
    git_log=$(container_exec git -C /workspace/docverse log -n 10 \
        --format='%H%n%ad%n%B---' --date=short \
        | tr -d '\r' || true)

    shortlist=$(build_shortlist "$prd_filter" "$forced_issue" || true)

    prd_context=""
    if [ -n "$prd_filter" ]; then
        local prd_body
        prd_body=$(container_exec gh issue view "$prd_filter" \
            --repo lsst-sqre/docverse \
            --json title,body \
            --template '## Parent PRD context (#'"$prd_filter"': {{.title}})

{{.body}}' 2>/dev/null || true)
        prd_context="$prd_body"
    fi

    forced_marker=""
    if [ -n "$forced_issue" ]; then
        forced_marker="## Forced issue
The operator invoked \`afk.sh --issue ${forced_issue}\`. Pick that issue and
skip the ranking rubric."
    fi

    # Export globals for render_prompt
    PF_BRANCH="$branch"
    PF_GIT_STATUS="$git_status"
    PF_GIT_LOG="$git_log"
    PF_SHORTLIST="$shortlist"
    PF_PRD_CONTEXT="$prd_context"
    PF_FORCED_MARKER="$forced_marker"
}

# ---- Helper: render prompt.md with placeholders substituted ----
# stdin: template
# stdout: rendered prompt
render_prompt() {
    local iter="$1" total="$2"
    local shortlist_text="$PF_SHORTLIST"
    [ -z "$shortlist_text" ] && shortlist_text="(shortlist is empty — no eligible tasks)"
    local git_status_text="$PF_GIT_STATUS"
    [ -z "$git_status_text" ] && git_status_text="(clean)"

    # Use python3 with env-var inputs: literal str.replace sidesteps awk's
    # gsub replacement semantics (where `&` means the matched text) and
    # `awk -v`'s backslash-escape processing on values.
    PF_ITERATION="$iter" \
    PF_TOTAL="$total" \
    PF_GIT_STATUS_R="$git_status_text" \
    PF_SHORTLIST_R="$shortlist_text" \
    python3 - "$SCRIPT_DIR/prompt.md" <<'PYEOF'
import os, re, sys
mapping = {
    "__ITERATION__":    os.environ["PF_ITERATION"],
    "__TOTAL__":        os.environ["PF_TOTAL"],
    "__BRANCH__":       os.environ.get("PF_BRANCH", ""),
    "__GIT_STATUS__":   os.environ["PF_GIT_STATUS_R"],
    "__GIT_LOG__":      os.environ.get("PF_GIT_LOG", ""),
    "__SHORTLIST__":    os.environ["PF_SHORTLIST_R"],
    "__PRD_CONTEXT__":  os.environ.get("PF_PRD_CONTEXT", ""),
    "__FORCED_ISSUE__": os.environ.get("PF_FORCED_MARKER", ""),
}
with open(sys.argv[1], encoding="utf-8") as f:
    text = f.read()
for k, v in mapping.items():
    if v == "":
        # Remove the whole line (and trailing newline) when the value is empty,
        # so we don't emit blank stanzas for unused placeholders.
        text = re.sub(rf'^[^\n]*{re.escape(k)}[^\n]*\n?', '', text, flags=re.MULTILINE)
    else:
        text = text.replace(k, v)
sys.stdout.write(text)
PYEOF
}

# ---- Summary accumulators ----
stuck_issues=()
abort_reason=""
sentinel_seen=false
completed_iterations=0

write_summary() {
    {
        echo "# Ralph AFK run $run_id"
        echo
        echo "- Iterations requested: $iterations"
        echo "- Iterations completed: $completed_iterations"
        echo "- Sentinel reached (loop exited cleanly): $sentinel_seen"
        if [ -n "$prd_filter" ]; then echo "- Scope: --prd $prd_filter"; fi
        if [ -n "$forced_issue" ]; then echo "- Scope: --issue $forced_issue"; fi
        echo
        if [ ${#stuck_issues[@]} -gt 0 ]; then
            echo "## Stuck issues (claimed during this run)"
            for i in "${stuck_issues[@]}"; do echo "- $i"; done
        fi
        if [ -n "$abort_reason" ]; then
            echo
            echo "## Abort"
            echo "$abort_reason"
        fi
    } > "$log_dir/summary.md"
}
trap write_summary EXIT

# ---- Main loop ----
for ((i=1; i<=iterations; i++)); do
    iter_pad=$(printf '%02d' "$i")
    echo
    echo "================================================================"
    echo "Iteration $i of $iterations"
    echo "================================================================"

    # --- Pre-flight: dirty tree is an abort condition ---
    dirty=$(container_exec git -C /workspace/docverse status --porcelain | tr -d '\r' || true)
    if [ -n "$dirty" ]; then
        abort_reason="dirty tree at start of iter $i: $dirty"
        abort "$abort_reason"
    fi

    # --- Refresh remote state so we see other humans' merges mid-run ---
    container_exec git -C /workspace/docverse fetch --prune origin \
        || die "git fetch origin failed on iter $i"

    # --- Build context + shortlist ---
    prefetch_context

    # Short-circuit: empty shortlist + no forced issue → nothing to do.
    if [ -z "$PF_SHORTLIST" ] && [ -z "$forced_issue" ]; then
        echo "No eligible tasks remain; emitting sentinel and stopping."
        sentinel_seen=true
        break
    fi

    # --- Render prompt to a log-dir file so we can replay it ---
    prompt_file="$log_dir/iter-${iter_pad}.prompt.md"
    render_prompt "$i" "$iterations" > "$prompt_file"

    jsonl_file="$log_dir/iter-${iter_pad}.jsonl"
    log_file="$log_dir/iter-${iter_pad}.log"

    # --- Run claude via devcontainer exec, streaming stream-json through tee
    # (raw → jsonl) and through jq (pretty → terminal + .log). Exit status
    # comes from the devcontainer exec in the pipeline.
    echo "Running claude (timeout ${iter_timeout}s)..."
    set +e
    cat "$prompt_file" | container_exec --timeout "$iter_timeout" \
        /usr/local/bin/agent-entry \
        claude --dangerously-skip-permissions --verbose \
               --output-format stream-json -p - \
        | tee "$jsonl_file" \
        | jq -r --unbuffered "$stream_filter" 2>/dev/null \
        | tee "$log_file"
    rc=${PIPESTATUS[1]}
    set -e

    if [ "$rc" -ne 0 ]; then
        abort_reason="claude exited $rc on iter $i (see $log_file / $jsonl_file)"
        abort "$abort_reason"
    fi

    # --- Parse sentinel from final result event in the raw jsonl ---
    final_result=$(jq -r 'select(.type == "result") | .result // empty' "$jsonl_file" | tail -n1)
    if printf '%s' "$final_result" | grep -q '<ralph-status>done</ralph-status>'; then
        echo "Sentinel reached on iter $i — stopping loop."
        sentinel_seen=true
        completed_iterations=$i
        break
    fi

    # --- Check for stuck-label drift (informational for summary.md) ---
    if [ -z "$forced_issue" ]; then
        # Re-query issues now labeled agent-stuck since run started.
        while IFS= read -r n; do
            [ -n "$n" ] && stuck_issues+=("#$n")
        done < <(container_exec gh issue list \
            --repo lsst-sqre/docverse \
            --label agent-stuck --state open \
            --json number --jq '.[].number' 2>/dev/null \
            | tr -d '\r' || true)
        # de-dup (bash 4+)
        if [ ${#stuck_issues[@]} -gt 0 ]; then
            mapfile -t stuck_issues < <(printf '%s\n' "${stuck_issues[@]}" | sort -u)
        fi
    fi

    completed_iterations=$i
done

echo
echo "Done. Summary: $log_dir/summary.md"
