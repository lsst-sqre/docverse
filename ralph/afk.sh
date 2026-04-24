#!/usr/bin/env bash
# Ralph AFK loop driver for docverse.
#
# Repeatedly spawns Claude Code inside the docverse sandbox to work through the
# backlog of open `prd-task` GitHub issues created by `rubin-prd-to-issues`.
#
# Each iteration is split into two host-orchestrated Claude phases:
#
#   1. select  (cheap model, e.g. Haiku) — ranks the host-prefiltered
#      shortlist by the priority rubric and emits a sentinel-wrapped JSON
#      pick. Sees no code, runs no git commands.
#   2. implement (default model) — receives only the picked issue's body +
#      its parent PRD (if any) + git state, and drives TDD → commit → push →
#      PR. Never sees the shortlist or rubric.
#
# Between the two phases the host extracts the picked issue from a JSON
# shortlist index, fetches its parent PRD body (cached), and does branch
# setup inside the container via `container_exec git …`.
#
# Usage:
#   ralph/afk.sh <iterations>               # all open AFK prd-tasks
#   ralph/afk.sh <iterations> --prd 42      # only children of PRD #42
#   ralph/afk.sh <iterations> --issue 57    # force task #57 (bypasses filters
#                                             *and* the select phase)
#   ralph/afk.sh --dry-run [--prd N | --issue N]
#                                           # print the shortlist and exit;
#                                             no Claude calls, no mutations
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
#   DOCVERSE_RALPH_SELECT_MODEL               (default: claude-haiku-4-5)
#   DOCVERSE_RALPH_SELECT_EFFORT              (low|medium|high|xhigh|max)
#   DOCVERSE_RALPH_IMPLEMENT_MODEL            (default: container default)
#   DOCVERSE_RALPH_IMPLEMENT_EFFORT           (low|medium|high|xhigh|max)
#   DOCVERSE_RALPH_SELECT_TIMEOUT             (default: 120)
#   DOCVERSE_RALPH_ITER_TIMEOUT               (default: 3600)
#
# Requires bash 4+ (uses `mapfile`, associative arrays, and modern array
# features). macOS ships bash 3.2 — install a newer bash via Homebrew
# (`brew install bash`) or run this script in the devcontainer.
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
dry_run=false
use_api=false
gh_token_uri="${DOCVERSE_SANDBOX_GH_TOKEN_OP:-}"
signing_key_private_uri="${DOCVERSE_SANDBOX_SIGNING_KEY_PRIVATE_OP:-}"
signing_key_public_uri="${DOCVERSE_SANDBOX_SIGNING_KEY_PUBLIC_OP:-}"
anthropic_key_uri="${DOCVERSE_SANDBOX_ANTHROPIC_KEY_OP:-}"
docker_context="${DOCVERSE_SANDBOX_DOCKER_CONTEXT:-}"
op_account="${DOCVERSE_SANDBOX_OP_ACCOUNT:-my.1password.com}"

# Per-phase model + effort knobs. Empty values fall back to the container's
# configured defaults (i.e. the flag is omitted from the `claude` invocation).
# See ralph/README.md for accepted effort levels.
select_model="${DOCVERSE_RALPH_SELECT_MODEL:-claude-haiku-4-5}"
select_effort="${DOCVERSE_RALPH_SELECT_EFFORT:-}"
implement_model="${DOCVERSE_RALPH_IMPLEMENT_MODEL:-}"
implement_effort="${DOCVERSE_RALPH_IMPLEMENT_EFFORT:-}"
# Timeout per phase (seconds).
select_timeout="${DOCVERSE_RALPH_SELECT_TIMEOUT:-120}"
iter_timeout="${DOCVERSE_RALPH_ITER_TIMEOUT:-3600}"

# ---- Flag parsing ----
while [[ $# -gt 0 ]]; do
    case "$1" in
        --prd)            prd_filter="$2"; shift 2 ;;
        --issue)          forced_issue="$2"; shift 2 ;;
        --dry-run)        dry_run=true; shift ;;
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

if $dry_run; then
    if [ -n "$iterations" ]; then
        echo "note: --dry-run ignores the iterations argument; shortlist is printed once." >&2
    fi
    iterations=1
else
    [ -n "$iterations" ] || die "Usage: $0 <iterations> [--prd N | --issue N] [--dry-run]"
    [[ "$iterations" =~ ^[0-9]+$ ]] || die "iterations must be a positive integer"
    [ "$iterations" -gt 0 ] || die "iterations must be > 0"
fi
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

# Export GH_TOKEN so host-side `gh` (used on branch-setup failure) finds it
# automatically. The container also receives it via container_exec.
export GH_TOKEN

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

# ---- Helper: decide whether a blocker ref is still blocking ----
# Args: issue_or_pr_number
# Return code: 0 if still blocking, 1 if resolved.
#
# The AFK flow closes task issues at PR-creation time (see README.md), so an
# issue being CLOSED alone does not mean the code has landed — we must follow
# it to its closing PR and require that PR to be MERGED. For refs that are
# themselves PR numbers, any non-OPEN state (CLOSED or MERGED) counts as
# resolved. Query failures fall back to "blocked" so transient ratelimit or
# network blips don't silently unblock a task.
blocker_is_open() {
    local num="$1"
    local payload
    # shellcheck disable=SC2016  # $num is a GraphQL variable, not a shell var
    payload=$(container_exec gh api graphql -F num="$num" -f query='
        query($num: Int!) {
          repository(owner: "lsst-sqre", name: "docverse") {
            issueOrPullRequest(number: $num) {
              __typename
              ... on Issue {
                state
                closedByPullRequestsReferences(first: 10, includeClosedPrs: true) {
                  nodes { state }
                }
              }
              ... on PullRequest { state }
            }
          }
        }
    ' --jq .data.repository.issueOrPullRequest 2>/dev/null || echo '{}')

    local tn st any_merged pr_count
    tn=$(echo "$payload" | jq -r '.__typename // ""')
    st=$(echo "$payload" | jq -r '.state // ""')
    any_merged=$(echo "$payload" \
        | jq -r '[.closedByPullRequestsReferences.nodes[]?.state] | any(. == "MERGED")')
    pr_count=$(echo "$payload" \
        | jq -r '[.closedByPullRequestsReferences.nodes[]?] | length')

    # Query failed or ref not found → treat as blocked (conservative).
    if [ -z "$tn" ] || [ "$tn" = "null" ]; then
        return 0
    fi
    # Still open → blocked.
    if [ "$st" = "OPEN" ]; then
        return 0
    fi
    # Closed issue with closing PRs, none merged → blocked.
    if [ "$tn" = "Issue" ] && [ "$pr_count" -gt 0 ] && [ "$any_merged" != "true" ]; then
        return 0
    fi
    return 1
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
# use. Raw stream is separately tee'd to iter-NN-<phase>.jsonl for replay.
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

# ---- Cache for parent PRD bodies (keyed by issue number) ----
declare -A prd_body_cache

# ---- Helper: write the iter shortlist JSON and markdown ----
# Writes the JSON index to $log_dir/iter-{iter_pad}.shortlist.json.
# Sets globals: PF_SHORTLIST_MD (markdown rendering), SHORTLIST_JSON_PATH.
build_shortlist() {
    local iter_pad="$1"
    local prd="$2"
    local forced="$3"
    local out_json="$log_dir/iter-${iter_pad}.shortlist.json"

    local items
    if [ -n "$forced" ]; then
        # --issue N: fetch that one issue, skip all filters.
        local single
        single=$(container_exec gh issue view "$forced" \
            --repo lsst-sqre/docverse \
            --json number,title,body) \
            || die "gh issue view #$forced failed"
        items=$(printf '%s\n' "$single" | jq '[.]')
    else
        local raw
        raw=$(container_exec gh issue list \
            --repo lsst-sqre/docverse \
            --label prd-task --state open --limit 100 \
            --json number,title,body,labels) \
            || die "gh issue list failed"

        # Apply label + Type + optional Parent PRD filters in jq.
        local candidates
        candidates=$(echo "$raw" | jq -c --arg prd "$prd" '
            [.[]
            | select(any(.labels[]; .name == "agent-stuck") | not)
            | select(.body | test("\\|\\s*Type\\s*\\|\\s*AFK\\s*\\|"; "i"))
            | select(
                ($prd == "") or
                (.body | test("\\|\\s*Parent PRD\\s*\\|\\s*#" + $prd + "\\b"))
              )]
        ') || die "jq filter failed"

        # Blocker filtering: drop any candidate with an open `Blocked by` ref.
        local filtered='[]'
        local len
        len=$(echo "$candidates" | jq 'length')
        local k
        for ((k=0; k<len; k++)); do
            local entry body blockers ok=1
            entry=$(echo "$candidates" | jq -c ".[$k]")
            body=$(echo "$entry" | jq -r .body)
            blockers=$(printf '%s\n' "$body" \
                | grep -iE '^\s*\|?\s*Blocked by' \
                | head -n 1 \
                | grep -oE '#[0-9]+' \
                | tr -d '#' || true)
            for b in $blockers; do
                if blocker_is_open "$b"; then ok=0; break; fi
            done
            if [ "$ok" = "1" ]; then
                filtered=$(jq -c --argjson e "$entry" '. + [$e]' <<<"$filtered")
            fi
        done
        items="$filtered"
    fi

    # Transform items into an index keyed by issue_number with structured
    # metadata extracted from each body's Metadata table. The items JSON is
    # staged through a temp file because python3's stdin is consumed by the
    # heredoc that carries the script body.
    local items_tmp
    items_tmp=$(mktemp)
    printf '%s' "$items" > "$items_tmp"
    local index
    index=$(python3 - "$items_tmp" <<'PYEOF'
import json, re, sys

with open(sys.argv[1], encoding="utf-8") as f:
    items = json.load(f)

index = {}

def field(body, name):
    pat = re.compile(
        r"\|\s*" + re.escape(name) + r"\s*\|\s*([^|]*?)\s*\|",
        re.IGNORECASE,
    )
    m = pat.search(body)
    if not m:
        return ""
    value = m.group(1).strip()
    # Strip markdown code-span backticks (single or double) and any
    # whitespace that was padded around them.
    return value.strip("`").strip()

for it in items:
    num = it["number"]
    body = it.get("body") or ""
    parent_raw = field(body, "Parent PRD")
    parent_match = re.search(r"#(\d+)", parent_raw)
    parent_prd = parent_match.group(1) if parent_match else ""
    order_raw = field(body, "Task Order")
    order_digits = re.sub(r"\D", "", order_raw)
    try:
        task_order = int(order_digits) if order_digits else 9999
    except ValueError:
        task_order = 9999
    index[str(num)] = {
        "title": it.get("title") or "",
        "body": body,
        "jira_key": field(body, "Jira Key"),
        "branch": field(body, "Branch"),
        "task_order": task_order,
        "parent_prd": parent_prd,
    }

json.dump(index, sys.stdout)
PYEOF
)
    rm -f "$items_tmp"

    printf '%s' "$index" > "$out_json"
    SHORTLIST_JSON_PATH="$out_json"

    # Render markdown from the same index, ordered by task_order then issue
    # number (ascending).
    PF_SHORTLIST_MD=$(printf '%s\n' "$index" | jq -r '
        to_entries
        | sort_by(.value.task_order, (.key | tonumber))
        | map("--- ISSUE #\(.key) ---\n\(.value.title)\n\n\(.value.body)\n\n")
        | join("")
    ')
}

# ---- Helper: fetch parent PRD body, cached across iterations ----
# Args: parent_issue_number
# Echoes a markdown block (with a `## Parent PRD …` header) on stdout, or
# empty string if no parent.
get_parent_prd_body() {
    local num="$1"
    [ -z "$num" ] && return 0
    if [ -n "${prd_body_cache[$num]+_}" ]; then
        printf '%s' "${prd_body_cache[$num]}"
        return 0
    fi
    local rendered
    rendered=$(container_exec gh issue view "$num" \
        --repo lsst-sqre/docverse \
        --json title,body \
        --template '## Parent PRD context (#'"$num"': {{.title}})

{{.body}}' 2>/dev/null || true)
    prd_body_cache[$num]="$rendered"
    printf '%s' "$rendered"
}

# ---- Helper: refresh git state from inside the container ----
# Sets globals PF_BRANCH, PF_GIT_STATUS, PF_GIT_LOG.
refresh_git_state() {
    PF_BRANCH=$(container_exec git -C /workspace/docverse branch --show-current \
        | tr -d '\r')
    PF_GIT_STATUS=$(container_exec git -C /workspace/docverse status --porcelain \
        | tr -d '\r' || true)
    PF_GIT_LOG=$(container_exec git -C /workspace/docverse log -n 10 \
        --format='%H%n%ad%n%B---' --date=short \
        | tr -d '\r' || true)
}

# ---- Helper: render a prompt template, substituting __VAR__ placeholders ----
# Args: template_path
# Placeholders with empty values have their entire line stripped.
# Reads mappings from PF_* env vars set by the caller.
render_prompt() {
    local template="$1"
    python3 - "$template" <<'PYEOF'
import os, re, sys

mapping = {
    "__ITERATION__":    os.environ.get("PF_ITERATION", ""),
    "__TOTAL__":        os.environ.get("PF_TOTAL", ""),
    "__BRANCH__":       os.environ.get("PF_BRANCH", ""),
    "__GIT_STATUS__":   os.environ.get("PF_GIT_STATUS_R", ""),
    "__GIT_LOG__":      os.environ.get("PF_GIT_LOG", ""),
    "__SHORTLIST__":    os.environ.get("PF_SHORTLIST_R", ""),
    "__PRD_CONTEXT__":  os.environ.get("PF_PRD_CONTEXT", ""),
    "__ISSUE_NUMBER__": os.environ.get("PF_ISSUE_NUMBER", ""),
    "__ISSUE_BODY__":   os.environ.get("PF_ISSUE_BODY", ""),
    "__PRD_BODY__":     os.environ.get("PF_PRD_BODY", ""),
    "__JIRA_KEY__":     os.environ.get("PF_JIRA_KEY", ""),
}

with open(sys.argv[1], encoding="utf-8") as f:
    text = f.read()

for placeholder, value in mapping.items():
    if value == "":
        text = re.sub(
            rf'^[^\n]*{re.escape(placeholder)}[^\n]*\n?',
            '', text, flags=re.MULTILINE,
        )
    else:
        text = text.replace(placeholder, value)

sys.stdout.write(text)
PYEOF
}

# ---- Helper: run one Claude phase, return final result text ----
# Args: phase_name prompt_file model effort timeout iter_pad
#
# Streams stream-json from claude inside the container through tee (→ jsonl
# replay), jq (→ human-readable), and a final tee (→ log + stderr for live
# terminal view). The function echoes only the parsed final `result` event on
# stdout so the caller can $(…)-capture it.
#
# If `model` / `effort` is non-empty, passes the corresponding flag to claude;
# otherwise uses the container's configured defaults.
#
# Emits log files named:
#   iter-{iter_pad}-{phase}.prompt.md  (already written by caller)
#   iter-{iter_pad}-{phase}.jsonl
#   iter-{iter_pad}-{phase}.log
run_phase() {
    local phase="$1" prompt_file="$2" model="$3" effort="$4" to="$5" iter_pad="$6"

    local jsonl="$log_dir/iter-${iter_pad}-${phase}.jsonl"
    local logf="$log_dir/iter-${iter_pad}-${phase}.log"

    local -a claude_args=(claude)
    if [ -n "$model" ]; then
        claude_args+=(--model "$model")
    fi
    if [ -n "$effort" ]; then
        claude_args+=(--effort "$effort")
    fi
    claude_args+=(--dangerously-skip-permissions --verbose
                  --output-format stream-json -p -)

    echo "Running claude [$phase] (timeout ${to}s, model=${model:-default}, effort=${effort:-default})..." >&2
    set +e
    cat "$prompt_file" | container_exec --timeout "$to" \
        /usr/local/bin/agent-entry "${claude_args[@]}" \
        | tee "$jsonl" \
        | jq -r --unbuffered "$stream_filter" 2>/dev/null \
        | tee "$logf" >&2
    local rc=${PIPESTATUS[1]}
    set -e

    if [ "$rc" -ne 0 ]; then
        abort_reason="claude [$phase] exited $rc on iter $iter_pad (see $logf / $jsonl)"
        abort "$abort_reason"
    fi

    jq -r 'select(.type == "result") | .result // empty' "$jsonl" | tail -n1
}

# ---- Helper: parse a <ralph-pick> sentinel from a result blob ----
# stdin: result text. stdout: pick JSON (one line) if sentinel present, else
# empty. Returns 0 always; caller inspects output emptiness.
parse_pick_sentinel() {
    python3 -c '
import json, re, sys
text = sys.stdin.read()
m = re.search(r"<ralph-pick>\s*(.*?)\s*</ralph-pick>", text, re.DOTALL)
if not m:
    sys.exit(0)
raw = m.group(1).strip()
try:
    obj = json.loads(raw)
except Exception:
    sys.exit(0)
print(json.dumps(obj))
'
}

# ---- Helper: set up the picked task's branch inside the container ----
# Args: branch
# stdout: empty on success; git error text on failure.
# Return code: 0 success, 1 failure (all three checkout attempts failed).
container_branch_setup() {
    local branch="$1"
    local errs=""

    # Best-effort fetch of the specific branch; it may legitimately not exist
    # upstream yet (new task branches).
    container_exec git -C /workspace/docverse fetch --prune origin "$branch" \
        >/dev/null 2>&1 || true

    # Try local, then remote-tracking, then a fresh branch from main.
    local out
    if out=$(container_exec git -C /workspace/docverse checkout "$branch" 2>&1); then
        # FF to origin if an upstream exists. The earlier branch-specific
        # fetch made origin/$branch current; --ff-only keeps this safe by
        # refusing to clobber local-only commits (divergence is surfaced
        # as a setup failure for the user to resolve manually).
        if container_exec git -C /workspace/docverse \
                rev-parse --verify --quiet "origin/$branch" >/dev/null 2>&1; then
            if ! out=$(container_exec git -C /workspace/docverse \
                    merge --ff-only "origin/$branch" 2>&1); then
                errs+=$'git merge --ff-only origin/'"$branch"$':\n'"$out"$'\n'
                printf '%s' "$errs"
                return 1
            fi
        fi
        return 0
    fi
    errs+=$'git checkout '"$branch"$':\n'"$out"$'\n'

    if out=$(container_exec git -C /workspace/docverse \
            checkout -b "$branch" "origin/$branch" 2>&1); then
        return 0
    fi
    errs+=$'git checkout -b '"$branch"$' origin/'"$branch"$':\n'"$out"$'\n'

    if out=$(container_exec git -C /workspace/docverse \
            checkout -b "$branch" origin/main 2>&1); then
        return 0
    fi
    errs+=$'git checkout -b '"$branch"$' origin/main:\n'"$out"$'\n'

    printf '%s' "$errs"
    return 1
}

# ---- Helper: host-side stuck marker when branch setup fails ----
# Args: issue_number error_text
host_mark_stuck() {
    local issue="$1" err="$2"
    local body
    # shellcheck disable=SC2016  # backticks are literal markdown, not command substitution
    body=$(printf 'Ralph host-side branch setup failed — no commit created.\n\n```\n%s\n```\n\nHuman intervention required; clear the `agent-stuck` label to retry.' "$err")
    gh issue edit "$issue" --repo lsst-sqre/docverse --add-label agent-stuck \
        >/dev/null 2>&1 \
        || echo "warning: host gh issue edit failed for #$issue" >&2
    gh issue comment "$issue" --repo lsst-sqre/docverse --body "$body" \
        >/dev/null 2>&1 \
        || echo "warning: host gh issue comment failed for #$issue" >&2
    host_stuck_issues+=("#$issue")
}

# ---- Summary accumulators ----
stuck_issues=()
host_stuck_issues=()
abort_reason=""
sentinel_seen=false
completed_iterations=0
declare -a iter_picks=()
declare -a iter_wall=()

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
        echo "## Configuration"
        echo "- Select model: ${select_model:-<claude default>}"
        echo "- Select effort: ${select_effort:-<claude default>}"
        echo "- Implement model: ${implement_model:-<claude default>}"
        echo "- Implement effort: ${implement_effort:-<claude default>}"
        echo
        if [ ${#iter_picks[@]} -gt 0 ]; then
            echo "## Per-iteration picks"
            for line in "${iter_picks[@]}"; do echo "- $line"; done
            echo
        fi
        if [ ${#iter_wall[@]} -gt 0 ]; then
            echo "## Per-phase wall-clock"
            for line in "${iter_wall[@]}"; do echo "- $line"; done
            echo
        fi
        if [ ${#host_stuck_issues[@]} -gt 0 ]; then
            echo "## Host-marked stuck (branch-setup failures)"
            for i in "${host_stuck_issues[@]}"; do echo "- $i"; done
            echo
        fi
        if [ ${#stuck_issues[@]} -gt 0 ]; then
            echo "## Stuck issues observed this run"
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

    # --- Build shortlist (writes JSON; sets PF_SHORTLIST_MD) ---
    build_shortlist "$iter_pad" "$prd_filter" "$forced_issue"

    # --- Dry-run: print the shortlist rows and exit without Claude calls ---
    if $dry_run; then
        scope_label="all AFK tasks"
        if [ -n "$forced_issue" ]; then
            scope_label="forced #$forced_issue"
        elif [ -n "$prd_filter" ]; then
            scope_label="PRD #$prd_filter"
        fi
        count=$(jq 'length' "$SHORTLIST_JSON_PATH")
        echo
        echo "Ralph dry-run: ${count} eligible task(s) — scope: ${scope_label}"
        if [ "$count" = "0" ]; then
            echo "(Nothing matches the host-side filters right now.)"
        else
            echo
            python3 - "$SHORTLIST_JSON_PATH" <<'PYEOF'
import json, sys

with open(sys.argv[1], encoding="utf-8") as f:
    idx = json.load(f)

rows = sorted(
    idx.items(),
    key=lambda kv: (kv[1].get("task_order", 9999), int(kv[0])),
)

jira_w = max((len(v.get("jira_key") or "") for _, v in rows), default=0)
branch_w = max((len(v.get("branch") or "") for _, v in rows), default=0)

for num, v in rows:
    jira = v.get("jira_key") or ""
    branch = v.get("branch") or ""
    parent = v.get("parent_prd") or ""
    parent_s = f"parent #{parent}" if parent else ""
    order = v.get("task_order", "")
    title = v.get("title") or ""
    print(
        f"  #{num:>4}  order {order:<3}  "
        f"{jira:<{jira_w}}  {branch:<{branch_w}}  "
        f"{parent_s:<12}  {title}"
    )
PYEOF
        fi
        echo
        echo "Full shortlist JSON: $SHORTLIST_JSON_PATH"
        exit 0
    fi

    # Short-circuit: empty shortlist + no forced issue → no Claude calls.
    shortlist_empty=false
    if [ "$(jq 'length' "$SHORTLIST_JSON_PATH")" = "0" ]; then
        shortlist_empty=true
    fi
    if $shortlist_empty && [ -z "$forced_issue" ]; then
        echo "No eligible tasks remain; exiting loop without any Claude calls."
        sentinel_seen=true
        break
    fi

    # --- Determine the pick (forced | select-phase | stop) ---
    pick_json=""
    if [ -n "$forced_issue" ]; then
        pick_json=$(jq -c --arg n "$forced_issue" '
            if has($n) then
                {issue_number: ($n | tonumber),
                 branch:       .[$n].branch,
                 jira_key:     .[$n].jira_key,
                 reason:       "forced via --issue"}
            else null end
        ' "$SHORTLIST_JSON_PATH")
        if [ "$pick_json" = "null" ] || [ -z "$pick_json" ]; then
            abort_reason="--issue $forced_issue not present in shortlist index"
            abort "$abort_reason"
        fi
        echo "Forced pick: #$forced_issue (skipping select phase)."
    else
        # ----- SELECT PHASE -----
        select_prompt_file="$log_dir/iter-${iter_pad}-select.prompt.md"

        prd_context=""
        if [ -n "$prd_filter" ]; then
            prd_context=$(get_parent_prd_body "$prd_filter")
        fi

        # Populate PF_GIT_LOG so the select prompt can surface recent
        # `Next-iteration notes:` hints to the ranking model.
        refresh_git_state

        PF_ITERATION="$i" PF_TOTAL="$iterations" \
        PF_GIT_LOG="$PF_GIT_LOG" \
        PF_SHORTLIST_R="$PF_SHORTLIST_MD" \
        PF_PRD_CONTEXT="$prd_context" \
        render_prompt "$SCRIPT_DIR/select-prompt.md" > "$select_prompt_file"

        select_start=$SECONDS
        set +e
        select_result=$(run_phase select "$select_prompt_file" \
            "$select_model" "$select_effort" "$select_timeout" "$iter_pad")
        select_rc=$?
        set -e
        select_elapsed=$((SECONDS - select_start))
        iter_wall+=("iter $iter_pad select: ${select_elapsed}s")
        if [ "$select_rc" -ne 0 ]; then
            # run_phase already aborted via the `abort` function; defensive.
            abort_reason="run_phase select returned $select_rc on iter $i"
            abort "$abort_reason"
        fi

        # Parse sentinels.
        if printf '%s' "$select_result" | grep -q '<ralph-status>done</ralph-status>'; then
            echo "Select phase signalled done on iter $i — stopping loop."
            sentinel_seen=true
            completed_iterations=$i
            break
        fi

        pick_json=$(printf '%s' "$select_result" | parse_pick_sentinel || true)
        if [ -z "$pick_json" ]; then
            echo "Select phase produced no valid <ralph-pick> on iter $i; retrying once." >&2
            set +e
            select_result=$(run_phase select "$select_prompt_file" \
                "$select_model" "$select_effort" "$select_timeout" "${iter_pad}-retry")
            retry_rc=$?
            set -e
            if [ "$retry_rc" -ne 0 ]; then
                abort_reason="run_phase select retry returned $retry_rc on iter $i"
                abort "$abort_reason"
            fi
            if printf '%s' "$select_result" | grep -q '<ralph-status>done</ralph-status>'; then
                echo "Select phase retry signalled done — stopping loop."
                sentinel_seen=true
                completed_iterations=$i
                break
            fi
            pick_json=$(printf '%s' "$select_result" | parse_pick_sentinel || true)
            if [ -z "$pick_json" ]; then
                echo "Select phase still malformed on iter $i; aborting iteration and continuing." >&2
                iter_picks+=("iter $iter_pad: SELECT MALFORMED — iteration skipped")
                continue
            fi
        fi
    fi

    # --- Validate pick against shortlist JSON ---
    picked_num=$(printf '%s' "$pick_json" | jq -r '.issue_number')
    if [ -z "$picked_num" ] || [ "$picked_num" = "null" ]; then
        echo "Pick JSON missing issue_number on iter $i; aborting iteration." >&2
        iter_picks+=("iter $iter_pad: INVALID PICK JSON — iteration skipped")
        continue
    fi
    if [ "$(jq -r --arg n "$picked_num" 'has($n)' "$SHORTLIST_JSON_PATH")" != "true" ]; then
        echo "Picked #$picked_num not in shortlist index on iter $i; aborting iteration." >&2
        iter_picks+=("iter $iter_pad: PICK #$picked_num NOT IN SHORTLIST — iteration skipped")
        continue
    fi

    # --- Extract picked task's data from JSON ---
    picked_title=$(jq -r --arg n "$picked_num" '.[$n].title' "$SHORTLIST_JSON_PATH")
    picked_body=$(jq -r --arg n "$picked_num" '.[$n].body' "$SHORTLIST_JSON_PATH")
    picked_branch=$(jq -r --arg n "$picked_num" '.[$n].branch' "$SHORTLIST_JSON_PATH")
    picked_jira=$(jq -r --arg n "$picked_num" '.[$n].jira_key' "$SHORTLIST_JSON_PATH")
    picked_parent=$(jq -r --arg n "$picked_num" '.[$n].parent_prd' "$SHORTLIST_JSON_PATH")
    pick_reason=$(printf '%s' "$pick_json" | jq -r '.reason // ""')
    iter_picks+=("iter $iter_pad: #$picked_num ($picked_title) — $pick_reason")
    echo "Picked #$picked_num: $picked_title"

    if [ -z "$picked_branch" ] || [ "$picked_branch" = "null" ]; then
        echo "Picked #$picked_num has no Branch metadata; marking stuck via host." >&2
        host_mark_stuck "$picked_num" "Task issue is missing a Branch field in its Metadata table."
        continue
    fi

    # --- Parent PRD body for implement prompt (cached) ---
    picked_prd_body=""
    if [ -n "$picked_parent" ] && [ "$picked_parent" != "null" ]; then
        picked_prd_body=$(get_parent_prd_body "$picked_parent")
    fi

    # --- Container-side branch setup ---
    echo "Setting up branch '$picked_branch' in container..."
    set +e
    branch_err=$(container_branch_setup "$picked_branch")
    branch_rc=$?
    set -e
    if [ "$branch_rc" -ne 0 ]; then
        echo "Container-side branch setup failed for #$picked_num:" >&2
        printf '%s\n' "$branch_err" >&2
        host_mark_stuck "$picked_num" "$branch_err"
        continue
    fi

    # --- Fresh git state after checkout ---
    refresh_git_state

    # --- Render implement prompt ---
    implement_prompt_file="$log_dir/iter-${iter_pad}-implement.prompt.md"
    git_status_text="$PF_GIT_STATUS"
    [ -z "$git_status_text" ] && git_status_text="(clean)"

    PF_ITERATION="$i" PF_TOTAL="$iterations" \
    PF_BRANCH="$PF_BRANCH" \
    PF_GIT_STATUS_R="$git_status_text" \
    PF_GIT_LOG="$PF_GIT_LOG" \
    PF_ISSUE_NUMBER="$picked_num" \
    PF_ISSUE_BODY="$picked_body" \
    PF_PRD_BODY="$picked_prd_body" \
    PF_JIRA_KEY="$picked_jira" \
    render_prompt "$SCRIPT_DIR/implement-prompt.md" > "$implement_prompt_file"

    # --- Run implement phase ---
    implement_start=$SECONDS
    set +e
    run_phase implement "$implement_prompt_file" \
        "$implement_model" "$implement_effort" "$iter_timeout" "$iter_pad" \
        >/dev/null
    implement_rc=$?
    set -e
    implement_elapsed=$((SECONDS - implement_start))
    iter_wall+=("iter $iter_pad implement: ${implement_elapsed}s")
    if [ "$implement_rc" -ne 0 ]; then
        abort_reason="run_phase implement returned $implement_rc on iter $i"
        abort "$abort_reason"
    fi

    # --- Check for stuck-label drift (informational for summary.md) ---
    if [ -z "$forced_issue" ]; then
        while IFS= read -r n; do
            [ -n "$n" ] && stuck_issues+=("#$n")
        done < <(container_exec gh issue list \
            --repo lsst-sqre/docverse \
            --label agent-stuck --state open \
            --json number --jq '.[].number' 2>/dev/null \
            | tr -d '\r' || true)
        if [ ${#stuck_issues[@]} -gt 0 ]; then
            mapfile -t stuck_issues < <(printf '%s\n' "${stuck_issues[@]}" | sort -u)
        fi
    fi

    completed_iterations=$i
done

echo
echo "Done. Summary: $log_dir/summary.md"
