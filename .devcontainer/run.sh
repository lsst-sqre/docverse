#!/usr/bin/env bash
# Host-side entry point for the docverse sandbox.
# One invocation = one autonomous iteration OR one interactive session.
#
# Usage:
#   .devcontainer/run.sh prompt.md          # Run claude with prompt from file
#   echo "fix the bug" | .devcontainer/run.sh  # Run claude with prompt from stdin
#   .devcontainer/run.sh --login            # Interactive login session
#   .devcontainer/run.sh --rebuild --login  # Rebuild image, then login
#
# Required env vars (or CLI flag overrides):
#   DOCVERSE_SANDBOX_GH_TOKEN_OP              (--gh-token-uri)
#   DOCVERSE_SANDBOX_SIGNING_KEY_PRIVATE_OP   (--signing-key-private-uri)
#   DOCVERSE_SANDBOX_SIGNING_KEY_PUBLIC_OP    (--signing-key-public-uri)
#
# Optional env vars:
#   DOCVERSE_SANDBOX_ANTHROPIC_KEY_OP         (--anthropic-key-uri, required with --api)
#   DOCVERSE_SANDBOX_DOCKER_CONTEXT           (--docker-context, e.g. agent-sandbox)
#   DOCVERSE_SANDBOX_OP_ACCOUNT               (--op-account, default: my.1password.com)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Exit code for infra failures
EX_INFRA=64

die() { echo "error: $*" >&2; exit "$EX_INFRA"; }

# --- Defaults ---
mode="prompt"
rebuild=false
use_api=false
gh_token_uri="${DOCVERSE_SANDBOX_GH_TOKEN_OP:-}"
signing_key_private_uri="${DOCVERSE_SANDBOX_SIGNING_KEY_PRIVATE_OP:-}"
signing_key_public_uri="${DOCVERSE_SANDBOX_SIGNING_KEY_PUBLIC_OP:-}"
anthropic_key_uri="${DOCVERSE_SANDBOX_ANTHROPIC_KEY_OP:-}"
docker_context="${DOCVERSE_SANDBOX_DOCKER_CONTEXT:-}"
op_account="${DOCVERSE_SANDBOX_OP_ACCOUNT:-my.1password.com}"

# --- Flag parsing ---
positional=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --api)
            use_api=true; shift ;;
        --rebuild)
            rebuild=true; shift ;;
        --login)
            mode="login"; shift ;;
        --gh-token-uri)
            gh_token_uri="$2"; shift 2 ;;
        --signing-key-private-uri)
            signing_key_private_uri="$2"; shift 2 ;;
        --signing-key-public-uri)
            signing_key_public_uri="$2"; shift 2 ;;
        --anthropic-key-uri)
            anthropic_key_uri="$2"; shift 2 ;;
        --docker-context)
            docker_context="$2"; shift 2 ;;
        --op-account)
            op_account="$2"; shift 2 ;;
        -*)
            die "Unknown flag: $1" ;;
        *)
            positional+=("$1"); shift ;;
    esac
done

# --- Validate required URIs ---
[ -n "$gh_token_uri" ] || die "DOCVERSE_SANDBOX_GH_TOKEN_OP or --gh-token-uri required"
[ -n "$signing_key_private_uri" ] || die "DOCVERSE_SANDBOX_SIGNING_KEY_PRIVATE_OP or --signing-key-private-uri required"
[ -n "$signing_key_public_uri" ] || die "DOCVERSE_SANDBOX_SIGNING_KEY_PUBLIC_OP or --signing-key-public-uri required"
if $use_api; then
    [ -n "$anthropic_key_uri" ] || die "DOCVERSE_SANDBOX_ANTHROPIC_KEY_OP or --anthropic-key-uri required with --api"
fi

# --- Secret loading (host-side via 1Password CLI) ---
echo "Loading secrets from 1Password..."
GH_TOKEN=$(op read --account "$op_account" "$gh_token_uri") || die "Failed to read GH token from 1Password"
SIGNING_KEY_PEM=$(op read --account "$op_account" "$signing_key_private_uri") || die "Failed to read signing key (private)"
SIGNING_KEY_PUB=$(op read --account "$op_account" "$signing_key_public_uri") || die "Failed to read signing key (public)"

ANTHROPIC_API_KEY=""
if $use_api; then
    ANTHROPIC_API_KEY=$(op read --account "$op_account" "$anthropic_key_uri") || die "Failed to read Anthropic API key"
fi

# --- Host git identity ---
GIT_USER_NAME=$(git config --global user.name 2>/dev/null) || die "git config --global user.name not set"
GIT_USER_EMAIL=$(git config --global user.email 2>/dev/null) || die "git config --global user.email not set"

# --- Read prompt (unless login mode) ---
PROMPT=""
if [ "$mode" = "prompt" ]; then
    if [ ${#positional[@]} -gt 0 ]; then
        PROMPT=$(cat "${positional[0]}") || die "Failed to read prompt file: ${positional[0]}"
    elif [ ! -t 0 ]; then
        PROMPT=$(cat)
    else
        die "No prompt provided. Pass a file path or pipe via stdin."
    fi
    [ -n "$PROMPT" ] || die "Prompt is empty"
fi

# --- Docker context ---
if [ -n "$docker_context" ]; then
    export DOCKER_CONTEXT="$docker_context"
fi

# --- Container lifecycle ---
echo "Starting devcontainer..."
up_args=(--workspace-folder "$REPO_DIR")
if $rebuild; then
    up_args+=(--remove-existing-container)
fi
up_output=$(devcontainer up "${up_args[@]}") || die "devcontainer up failed"
container_id=$(echo "$up_output" | grep -o '"containerId":"[^"]*"' | head -1 | cut -d'"' -f4)
[ -n "$container_id" ] || die "Could not determine container ID from devcontainer up output"

# --- Build remote-env array ---
exec_args=(--workspace-folder "$REPO_DIR")
exec_args+=(--remote-env "GH_TOKEN=$GH_TOKEN")
exec_args+=(--remote-env "DOCVERSE_SIGNING_KEY_PEM=$SIGNING_KEY_PEM")
exec_args+=(--remote-env "DOCVERSE_SIGNING_KEY_PUB=$SIGNING_KEY_PUB")
exec_args+=(--remote-env "DOCVERSE_SANDBOX_GIT_USER_NAME=$GIT_USER_NAME")
exec_args+=(--remote-env "DOCVERSE_SANDBOX_GIT_USER_EMAIL=$GIT_USER_EMAIL")
if $use_api; then
    exec_args+=(--remote-env "ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY")
fi

# --- Execute ---
if [ "$mode" = "login" ]; then
    echo "Starting interactive session..."
    docker_args=(-it)
    docker_args+=(-e "GH_TOKEN=$GH_TOKEN")
    docker_args+=(-e "DOCVERSE_SIGNING_KEY_PEM=$SIGNING_KEY_PEM")
    docker_args+=(-e "DOCVERSE_SIGNING_KEY_PUB=$SIGNING_KEY_PUB")
    docker_args+=(-e "DOCVERSE_SANDBOX_GIT_USER_NAME=$GIT_USER_NAME")
    docker_args+=(-e "DOCVERSE_SANDBOX_GIT_USER_EMAIL=$GIT_USER_EMAIL")
    if $use_api; then
        docker_args+=(-e "ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY")
    fi
    docker_args+=(-u node)
    exec docker exec "${docker_args[@]}" "$container_id" /usr/local/bin/agent-entry --interactive zsh
else
    echo "Running claude with prompt..."
    echo "$PROMPT" | devcontainer exec "${exec_args[@]}" \
        /usr/local/bin/agent-entry \
        claude --dangerously-skip-permissions -p -
fi
