#!/usr/bin/env bash
# Stop the devcontainer. Named volumes persist for next session.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ -n "${DOCVERSE_SANDBOX_DOCKER_CONTEXT:-}" ]; then
    export DOCKER_CONTEXT="$DOCVERSE_SANDBOX_DOCKER_CONTEXT"
fi

devcontainer down --workspace-folder "$REPO_DIR"
