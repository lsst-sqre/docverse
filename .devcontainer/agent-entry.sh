#!/usr/bin/env bash
# In-container entry point for sandbox sessions.
# Called by run.sh via devcontainer exec.
set -euo pipefail

# --- Materialize signing key to tmpfs ---
if [ -n "${DOCVERSE_SIGNING_KEY_PEM:-}" ]; then
    sudo mkdir -p /run/docverse-sandbox
    sudo chown node:node /run/docverse-sandbox
    umask 077
    printf '%s\n' "$DOCVERSE_SIGNING_KEY_PEM" > /run/docverse-sandbox/signing-key
    chmod 0600 /run/docverse-sandbox/signing-key
    printf '%s\n' "$DOCVERSE_SIGNING_KEY_PUB" > /run/docverse-sandbox/signing-key.pub
    chmod 0644 /run/docverse-sandbox/signing-key.pub
    git config --global user.signingkey /run/docverse-sandbox/signing-key.pub
fi

# --- Configure git identity ---
if [ -n "${DOCVERSE_SANDBOX_GIT_USER_NAME:-}" ]; then
    git config --global user.name "$DOCVERSE_SANDBOX_GIT_USER_NAME"
fi
if [ -n "${DOCVERSE_SANDBOX_GIT_USER_EMAIL:-}" ]; then
    git config --global user.email "$DOCVERSE_SANDBOX_GIT_USER_EMAIL"
fi

# --- Authenticate gh CLI ---
if [ -n "${GH_TOKEN:-}" ]; then
    gh auth status || echo "warning: gh auth status failed (GH_TOKEN is set, gh will use it)" >&2
fi

# --- Clone repo if not yet present ---
if [ ! -d /workspace/docverse/.git ]; then
    git clone https://github.com/lsst-sqre/docverse.git /workspace/docverse
fi
cd /workspace/docverse

# --- Execute command ---
if [ "${1:-}" = "--interactive" ]; then
    shift
    exec "${@:-zsh}"
else
    exec "$@"
fi
