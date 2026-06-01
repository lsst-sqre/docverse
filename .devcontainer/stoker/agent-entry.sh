#!/usr/bin/env bash
# stoker-managed: sandbox:stoker/agent-entry.sh:385f9b5e805c5d65
# Sandbox entrypoint for stoker. Configures git identity, refreshes the
# firewall, and exec's the agent harness. Secrets are resolved host-side
# by the stoker CLI and injected via `--remote-env` on each `exec` /
# harness invocation, so this script has no secret-loading work to do.
set -euo pipefail

THIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

stoker_configure_git() {
    # The git identity name/email come from host `git config --global` —
    # they're plumbed through devcontainer environment variables and
    # written into ~/.gitconfig here. Phase 1 leaves the body inert.
    : "${STOKER_GIT_IDENTITY_NAME:=}"
    : "${STOKER_GIT_IDENTITY_EMAIL:=}"
    if [ -n "${STOKER_GIT_IDENTITY_NAME}" ]; then
        git config --global user.name "${STOKER_GIT_IDENTITY_NAME}"
    fi
    if [ -n "${STOKER_GIT_IDENTITY_EMAIL}" ]; then
        git config --global user.email "${STOKER_GIT_IDENTITY_EMAIL}"
    fi
}

stoker_configure_ssh_signing() {
    # When the host has injected both signing keys plus the matching
    # allowed_signers list, point git at them for SSH commit signing.
    # All three files must be present — partial state is treated as
    # "signing not configured" (matches the host-side gating).
    if [ -f "${HOME}/.ssh/id_ed25519" ] \
        && [ -f "${HOME}/.ssh/id_ed25519.pub" ] \
        && [ -f "${HOME}/.ssh/allowed_signers" ]; then
        git config --global gpg.format ssh
        git config --global user.signingkey "${HOME}/.ssh/id_ed25519.pub"
        git config --global commit.gpgsign true
        git config --global gpg.ssh.allowedSignersFile \
            "${HOME}/.ssh/allowed_signers"
    fi
}

stoker_configure_firewall() {
    if command -v sudo >/dev/null 2>&1; then
        sudo -n "${THIS_DIR}/init-firewall.sh" || true
    else
        "${THIS_DIR}/init-firewall.sh" || true
    fi
}

stoker_configure_git
stoker_configure_ssh_signing
stoker_configure_firewall

# Default to dropping into a shell when no command is supplied; Phase 2's
# Claude binding will exec `claude` directly.
if [ "$#" -eq 0 ]; then
    exec /bin/bash -l
fi
exec "$@"
