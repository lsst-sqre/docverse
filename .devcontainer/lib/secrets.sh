#!/usr/bin/env bash
# Shared secret-loading helpers for host-side scripts (run.sh, ralph/afk.sh).
#
# Sourcing this file defines functions but takes no action. Callers invoke the
# functions below after they have validated their own flags and env vars.
#
# Required inputs (set by caller before calling load_sandbox_secrets):
#   gh_token_uri             — 1Password URI for GitHub token
#   signing_key_private_uri  — 1Password URI for SSH signing key (private)
#   signing_key_public_uri   — 1Password URI for SSH signing key (public)
#   op_account               — 1Password account shorthand (e.g. my.1password.com)
#
# Optional:
#   use_api=true + anthropic_key_uri — load ANTHROPIC_API_KEY
#
# Outputs (exported into the caller's shell):
#   GH_TOKEN, SIGNING_KEY_PEM, SIGNING_KEY_PUB, ANTHROPIC_API_KEY (if use_api),
#   GIT_USER_NAME, GIT_USER_EMAIL

# Callers are expected to have set `set -euo pipefail` and defined `die`.
# We avoid setting those here so sourcing is side-effect-free.

load_sandbox_secrets() {
    local _use_api="${use_api:-false}"
    echo "Loading secrets from 1Password..."
    GH_TOKEN=$(op read --account "$op_account" "$gh_token_uri") \
        || die "Failed to read GH token from 1Password"
    SIGNING_KEY_PEM=$(op read --account "$op_account" "$signing_key_private_uri") \
        || die "Failed to read signing key (private)"
    SIGNING_KEY_PUB=$(op read --account "$op_account" "$signing_key_public_uri") \
        || die "Failed to read signing key (public)"

    ANTHROPIC_API_KEY=""
    if [ "$_use_api" = "true" ]; then
        ANTHROPIC_API_KEY=$(op read --account "$op_account" "$anthropic_key_uri") \
            || die "Failed to read Anthropic API key"
    fi
}

load_host_git_identity() {
    GIT_USER_NAME=$(git config --global user.name 2>/dev/null) \
        || die "git config --global user.name not set"
    GIT_USER_EMAIL=$(git config --global user.email 2>/dev/null) \
        || die "git config --global user.email not set"
}
