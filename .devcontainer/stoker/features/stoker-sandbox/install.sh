#!/usr/bin/env bash
# stoker-managed: sandbox:.devcontainer/stoker/features/stoker-sandbox/install.sh:c2b7ffe1fa4d1666
# stoker-sandbox devcontainer Feature install script.
#
# Bakes stoker's *operational* sandbox tooling into the image so the repo's
# own .devcontainer/Dockerfile (the human, general-purpose dev container) can
# stay project-only. Runs as root at image build (PRD #200 follow-up):
#
#   * the egress-firewall stack init-firewall.sh drives — iproute2 / iptables /
#     ipset, plus the gh / jq / curl / gnupg / ca-certificates it shells out to;
#   * sudo + cron, so agent-entry.sh and firewall-refresh.cron can re-run the
#     firewall as root without a password prompt;
#   * the 1Password CLI for the host-side `op` secret backend handoff during
#     `stoker shell`;
#   * the NOPASSWD sudoers rule pinning the relocated init-firewall.sh path.
#
# The init-firewall.sh / firewall-refresh.cron / agent-entry.sh scripts
# themselves stay package files under .devcontainer/stoker/ — this Feature only
# provides the OS packages and sudoers rule they depend on.
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

# Drop the base image's stale Yarn apt source before the first apt-get update:
# Yarn's signing key rotates faster than the base image ships, and stoker
# doesn't use Yarn. Best-effort (`rm -f`) so a base without it is a no-op.
rm -f /etc/apt/sources.list.d/yarn.list \
      /usr/share/keyrings/yarnkey.gpg

apt-get update
apt-get install -y --no-install-recommends \
    ca-certificates \
    cron \
    curl \
    gnupg \
    iproute2 \
    iptables \
    ipset \
    jq \
    sudo

# gh (GitHub CLI): init-firewall.sh pulls GitHub's live /meta CIDR ranges via
# `gh api meta`.
curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
    | gpg --dearmor -o /usr/share/keyrings/githubcli-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
    > /etc/apt/sources.list.d/github-cli.list
apt-get update
apt-get install -y --no-install-recommends gh

# 1Password CLI: the host-side `op` secret backend handoff during
# `stoker shell`. Light footprint when unused.
curl -fsSL https://downloads.1password.com/linux/keys/1password.asc \
    | gpg --dearmor -o /usr/share/keyrings/1password-archive-keyring.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/1password-archive-keyring.gpg] https://downloads.1password.com/linux/debian/$(dpkg --print-architecture) stable main" \
    > /etc/apt/sources.list.d/1password.list
apt-get update
apt-get install -y --no-install-recommends 1password-cli

rm -rf /var/lib/apt/lists/*

# Allow vscode to re-run the firewall script without a password prompt
# (agent-entry.sh and the cron unit both invoke it via sudo).
echo 'vscode ALL=(root) NOPASSWD: /workspace/.devcontainer/stoker/init-firewall.sh' \
    > /etc/sudoers.d/stoker-firewall
chmod 0440 /etc/sudoers.d/stoker-firewall
