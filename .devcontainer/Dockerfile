# stoker-managed: sandbox:Dockerfile:e18e124ac75393fe
# syntax=docker/dockerfile:1.7
FROM mcr.microsoft.com/devcontainers/python:1-3.12-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_LINK_MODE=copy

# Core toolchain: gh, jq, git, build-essential. Node + npm are installed
# from the base image's Node feature in devcontainer.json. Docker (for
# Docker-in-Docker testcontainers) is installed by the docker-in-docker
# devcontainer feature, not here.
# Drop the base image's stale Yarn apt source — Yarn's signing key rotates
# faster than the base image ships, and stoker doesn't use Yarn.
RUN rm -f /etc/apt/sources.list.d/yarn.list \
         /usr/share/keyrings/yarnkey.gpg \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        cron \
        curl \
        git \
        gnupg \
        iproute2 \
        iptables \
        ipset \
        jq \
        less \
        lsb-release \
        sudo \
        unzip \
    && curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
        | gpg --dearmor -o /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
        > /etc/apt/sources.list.d/github-cli.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends gh \
    && rm -rf /var/lib/apt/lists/*

# Allow vscode to re-run the firewall script without a password prompt
# (agent-entry.sh and the cron unit both invoke it via sudo).
RUN echo 'vscode ALL=(root) NOPASSWD: /workspace/.devcontainer/init-firewall.sh' \
        > /etc/sudoers.d/stoker-firewall \
    && chmod 0440 /etc/sudoers.d/stoker-firewall

# uv (Astral). Pinned at install time; bump on profile upgrades.
RUN curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh

# 1Password CLI is needed for the host-side `op` secret backend handoff
# during `stoker shell`. Light footprint when unused.
RUN curl -fsSL https://downloads.1password.com/linux/keys/1password.asc \
        | gpg --dearmor -o /usr/share/keyrings/1password-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/1password-archive-keyring.gpg] https://downloads.1password.com/linux/debian/$(dpkg --print-architecture) stable main" \
        > /etc/apt/sources.list.d/1password.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends 1password-cli \
    && rm -rf /var/lib/apt/lists/*

# SQuaRE signs every commit with SSH. Bake the system-level git signing
# posture (format + gpgsign defaults) so the sandbox never produces an
# unsigned commit. The stoker package's agent-entry.sh layers the actual
# per-session key paths on top at the --global level (user.signingkey,
# gpg.ssh.allowedSignersFile) from the injected signing_key_* secrets,
# which the Rubin profile marks REQUIRED.
RUN git config --system gpg.format ssh \
    && git config --system commit.gpgsign true \
    && git config --system tag.gpgsign true

# claude-code is installed via npm by the devcontainer postCreateCommand
# rather than baked here, so the image stays slim and the npm install
# happens against the user's home volume.

WORKDIR /workspace
