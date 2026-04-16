FROM node:20-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    gh \
    curl \
    wget \
    sudo \
    ripgrep \
    fd-find \
    jq \
    tree \
    build-essential \
    python3 \
    python3-venv \
    fzf \
    zsh \
    iptables \
    ipset \
    iproute2 \
    dnsutils \
    aggregate \
    && rm -rf /var/lib/apt/lists/*

# uv - baked in, no runtime network call to astral.sh
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Claude Code - always use latest; old versions can stop working
RUN npm install -g @anthropic-ai/claude-code@latest

# Git signing defaults (identity + signing key path set per-session by agent-entry)
RUN git config --system gpg.format ssh \
    && git config --system commit.gpgsign true \
    && git config --system tag.gpgsign true

# Firewall script
COPY init-firewall.sh /usr/local/bin/init-firewall.sh
RUN chmod +x /usr/local/bin/init-firewall.sh

# Agent entry point
COPY agent-entry.sh /usr/local/bin/agent-entry
RUN chmod +x /usr/local/bin/agent-entry

# Restricted sudoers: node can run firewall script and chown only
RUN echo "node ALL=(root) NOPASSWD: /usr/local/bin/init-firewall.sh" > /etc/sudoers.d/firewall \
    && echo "node ALL=(root) NOPASSWD: /bin/chown" >> /etc/sudoers.d/firewall \
    && echo "node ALL=(root) NOPASSWD: /bin/mkdir" >> /etc/sudoers.d/firewall \
    && chmod 0440 /etc/sudoers.d/firewall

USER node
