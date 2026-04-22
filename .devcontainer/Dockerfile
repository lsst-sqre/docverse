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
    vim \
    iptables \
    ipset \
    iproute2 \
    dnsutils \
    aggregate \
    cron \
    && rm -rf /var/lib/apt/lists/*

# uv - baked in, no runtime network call to astral.sh
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Claude Code - always use latest; old versions can stop working
RUN npm install -g @anthropic-ai/claude-code@latest

# Git signing defaults (identity + signing key path set per-session by agent-entry)
RUN git config --system gpg.format ssh \
    && git config --system commit.gpgsign true \
    && git config --system tag.gpgsign true \
    && git config --system core.editor vim

# Firewall script
COPY init-firewall.sh /usr/local/bin/init-firewall.sh
RUN chmod +x /usr/local/bin/init-firewall.sh

# Periodic firewall refresh so CDN-backed domains don't drift during long runs
COPY firewall-refresh.cron /etc/cron.d/docverse-firewall
RUN chmod 0644 /etc/cron.d/docverse-firewall

# Agent entry point
COPY agent-entry.sh /usr/local/bin/agent-entry
RUN chmod +x /usr/local/bin/agent-entry

# Restricted sudoers: node can run exactly the argv patterns used by the lifecycle scripts
RUN { \
    echo "node ALL=(root) NOPASSWD: /usr/local/bin/init-firewall.sh"; \
    echo "node ALL=(root) NOPASSWD: /usr/sbin/cron"; \
    echo "node ALL=(root) NOPASSWD: /bin/mkdir -p /run/docverse-sandbox"; \
    echo "node ALL=(root) NOPASSWD: /bin/chown node\\:node /run/docverse-sandbox"; \
    echo "node ALL=(root) NOPASSWD: /bin/chown node\\:node /home/node/.cache"; \
    echo "node ALL=(root) NOPASSWD: /bin/chown -R node\\:node /workspace /home/node/.claude /commandhistory /home/node/.cache/uv"; \
    } > /etc/sudoers.d/firewall \
    && chmod 0440 /etc/sudoers.d/firewall

# Oh-my-zsh for the node user + sane default .zshrc
RUN chsh -s /usr/bin/zsh node \
    && runuser -u node -- sh -c \
       'RUNZSH=no CHSH=no KEEP_ZSHRC=yes sh -c \
        "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"'
COPY --chown=node:node zshrc /home/node/.zshrc

USER node
