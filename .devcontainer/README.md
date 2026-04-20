# Docverse Sandbox

Isolated devcontainer for running Claude Code autonomously. No bind-mounts — the repo is cloned inside a named volume to avoid Colima virtiofs issues.

## Prerequisites

- **Colima** — choose one:
  - **Shared** (use existing Colima instance):
    ```bash
    colima start --cpu 6 --memory 16 --disk 100 --vm-type vz --vz-rosetta --mount-type virtiofs
    ```
  - **Dedicated** (recommended — isolates sandbox into its own VM):
    ```bash
    colima start --profile agent-sandbox --cpu 6 --memory 16 --disk 100 --vm-type vz --vz-rosetta
    ```
    This creates a Docker context named `agent-sandbox`. Set `DOCVERSE_SANDBOX_DOCKER_CONTEXT=agent-sandbox` so `run.sh` and `stop.sh` target the right daemon.
- **devcontainer CLI**: `npm install -g @devcontainers/cli`
- **1Password CLI** (`op`): authenticated on host
- **Git identity**: `git config --global user.name` and `user.email` must be set

## Environment variables

Set these in your shell (or via direnv `.env`):

| Variable | Description |
|---|---|
| `DOCVERSE_SANDBOX_GH_TOKEN_OP` | 1Password URI for GitHub token |
| `DOCVERSE_SANDBOX_SIGNING_KEY_PRIVATE_OP` | 1Password URI for SSH signing key (private) |
| `DOCVERSE_SANDBOX_SIGNING_KEY_PUBLIC_OP` | 1Password URI for SSH signing key (public) |
| `DOCVERSE_SANDBOX_ANTHROPIC_KEY_OP` | 1Password URI for Anthropic API key (only with `--api`) |
| `DOCVERSE_SANDBOX_DOCKER_CONTEXT` | Docker context to use (e.g. `agent-sandbox`) |
| `DOCVERSE_SANDBOX_OP_ACCOUNT` | 1Password account (default: `my.1password.com`) |

## Usage

### First-time setup

Build the container and log in to Claude (Max OAuth):

```bash
# With dedicated Colima profile:
DOCVERSE_SANDBOX_DOCKER_CONTEXT=agent-sandbox .devcontainer/run.sh --rebuild --login
# Or with --docker-context flag:
.devcontainer/run.sh --docker-context agent-sandbox --rebuild --login
# Inside the container:
claude    # Complete OAuth flow
exit
```

### Run a prompt

```bash
# From a file
.devcontainer/run.sh prompt.md

# From stdin
echo "Run the test suite and fix any failures" | .devcontainer/run.sh

# With API key instead of Max OAuth
.devcontainer/run.sh --api prompt.md
```

### Interactive session

```bash
.devcontainer/run.sh --login
```

The login shell is zsh with oh-my-zsh (robbyrussell theme) preconfigured.
Command history is persisted to the `docverse-cmdhistory` named volume, so
it survives `--rebuild`.

### Stop the container

```bash
DOCVERSE_SANDBOX_DOCKER_CONTEXT=agent-sandbox .devcontainer/stop.sh
```

Named volumes persist across stop/start cycles.

### Rebuild (pick up latest Claude Code, etc.)

```bash
.devcontainer/run.sh --rebuild --login
```

This destroys the container but preserves all named volumes (workspace, claude auth, uv cache).

## Firewall

The container runs an egress firewall that only allows connections to:

- Anthropic (API, console, telemetry)
- GitHub (web, API, git, packages)
- PyPI + npm
- Docker Hub (for testcontainers)
- Private networks (for DinD)

To refresh stale DNS (if domains resolve to new IPs):

```bash
devcontainer exec --workspace-folder /path/to/docverse \
    sudo /usr/local/bin/init-firewall.sh
```

## Named volumes

| Volume | Mount point | Purpose |
|---|---|---|
| `docverse-workspace` | `/workspace` | Cloned repo |
| `docverse-claude-home` | `/home/node/.claude` | Claude auth + config |
| `docverse-cmdhistory` | `/commandhistory` | Shell history |
| `docverse-uv-cache` | `/home/node/.cache/uv` | Python package cache |
| (DinD feature-managed) | `/var/lib/docker` | Docker-in-Docker storage |

## Ralph loop (AFK autonomous mode)

For longer unattended runs that work through a backlog of `prd-task` GitHub
issues, use the Ralph loop driver in [`ralph/`](../ralph/README.md). It reuses
the same 1Password secret loading and container lifecycle, so the env vars
above apply.

## Testing inside the container

```bash
cd /workspace/docverse
TC_HOST=localhost TESTCONTAINERS_RYUK_DISABLED=true uv run --only-group=nox nox -s test
```
