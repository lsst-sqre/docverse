# Project-only dev container base for docverse: Python 3.13 (the repo is
# requires-python >=3.13) + uv. No firewall / sudo / cron / gh / 1Password and
# no commit-signing config — those are the stoker-sandbox devcontainer
# Feature's / package's job in the AFK sandbox; humans don't need them. Kept
# COPY-free so the human build and the cloned-volume sandbox build are
# identical.
# syntax=docker/dockerfile:1.7
FROM mcr.microsoft.com/devcontainers/python:1-3.13-bookworm

ENV UV_LINK_MODE=copy \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# uv (Astral) — the project's package manager (uv.lock).
RUN curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh

WORKDIR /workspace
