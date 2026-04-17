#!/bin/bash
# Deny Bash commands whose primary invocation is bare `python` or `python3`.
# Purpose: nudge toward `uv run python` so the command uses the uv-managed
# project virtualenv instead of whatever `python` happens to be on PATH.
#
# Scope: we intentionally only check the leading token of the command (after
# stripping whitespace). We do NOT try to parse chained commands, command
# substitution, env wrappers, absolute paths, or quoted strings. That kept
# biting us with false positives on commit messages, heredocs, and grep/echo
# commands that merely mention `python`. A determined bypass (e.g.
# `/usr/bin/python`, `ls && python`) is out of scope — this is a guardrail
# against the common accidental case, not a sandbox.

set -euo pipefail

cmd=$(jq -r '.tool_input.command // empty')
[ -z "$cmd" ] && exit 0

# Use bash =~ so ^/$ anchor the WHOLE command string, not each line. grep -E
# would match any heredoc body line that happens to start with `python`,
# which trips on commit messages that mention python.
if [[ "$cmd" =~ ^[[:space:]]*python3?([[:space:]]|$) ]]; then
  jq -n '{
    hookSpecificOutput: {
      hookEventName: "PreToolUse",
      permissionDecision: "deny",
      permissionDecisionReason: "Blocked bare python invocation. Use \"uv run python\" (or \"uv run --only-group=<group> python\") so the command runs in the uv-managed project virtualenv."
    }
  }'
fi
