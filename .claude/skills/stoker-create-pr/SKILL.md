---
name: stoker-create-pr
description: Create or update a pull request with a Summary / Validation steps / References body, including `Closes #<task_issue>`, `PRD: #<parent_prd>`, and `Jira: <url>` trailers, and a `DM-XXXXX:`-prefixed title for ticket branches. Use when invoked from `stoker-implement`'s PR step or when the user asks to create a PR, open a pull request, or update a PR body — and you are working in a Rubin stoker-installed repo.
---
<!-- stoker-managed: skills:.claude/skills/stoker-create-pr/SKILL.md:7d81a5cae898a687 -->

# stoker-create-pr — the Rubin PR-creation primitive

Create a pull request (or update one that already exists on this
branch) using a fixed body template that the AFK loop and humans
both produce identically. This skill is the single source of truth
for the Rubin profile's PR shape: title format, body sections, and
the `Closes #N` / `PRD: #N` / `Jira: <url>` trailer conventions.

`stoker-implement` delegates here for its PR step. Humans invoke it
directly for hand-driven PRs.

This is the Rubin variant of the default `stoker-create-pr`. It keeps
the default's create-vs-update logic and multi-task `Closes #N`
regeneration verbatim, and layers two Rubin conventions on top: a
`DM-XXXXX:`-prefixed title for `tickets/DM-XXXXX...` branches, and a
`Jira: <url>` line in References.

## Workflow

### 1. Gather context

Collect these values. Use the first available source for each:

- **Task issue** — the `prd-task` whose work this PR delivers. Sources
  in order: current conversation context (e.g. the implement-phase
  prompt) > auto-detect by branch (see below) > ask the user
  (optional — solo PRs without a tracked task are allowed).
- **Parent PRD** — the task issue's `### Parent PRD` field, when the
  task is part of a PRD breakdown. Optional.
- **Branch** — the current branch (`git branch --show-current`).
- **Jira key** — sources in order: current conversation context >
  the branch name when it matches `tickets/<KEY>...` (the `<KEY>`
  segment, e.g. `tickets/DM-12345-add-foo` → `DM-12345`) > the task
  issue's `### Jira Key` field > none. Optional — `u/<login>/<desc>`
  branches and ticket-less PRDs have no Jira key.
- **Jira URL** — sources in order: the task issue's `### Jira URL`
  field > constructed as `https://rubinobs.atlassian.net/browse/<key>`
  when a Jira key is known > none.

#### Auto-detecting the task issue

If no task is in context, search by branch:

```
gh issue list --label prd-task --state open --json number,title,body --limit 50
```

Find the issue whose `### Branch` field matches the current branch
name. Extract `### Parent PRD`, `### Jira Key`, and `### Jira URL`
from that issue's body.

### 2. Push the branch

```
git status -sb
```

If the branch has no remote tracking branch or has unpushed commits:

```
git push -u origin HEAD
```

If the push fails (non-fast-forward, permission denied, network),
**do not retry or rebase from this skill**. Surface the failure to
the caller (`stoker-implement` routes to the stuck path; humans get an
error message).

### 3. Detect existing PR

```
gh pr list --head <branch> --json number,title,body,state,baseRefName --limit 1
```

Three cases:

- **No existing PR** → create a new one (step 4).
- **Existing PR, state OPEN** → regenerate the body from the branch's
  cumulative scope (step 5).
- **Existing PR, state CLOSED or MERGED** → skip PR handling. Log
  the fact and return.

### 4. Create a new PR

**Title.**

- When a **Jira key** is known, prefix the title: `DM-XXXXX: <concise
  description>`. The description is a concise summary of what this PR
  accomplishes; keep the whole title under 70 characters.
- With **no Jira key** (a `u/<login>/<desc>` branch or ticket-less
  work), use a plain concise descriptive title under 70 characters.

**Body.** Use the template below verbatim.

```
gh pr create --title "<title>" --body "$(cat <<'EOF'
<body>
EOF
)"
```

### 5. Update an existing OPEN PR

Regenerate the body from the branch's full scope so Summary,
Validation steps, and References reflect every task that has landed,
not just the first.

1. Enumerate every commit since the PR's base. Use `baseRefName` from
   the `gh pr list` JSON; do not hardcode `origin/main`:

   ```
   git log origin/<baseRefName>..HEAD --format='%H%n%s%n%b---'
   ```

2. Take the union of `Closes #N` trailers from those commits, the
   `Closes #N` lines already in the existing PR body's References
   section, and the currently-picked task. (`Refs #N` trailers used
   by stuck WIP commits naturally don't surface here, so stuck work
   never leaks into References or Summary.)

3. For each unique `N`, fetch the issue:

   ```
   gh issue view <N> --json number,title,body,state
   ```

   Pull manual-QA bullets from the issue's `### Acceptance criteria`
   field (if present) for the Validation steps section. Also re-read
   the `### Jira Key` / `### Jira URL` fields here so the `Jira:`
   reference stays correct after the first task.

4. Compose a fresh body using the template:

   - **Summary** — 1–3 bullets (up to 5 if scope is genuinely
     diverse). Collapse multiple commits advancing the same slice
     into one bullet.
   - **Validation steps** — union of manual-QA items from every
     referenced task. Drop bullets CI already covers and bullets
     superseded by later commits.
   - **References** — one `Closes #N` per referenced task, sorted
     ascending; then `PRD: #<parent_prd>` (omit if none); then
     `Jira: <url>` (omit if no Jira key). Sorted + deduped is
     idempotent by construction.

5. Apply:

   ```
   gh pr edit <pr_number> --body "$(cat <<'EOF'
   <body>
   EOF
   )"
   ```

   Leave the existing PR **title** unchanged on update — it already
   carries the `DM-XXXXX:` prefix from creation.

### 6. Report the URL

After create or edit, print the PR URL so the caller can surface it
to the user (or the comment-and-close steps in `stoker-implement`).

## PR body template

```markdown
## Summary

<1–3 bullets describing what this PR does and why, in user / PRD-
level language. Draw ground truth from the commits on this branch;
draw user-facing framing from the task issue title.>

## Validation steps

<Bulleted manual-QA checklist for verifying the change. Omit anything
CI runs automatically (no linting, type-checking, or automated test
suite items). Only manual verification.>

## References

- Closes #<task_issue>
- PRD: #<parent_prd>   <!-- omit if no parent PRD -->
- Jira: <jira_url>     <!-- omit if no Jira key -->
```

Rules for the References section:

- Use `Closes #<task_issue>` so the task auto-closes on merge.
- Omit the `PRD:` line if there is no parent PRD.
- Omit the `Closes #` line if there is no task issue (a hand-driven
  PR with no tracked task).
- Omit the `Jira:` line if there is no Jira key.
- For multi-task branches, include one `Closes #N` per referenced
  task, sorted ascending. There is a single `Jira:` line per PR (all
  tasks on a branch share one ticket).

## Example output

For a branch `tickets/DM-45678-run-harness-flag`:

```markdown
## Summary

- Add `--harness` flag to `stoker run` so an iteration can be locked
  to a single harness regardless of profile defaults.
- Validate the harness name against the registered-harness registry
  and fail fast on unknown values.

## Validation steps

- Run `stoker run --harness codex 1` against a fixture issue and
  confirm the implement+review phases both invoke codex.
- Run `stoker run --harness foo 1` and confirm it exits with a
  non-zero code before any container work starts.
- Run `stoker run 1` (no flag) and confirm behavior is unchanged.

## References

- Closes #45
- PRD: #41
- Jira: https://rubinobs.atlassian.net/browse/DM-45678
```

The PR title for that branch would be
`DM-45678: Add --harness flag to stoker run`.

## Notes

- This skill is the PR-creation primitive shared by `stoker-implement`
  and direct human use. Keep the body shape stable — the AFK loop's
  multi-task PR-update logic depends on the exact section names and
  trailer format above.
- This skill never calls Jira. The Jira URL comes entirely from the
  task issue's `### Jira URL` field (written interactively by
  `stoker-prd-to-issues`) or is constructed from the key in the branch
  name. The sandbox AFK loop is GitHub-only — it has no Jira creds,
  MCP, or firewall egress to Jira.
- No `Co-authored-by:` trailer (signing, if configured, is
  self-identifying).
- No "Files changed" section — `git show` and the GitHub UI cover
  that.
