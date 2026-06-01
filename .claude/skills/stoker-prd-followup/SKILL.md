---
name: stoker-prd-followup
description: Add follow-up `prd-task` issues to an existing PRD so the stoker AFK loop picks them up and appends commits to the open PR, propagating the Jira Key/URL and commenting back on Jira. Use for review-derived fix-ups on an open PR, or for ad-hoc adjust/refactor ideas that should feed back into the AFK loop instead of being done interactively — in a Rubin stoker-installed repo.
---
<!-- stoker-managed: skills:.claude/skills/stoker-prd-followup/SKILL.md:7c8eeef82e2b61d5 -->

# stoker-prd-followup — append tasks to an existing PRD

Add new `prd-task` issues to an existing PRD. The new issues are
structurally identical to those produced by `stoker-prd-to-issues`, so
`stoker shortlist` picks them up with zero changes. Each inherits
the parent PRD's branch (when the PRD uses a single-branch strategy),
so `stoker-create-pr` appends `Closes #N` to the existing open PR instead of
opening a new one.

Use this skill for two input sources, one unified flow:

- **`review`** — follow-up work derived from a review pass on a PR
  that is still open. The user pastes curated findings.
- **`followup`** — ad-hoc refactor/adjust ideas the user has been
  accumulating against this PRD.

The skill does **not** parse review output or fetch PR comments. The
user pastes the curated list themselves.

This is the Rubin variant of the default `stoker-prd-followup`. It
keeps the default's inherited-field detection and sub-issue /
blocker-edge plumbing verbatim, and layers the Rubin workflow on top:
`### Jira Key` / `### Jira URL` propagation onto each follow-up and a
Jira linkback comment.

## Jira via the Atlassian MCP

The Jira **read** happens off the PRD's / existing tasks' `### Jira Key`
/ `### Jira URL` body fields (no MCP needed). The Jira **comment** at
the end uses the Atlassian MCP server
(`mcp__atlassian__jira_add_comment`), which the user configures (see
the repo's AGENTS.md). **Degrade gracefully:** if the MCP is
unavailable, print the comment text so the user can paste it into
Jira. Skip every Jira touch for a ticket-less PRD.

## Workflow

### 1. Identify the parent PRD

Ask for the PRD GitHub issue number. Fetch it:

```
gh issue view <prd_number> --json number,title,body
```

Detect the repo:

```
gh repo view --json nameWithOwner -q .nameWithOwner
```

Extract the `### Jira Key` and `### Jira URL` from the PRD body (also
recoverable from any existing `prd-task`'s body in step 2). An
empty/absent Jira Key means a ticket-less PRD — skip every Jira touch.

### 2. Detect inherited fields from existing tasks

List existing `prd-task` issues under this PRD (open and closed —
both states matter for `Task Order` arithmetic and branch
detection):

```
gh issue list --repo <owner/repo> --label prd-task --state all --limit 200 \
  --json number,title,body
```

Filter to entries whose body has `### Parent PRD` resolving to the
PRD's `#N`. From those, extract:

- **Branch** — the value of the `Branch` field. All tasks under a
  single-branch PRD share it. If branches diverge across tasks, the
  PRD is using task-specific branches; ask the user which branch each
  new follow-up should target (or whether each follow-up should get
  its own new branch, named per the Rubin convention:
  `tickets/DM-XXXXX-<short-desc>`, or `u/<login>/<short-desc>` for a
  ticket-less PRD).
- **Max Task Order** — largest numeric `Task Order` across existing
  tasks. New follow-ups start at `max + 1`, incrementing per item.
- **Jira Key / Jira URL** — confirm against the PRD's values from
  step 1.

If no existing `prd-task` issues are found, abort with an
explanation — this skill is for adding to an existing breakdown,
not for the first pass (use `stoker-prd-to-issues` for that).

### 3. Gather the source

Ask:

- Source type: `review` or `followup`.
- If `review`: the PR number the review was on. This populates the
  follow-up's body with `Origin: Review of PR #<n>`. Assume the PR is
  still open (this skill is for append-via-AFK, not for re-opening
  closed PRs).
- If `followup`: no extra args. Origin reads `Follow-up suggestion`.

### 4. Collect picks

Prompt the user to paste the curated list. Parse robustly — accept
numbered lists, bulleted lists, mixed formats, and items ranging from
one-liners to paragraphs.

Echo a numbered list back for confirmation. Support edits in natural
language ("drop #3", "merge #1 and #2", "split #4 into two").

### 5. Per-item planning

For each approved item:

- Launch an `Explore` subagent scoped to the files named or implied
  by the item. Confirm the finding is still valid in the current
  tree and identify existing patterns to preserve.
- Interview the user on:
  - **Acceptance criteria** — what defines "done"?
  - **Approach sketch** — the shape of the change, not layer-by-layer
    detail.
  - **Blocked by** — does this item depend on another in the batch,
    or on an existing open issue?
  - **Parallel with** — informational peers.
- Iterate on granularity — merge tightly-coupled items, split
  oversized ones.

Keep interviewing until you have a shared understanding of each
item.

### 6. Confirmation table

Present the final breakdown:

| # | Title | Blocked by | Parallel with | Task Order | Branch |
|---|-------|------------|---------------|------------|--------|

The `Type` column is omitted — every follow-up is `AFK` by
construction, to keep the items loop-eligible. (For a `MANUAL`
follow-up, file a regular issue and exclude it from this batch.)

Iterate with the user until they approve.

### 7. Create issues (and link sub-issue + blocker edges)

Ensure the `prd-task` label exists **before** creating any issues —
the shortlist builder filters on this label, so a task without it
is invisible to the AFK loop:

```
gh label create prd-task --description "Implementation task from a PRD" --color "1d76db" --force
```

`--force` makes the command idempotent.

Create issues in dependency order so real issue numbers populate
`### Blocked by` fields. Use the same body shape as
`stoker-prd-to-issues` (including the inherited `### Jira Key` /
`### Jira URL`), plus an `### Origin` section documenting where the
follow-up came from. The `### Origin` section is extra context —
`stoker.github.issue` ignores fields it doesn't recognize.

Each task gets a **coupled sequence** of commands: a
`gh issue create`, followed *immediately* by a sub-issue link
call, then one dependency-add call per blocker. Run the whole
sequence before moving on to the next item — the sub-issue edge
populates the parent PRD's sub-issues panel and what
`stoker run --prd <N>` filters on; the blocker edges are the
native `dependencies/blocked_by` relationship the shortlist
enumerates a candidate task's blockers from.

Keep writing the `### Parent PRD\n\n#<prd_number>` section in the
task body. This is a deliberate **dual-write**: the body field
is the runtime fallback path the shortlist consults when the
sub-issue edge is missing, so a task whose link call later
failed (or is mid-migration) still gets picked up by the loop.

Likewise keep writing the `### Blocked by\n\n#<n>, #<m>` section
in the task body — the **same dual-write contract** applies to
blocker edges. The body field is the runtime fallback the
shortlist's blocker enumeration falls back to when the native
`dependencies/blocked_by` edge is missing (a follow-up that
pre-dates the migration, or whose dependency-add call failed).

Propagate the parent PRD's `### Jira Key` / `### Jira URL` onto
every follow-up body verbatim (empty for a ticket-less PRD), so
`stoker-create-pr` keeps the `Jira:` reference correct as the
follow-up commits append to the open PR.

Each follow-up is assigned to **you** (the operator) via
`--assignee @me` in the `gh issue create` below. This is
load-bearing: the loop's self-scoped shortlist
(`[selection].scope_to_self`, default on) only surfaces tasks the
operator is an assignee of, so an unassigned follow-up would never
be picked up. If the user explicitly asks to file the follow-ups
**on behalf of** someone else, replace `@me` with that person's
GitHub login (`--assignee <login>`); otherwise always default to
`@me`.

```
TASK_URL=$(gh issue create --repo <owner/repo> --title "<title>" --label "prd-task" --assignee @me --body "$(cat <<'EOF'
<body, including `### Jira Key`, `### Jira URL`,
`### Parent PRD\n\n#<prd_number>`, and
`### Blocked by\n\n#<n>, #<m>` verbatim>
EOF
)")
TASK_NUM="${TASK_URL##*/}"

# Resolve the new issue's integer database id — the sub-issues
# POST endpoint refuses repo-scoped issue numbers.
TASK_DB_ID=$(gh api --jq .id "/repos/<owner>/<name>/issues/${TASK_NUM}")

# Link the new task as a sub-issue under the parent PRD.
if gh api --method POST "/repos/<owner>/<name>/issues/<prd_number>/sub_issues" \
       -F "sub_issue_id=${TASK_DB_ID}" >/dev/null; then
    LINKED+=("#${TASK_NUM}")
else
    UNLINKED+=("#${TASK_NUM}")
    echo "WARN: failed to link #${TASK_NUM} under PRD #<prd_number>; \
continuing batch. Repair with: stoker prd link-tasks <prd_number>" >&2
fi

# Link each blocker edge via GitHub's native issue-dependencies
# API. Iterate this follow-up's `### Blocked by` list —
# `<blockers>` is the space-separated blocker issue numbers (empty
# for a follow-up with no blockers, in which case this is a no-op).
for BLOCKER in <blockers>; do
    # Resolve the blocker's integer database id — the
    # dependencies/blocked_by POST refuses repo-scoped numbers.
    BLOCKER_DB_ID=$(gh api --jq .id "/repos/<owner>/<name>/issues/${BLOCKER}")
    if gh api --method POST \
           "/repos/<owner>/<name>/issues/${TASK_NUM}/dependencies/blocked_by" \
           -F "issue_id=${BLOCKER_DB_ID}" >/dev/null; then
        LINKED_BLOCKERS+=("#${TASK_NUM} → #${BLOCKER}")
    else
        UNLINKED_BLOCKERS+=("#${TASK_NUM} → #${BLOCKER}")
        echo "WARN: failed to link blocker #${BLOCKER} for #${TASK_NUM}; \
continuing batch. Repair with: stoker prd link-blockers <prd_number>" >&2
    fi
done
```

**Link-failure handling.** A non-zero exit from the
`/sub_issues` POST *or* a `dependencies/blocked_by` POST is a
*warning, not a batch-abort*. **Continue creating the remaining
tasks and edges**; the warning goes to stderr so the operator can
see it, and the failed sub-issue link / blocker edge gets named in
the end-of-run summary (step 8 below) alongside a
`stoker prd link-tasks <prd>` / `stoker prd link-blockers <prd>`
retry hint. Because the body still carries `### Parent PRD: #<prd>`
and `### Blocked by: #<n>` (the dual-write fallbacks), the unlinked
follow-up is still loop-eligible and its blockers are still
resolved via the shortlist's body-field paths.

**Never drop the `--label "prd-task"` flag.** If `gh issue create`
fails with `could not add label: 'prd-task' not found`, the label
step above was skipped or the label was deleted — re-run
`gh label create prd-task ... --force` and retry the issue creation
**with the `--label` flag intact**.

After creating all issues, verify the labels stuck:

```
gh issue list --repo <owner/repo> --label prd-task --state open --limit 50 --json number,title
```

Every issue you just created should appear. If any are missing,
backfill with `gh issue edit <n> --add-label prd-task`.

Do NOT close or modify the parent PRD issue or any existing
`prd-task` issues.

### 8. End-of-run summary

Print one summary block to stdout summarizing the batch. The
loop's stdout is the operator's only signal that any follow-up
needs manual link repair; without this block, sub-issue and
blocker-edge link failures get buried under stderr noise.

If every sub-issue link and blocker edge landed cleanly:

```
Added N follow-up tasks under PRD #<prd_number>. All linked as
sub-issues and all blocker edges linked.
```

If any sub-issue link or blocker edge failed, list the affected
groups and end with the retry hints:

```
Added N follow-up tasks under PRD #<prd_number>.
Linked as sub-issues: #<a>, #<b>, ...
Unlinked sub-issues (link API failed): #<c>
Unlinked blocker edges (dependency-add failed): #<c> → #<B>, ...
Retry with: stoker prd link-tasks <prd_number>
Retry with: stoker prd link-blockers <prd_number>
```

Emit only the lines that apply — a batch with clean sub-issue
links but a failed blocker edge prints just the
`Unlinked blocker edges` line and the `stoker prd link-blockers`
hint.

Both `stoker prd link-tasks` and `stoker prd link-blockers` are
idempotent — running either after a partial-success batch links
exactly the unlinked edges and reports `linked=K already-linked=M`.
The unlinked tasks remain loop-eligible (and their blockers stay
resolved) via the body-field fallbacks in the meantime, so the
operator can repair on their own cadence.

### 9. Linkback comments

**GitHub.** Post one comment on the parent PRD summarizing the batch:

```
gh issue comment <prd_number> --repo <owner/repo> --body "$(cat <<'EOF'
Added <N> follow-up tasks from <origin>:

| # | Task | Blocked by |
|---|------|------------|
| 1 | [<title>](<url>) | None |
| 2 | [<title>](<url>) | #<n> |
EOF
)"
```

Where `<origin>` is `review of PR #<n>` or `a follow-up suggestion
batch`.

**Jira.** When the PRD has a Jira key, mirror the GitHub comment to
the Jira ticket. Include a `Type` column for parity with the original
breakdown comment, though the value is always `AFK`:

```
mcp__atlassian__jira_add_comment(
  issue_key="DM-XXXXX",
  body="Follow-up tasks added to PRD <prd_issue_url> from <origin>:\n\n| # | Task | Type | Blocked by |\n|---|------|------|------------|\n| 1 | [<title>](<url>) | AFK | None |\n| 2 | [<title>](<url>) | AFK | #<n> |"
)
```

If the Atlassian MCP is unavailable, print the comment text so the
user can paste it into Jira. Skip the Jira comment entirely for a
ticket-less PRD.

## Task body template

```markdown
### Branch

<inherited branch>

### Type

AFK

### Jira Key

DM-XXXXX

### Jira URL

https://rubinobs.atlassian.net/browse/DM-XXXXX

### Blocked by

#<n>

### Parent PRD

#<prd_number>

### Task Order

<max_existing + i>

### Parallel with

#<n>

### Origin

Review of PR #<pr_number>   <!-- or: Follow-up suggestion -->

### Scope

A concise description of the change. Reference specific files or
sections of the parent PRD rather than duplicating content.

### Acceptance criteria

- [ ] Criterion 1
- [ ] Criterion 2
```

## Example output

```markdown
### Branch

tickets/DM-45678-run-harness-flag

### Type

AFK

### Jira Key

DM-45678

### Jira URL

https://rubinobs.atlassian.net/browse/DM-45678

### Blocked by

#45

### Parent PRD

#41

### Task Order

5

### Parallel with

### Origin

Review of PR #50

### Scope

Tighten the `--harness` validation error to name the registered
harnesses, not just say "unknown". The reviewer flagged the bare
"unknown harness 'foo'" message as unhelpful when the user typo'd
`coddex`. Reference PR #50 review thread on
`src/stoker/cli/run.py:128`.

### Acceptance criteria

- [ ] Error includes the list of registered harnesses, sorted.
- [ ] Existing exit code unchanged.
```

## Notes

- The skill never touches `stoker-implement`, `stoker-create-pr`, `stoker-prd`, or
  `stoker-prd-to-issues`. Everything downstream runs unchanged.
- The `### Origin` section is extra body content beyond what the
  shortlist builder parses; it is safely ignored.
- If a follow-up depends on a not-yet-merged change in the same PR,
  record it as a `Blocked by` reference to the task issue that will
  deliver it (not to the PR). The loop gates on closed blocker
  issues, not on merge state.
- Jira touches are interactive and host-side only — the sandbox AFK
  loop never calls Jira. It reads the Jira key/URL from the GitHub
  task metadata propagated above.
