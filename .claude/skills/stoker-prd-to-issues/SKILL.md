---
name: stoker-prd-to-issues
description: Break a PRD GitHub issue into independently-grabbable `prd-task` issues using tracer-bullet vertical slices, applying Rubin branch naming and propagating the Jira Key/URL onto each task, then comment back on Jira. Use when the user wants to break a PRD into tasks, convert a PRD to issues, or create implementation tickets — in a Rubin stoker-installed repo.
---
<!-- stoker-managed: skills:.claude/skills/stoker-prd-to-issues/SKILL.md:5ed1e53c37a0c61c -->

# stoker-prd-to-issues — turn a PRD into AFK-eligible task issues

Read a `prd`-labeled GitHub issue, interview the user about the
implementation breakdown, then create one `prd-task` issue per
vertical slice. Each task issue's body must match
`.github/ISSUE_TEMPLATE/prd-task.yml` so `stoker shortlist` and
`stoker run --prd <N>` pick it up cleanly.

The default for new tasks is `Type: AFK`. AFK tasks are eligible for
the loop. Use `Type: MANUAL` only when human judgement is required
(architectural call, design review, schema migration the user wants
to drive interactively); the loop skips those.

This is the Rubin variant of the default `stoker-prd-to-issues`. It
keeps the default's tracer-bullet slicing, pressure-test axes,
approval gate, and sub-issue / blocker-edge plumbing verbatim, and
layers the Rubin workflow on top: Rubin branch naming, `### Jira Key`
/ `### Jira URL` propagation onto each task, and a Jira linkback
comment.

## Jira via the Atlassian MCP

The Jira **read** happens off the PRD's `### Jira Key` / `### Jira URL`
body fields (no MCP needed). The Jira **comment** at the end uses the
Atlassian MCP server (`mcp__atlassian__jira_add_comment`), which the
user configures (see the repo's AGENTS.md). **Degrade gracefully:** if
the MCP is unavailable, print the comment text so the user can paste
it into Jira. Skip every Jira touch for a ticket-less PRD.

## Workflow

### 1. Locate the parent PRD

Ask the user for the PRD GitHub issue number. Fetch it:

```
gh issue view <prd_number> --json number,title,body
```

Extract the `### Jira Key` and `### Jira URL` from the PRD body. A
PRD with an empty/absent Jira Key is a ticket-less PRD — use
`u/<login>/<desc>` branches (step 4) and skip every Jira touch.

If the PRD is not already in your context, fetch it now.

### 2. Explore the codebase

If you have not already explored the repo for this PRD, do so. Verify
that what the PRD describes still matches the tree.

### 3. Interview the developer

Push on:

- Which layers does this PRD touch (data, service, API, client, UI, tests)?
- Are there existing patterns to follow?
- What's the riskiest slice?
- Any ordering constraints already known?
- Any slices that need human judgement before code can be written?
  (Those become `Type: MANUAL`.)

Keep interviewing until you have a shared understanding of the
implementation strategy.

### 4. Draft vertical slices

Break the PRD into **tracer-bullet** slices. Each slice is a thin
vertical cut that exercises one new behavior end-to-end through every
relevant layer (route → service → data → response, or whatever the
analogue is in this repo).

<vertical-slice-rules>
- Each slice delivers a narrow but COMPLETE path through every
  relevant layer.
- A completed slice is demoable or verifiable on its own.
- Prefer many thin slices over a few thick ones.
- Each slice maps to one task issue and (typically) one branch.
</vertical-slice-rules>

For each slice, decide:

- A short descriptive title.
- `Type` — `AFK` (loop-eligible) or `MANUAL` (human-driven). Prefer
  AFK.
- `Blocked by` — `#N` issue refs that must close first.
- `Parallel with` — informational `#N` peers.
- `Task Order` — integer ordering within the parent PRD; lower runs
  first within a tier.
- Branch name — proposed at draft time, pressure-tested per slice
  in step 5, using **Rubin branch naming** (below).

#### Rubin branch naming

- **With a Jira key** (the common case): branches use a `tickets/`
  prefix.
  - **Single-branch PRD** (all slices land on one branch / one PR):
    `tickets/DM-XXXXX`.
  - **Task-specific branches** (each slice its own branch / PR):
    `tickets/DM-XXXXX-<short-desc>`, dash-separated
    (e.g. `tickets/DM-12345-add-foo`, `tickets/DM-12345-add-bar`).
- **Ticket-less PRD** (no Jira key): use the operator's GitHub login,
  `u/<login>/<short-desc>`. Resolve the login once:

  ```
  gh api user --jq .login
  ```

The loop just checks out whatever the `### Branch` field says, so the
naming is purely a Rubin convention — but apply it consistently.

### 5. Pressure-test the slicing

Present the first-pass breakdown as a numbered table so the user
can see exactly what is being pressure-tested:

| # | Title | Type | Blocked by | Parallel with | Task Order |
|---|-------|------|------------|---------------|------------|

Then walk every slicing axis below and push on each one until either
(a) the user confirms the slices as drawn are right on that axis,
or (b) you agree on a concrete change (merge, split, retype,
rebranch, drop or add a blocker). **Silent assumptions on these
axes are how AFK runs ship two-day "slices", unrunnable issues
blocked on phantom dependencies, or "tasks" that don't actually
deliver anything observable.**

The canonical axes:

- **Too thick** — does any slice estimate > 1 working day, or
  touch more than 3 layers (data / service / API / UI / tests)?
  Thick slices hide multiple commits behind one issue and starve
  later iterations that cannot start until the giant one ships.
- **Too thin** — does any slice deliver no observable behavior
  change in isolation? A slice that only rearranges internal
  helpers cannot be demoed, rolled back, or independently
  verified — fold it into the next slice that uses it.
- **Merge candidates** — are adjacent slices tightly coupled
  enough that they will land in the same commit anyway? One issue
  with two acceptance-criteria bullets reads cleaner than two
  issues that ping-pong against each other across iterations.
- **Split candidates** — does any slice carry two distinct
  behavior changes? If the acceptance criteria fork ("X works
  for case A" AND "X works for case B"), split so one can ship
  while the other iterates.
- **Branch / PR strategy** — per-slice, does this slice share a
  branch with adjacent slices (so commits append to the same PR)
  or get its own branch (independent review)? Single-branch is
  the default the AFK loop's `stoker-implement` was designed
  around; task-specific branches are right when slices are
  independent and benefit from separate review. Decide per
  slice, not PRD-wide — a PRD can mix strategies. Branch names
  follow the Rubin convention in step 4.
- **Dependency reality** — for every recorded `Blocked by` ref,
  is the dependency real (the later slice genuinely cannot start
  until the earlier one ships) or an artifact of how the slices
  were drawn? Bogus blockers turn parallelizable work serial.

For any axis where more than one slicing decision is unresolved,
emit a single `AskUserQuestion` batch covering that axis. Batching
by axis (rather than one slice at a time) lets the user compare
slices side-by-side and surfaces patterns across the breakdown.

Worked example — suppose the first-pass breakdown for a PRD that
adds a new `stoker prune` command has three slices: (1) wire the
flag through CLI parsing, (2) implement the branch-deletion
logic, (3) add the protected-branch lookups. Slice 1 is suspected
too-thin (no observable behavior change in isolation) and slice 2
is suspected too-thick (touches CLI + service + tests in one go).
Emit one batched question per axis where decisions are unresolved:

```
AskUserQuestion(
  questions=[
    {
      "question": "Slice 1 only wires the flag — no observable
                   behavior change. Fold into slice 2, or keep
                   separate?",
      "header": "Slice 1 too thin",
      "options": [
        {"label": "Fold into slice 2", "description": "..."},
        {"label": "Keep separate",     "description": "..."},
      ],
      "multiSelect": False,
    },
    {
      "question": "Slice 2 touches CLI + service + tests in one
                   pass. Split the CLI wiring out of the service
                   work?",
      "header": "Slice 2 too thick",
      "options": [
        {"label": "Split CLI out",     "description": "..."},
        {"label": "Keep as one slice", "description": "..."},
      ],
      "multiSelect": False,
    },
  ]
)
```

When an axis has only one unresolved decision (or none), skip the
batch — a one-question batch adds no value over a plain prose
question. When an axis is fully resolved by reading the seed PRD
or the breakdown table, skip the axis entirely.

After resolving every axis, present the final table with branches
filled in (Rubin naming from step 4):

| # | Title | Type | Blocked by | Parallel with | Task Order | Branch |
|---|-------|------|------------|---------------|------------|--------|

### 6. Ensure the `prd-task` label exists

This is a separate step on purpose. The shortlist builder filters
`gh issue list --label prd-task`, so a task issue without the label
is invisible to the AFK loop. Run this **before** creating any
issues:

```
gh repo view --json nameWithOwner -q .nameWithOwner
gh label create prd-task --description "Implementation task from a PRD" --color "1d76db" --force
```

`--force` makes the create idempotent; if the label already exists
the command updates its description/color and exits 0.

### 7. Wait for explicit approval

Before any `gh issue create` runs, you must receive an explicit
user approval token. The final breakdown table is a proposal,
not a green light; the user not objecting after you present it
**does not** satisfy this gate, and **silence** never satisfies
it.

Acceptable approval tokens:

- A clear typed confirmation: `approved`, `looks good — ship it`,
  `go ahead`, `yes, create them`, or equivalent unambiguous
  affirmation. Hedged language ("I guess that works", "sure, OK")
  is **not** approval — push back and ask for a concrete
  approve / change.
- An explicit `AskUserQuestion` selection of an "Approve
  breakdown" option. Use this when the pressure-test produced
  several changes, the conversation has drifted, or you want a
  clean Yes/No on the final shape. Surface the question with the
  approve option labelled unambiguously:

```
AskUserQuestion(
  questions=[
    {
      "question": "Final breakdown above — approve and create
                   the issues, or another pass?",
      "header": "Approve breakdown",
      "options": [
        {"label": "Approve and create",
         "description": "..."},
        {"label": "Another pass on the slicing",
         "description": "..."},
      ],
      "multiSelect": False,
    },
  ]
)
```

If the user requests further changes, loop back to step 5 and
re-run the affected axis. **Do not skip this step on the second
pass either** — every breakdown that calls `gh issue create`
needs a fresh approval token after its last change.

Until the token is received, **do not run any `gh issue create`
command, even with `--dry-run` or any other flag.**

### 8. Create the issues (and link sub-issue + blocker edges)

Create issues in dependency order (blockers first) so you can
reference real issue numbers in later `### Blocked by` fields.

Each task gets a **coupled sequence** of commands: a
`gh issue create`, followed *immediately* by a sub-issue link
call, then one dependency-add call per blocker. Run the whole
sequence before moving on to the next slice — the sub-issue edge
populates the parent PRD's sub-issues panel and what
`stoker run --prd <N>` filters on; the blocker edges are the
native `dependencies/blocked_by` relationship the shortlist
enumerates a candidate task's blockers from.

Keep writing the `### Parent PRD\n\n#<prd_number>` section in the
task body. This is a deliberate **dual-write**: the body field is
the runtime fallback path the shortlist consults when the
sub-issue edge is missing, so a task whose link call later failed
(or is mid-migration) still gets picked up by the loop. Do not
drop the body field on the assumption that the edge replaces it.

Likewise keep writing the `### Blocked by\n\n#<n>, #<m>` section
in the task body — the **same dual-write contract** applies to
blocker edges. The body field is the runtime fallback the
shortlist's blocker enumeration falls back to when the native
`dependencies/blocked_by` edge is missing (a task that pre-dates
the migration, or whose dependency-add call failed). Do not drop
the body field on the assumption that the edge replaces it.

Propagate the parent PRD's `### Jira Key` / `### Jira URL` onto
every task body verbatim (empty for a ticket-less PRD). The Rubin
`stoker-create-pr` reads the task's `### Jira URL` to add the
`Jira:` reference to the PR; the sandbox loop never touches Jira
itself.

Each task is assigned to **you** (the operator) via `--assignee @me`
in the `gh issue create` below. This is load-bearing: the loop's
self-scoped shortlist (`[selection].scope_to_self`, default on) only
surfaces tasks the operator is an assignee of, so an unassigned task
would never be picked up. If the user explicitly asks to file the
tasks **on behalf of** someone else, replace `@me` with that
person's GitHub login (`--assignee <login>`); otherwise always
default to `@me`.

For each approved slice:

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
# API. Iterate this task's `### Blocked by` list — `<blockers>` is
# the space-separated blocker issue numbers (empty for a task with
# no blockers, in which case this loop is a no-op).
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
the end-of-run summary (step 9 below) alongside a
`stoker prd link-tasks <prd>` / `stoker prd link-blockers <prd>`
retry hint. Because the body still carries `### Parent PRD: #<prd>`
and `### Blocked by: #<n>` (the dual-write fallbacks), the unlinked
task is still loop-eligible and its blockers are still resolved via
the shortlist's body-field paths — it is not stranded.

**Never drop the `--label "prd-task"` flag.** If `gh issue create`
fails with a `could not add label: 'prd-task' not found` error,
Step 6 was skipped or the label was deleted out from under you —
re-run the `gh label create` command from Step 6 and retry the
issue creation **with the `--label` flag intact**. The
`labels: [prd-task]` declared in `.github/ISSUE_TEMPLATE/prd-task
.yml` only auto-applies for issues filed via the GitHub UI form,
not for issues created by `gh issue create`.

After all issues are created, sanity-check the labels stuck:

```
gh issue list --repo <owner/repo> --label prd-task --state open --limit 50 --json number,title
```

Every issue you just created should appear. If any are missing,
backfill with `gh issue edit <n> --add-label prd-task` rather than
re-creating.

Use the template below. **Section headers must match the Issue Form
field labels exactly** — `stoker.github.issue` reads
`### Field name\n\nvalue` sections and normalizes labels to
snake_case.

Do NOT close or modify the parent PRD issue.

### 9. End-of-run summary

Print one summary block to stdout summarizing the batch. The
loop's stdout is the operator's only signal that any task needs
manual link repair; without this block, sub-issue and blocker-edge
link failures get buried under stderr noise.

If every sub-issue link and blocker edge landed cleanly:

```
Created N tasks under PRD #<prd_number>. All linked as sub-issues
and all blocker edges linked.
```

If any sub-issue link or blocker edge failed, list the affected
groups and end with the retry hints:

```
Created N tasks under PRD #<prd_number>.
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
operator is free to repair on their own cadence.

### 10. Linkback comments

**GitHub.** Post one comment on the parent PRD GitHub issue
summarizing the batch:

```
gh issue comment <prd_number> --repo <owner/repo> --body "$(cat <<'EOF'
Implementation tasks created:

| # | Task | Type | Blocked by |
|---|------|------|------------|
| 1 | [<title>](<url>) | AFK | None |
| 2 | [<title>](<url>) | AFK | #1 |
EOF
)"
```

**Jira.** When the PRD has a Jira key, post the same table to the
Jira ticket so stakeholders get visibility into the planned work:

```
mcp__atlassian__jira_add_comment(
  issue_key="DM-XXXXX",
  body="Implementation tasks created from PRD <prd_issue_url>:\n\n| # | Task | Type | Blocked by |\n|---|------|------|------------|\n| 1 | [<title>](<url>) | AFK | None |\n| 2 | [<title>](<url>) | AFK | #1 |"
)
```

If the Atlassian MCP is unavailable, print the comment text so the
user can paste it into Jira. Skip the Jira comment entirely for a
ticket-less PRD.

## Task body template

```markdown
### Branch

<branch-name>

### Type

AFK

### Jira Key

DM-XXXXX

### Jira URL

https://rubinobs.atlassian.net/browse/DM-XXXXX

### Blocked by

#<n>, #<n>

### Parent PRD

#<prd_number>

### Task Order

<integer>

### Parallel with

#<n>

### Scope

A concise description of this vertical slice. Describe the end-to-end
behavior, not layer-by-layer implementation. Reference specific
sections of the parent PRD rather than duplicating content.

### Acceptance criteria

- [ ] Criterion 1
- [ ] Criterion 2
```

Rules:

- Use `Type: AFK` unless this slice genuinely needs human judgement.
- `Jira Key` / `Jira URL` are inherited from the parent PRD. Leave
  both empty for a ticket-less PRD.
- For "no blockers" leave the field empty rather than writing "None"
  — empty parses to `[]` cleanly.
- Same for `Parallel with`.
- `Parent PRD` and `Task Order` are recommended on every task; the
  AFK loop uses them for `--prd` filtering and ordered execution.
- Blocker references: GitHub issues and PRs share one numbering
  system, so PR blockers use the same `#N` form. Stoker's preflight
  treats any non-`OPEN` state (`CLOSED`, `MERGED`) as resolved.

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

#42

### Parent PRD

#41

### Task Order

2

### Parallel with

#43

### Scope

Add the `--harness` flag to `stoker run` and route the chosen value
through the iteration's phase-config resolution so every phase uses
it instead of the per-phase default. Validate the name against the
registered-harness registry; fail fast on unknown names. Reference
PRD §Scope bullet 1 for the override semantics.

### Acceptance criteria

- [ ] `stoker run --harness codex 1` selects codex for implement+review.
- [ ] Unknown harness name exits with a helpful error before any
      container work starts.
- [ ] `stoker run 1` (no flag) is unchanged.
```

## Notes

- This skill never modifies `prd-task` parsers, `stoker-implement`,
  `stoker-create-pr`, or `stoker-prd`. Everything downstream picks up the new
  issues by the same shortlist mechanism.
- If a slice depends on a not-yet-merged change in the same PR,
  record it as a `Blocked by` reference to the task issue that will
  deliver it (not to the PR). The loop gates on closed blocker
  issues, not on merge state.
- Jira touches are interactive and host-side only — the sandbox AFK
  loop never calls Jira. It reads the Jira key/URL from the GitHub
  task metadata propagated above.
