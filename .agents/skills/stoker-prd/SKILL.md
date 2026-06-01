---
name: stoker-prd
description: Write a Product Requirements Document seeded from a Rubin/DM Jira ticket (via the Atlassian MCP), interviewing the user and exploring the codebase, then submit it as a GitHub issue using the stoker `prd` Issue Form and comment back on Jira. Use when the user wants to write a PRD, plan a feature, or create a product requirements document — especially from a `DM-` ticket — in a Rubin stoker-installed repo.
---
<!-- stoker-managed: skills:.agents/skills/stoker-prd/SKILL.md:937c8da6dd0c33cc -->

# stoker-prd — write a PRD from a Jira ticket and file it as a GitHub issue

Drive the user through a structured PRD interview, **seeded from the
Rubin/DM Jira ticket**, explore the codebase to ground assumptions,
sketch the module-level design, then submit the result as a GitHub
issue via the repo's `prd` Issue Form — and finally comment back on
the Jira ticket linking the PRD so stakeholders can follow along.

The output is a `prd`-labeled issue whose body matches
`.github/ISSUE_TEMPLATE/prd.yml`. Downstream skills (`stoker-prd-to-issues`,
`stoker-prd-followup`, the AFK loop) consume the issue by parsing those
section headers, so emit them verbatim.

This is the Rubin variant of the default `stoker-prd`. It keeps the
default's relentless design-tree interview and Open-questions
self-check, and layers the Rubin workflow on top: a Jira-ticket seed,
`### Jira Key` / `### Jira URL` body fields, a `DM-XXXXX:`-prefixed
title, and a Jira linkback comment.

## Jira via the Atlassian MCP

This skill reads and comments on Jira through the **Atlassian MCP
server** (tools `mcp__atlassian__jira_get_issue` and
`mcp__atlassian__jira_add_comment`), which the user configures (see
the repo's AGENTS.md). Jira touches happen **interactively, on the
host** — never in the sandbox AFK loop.

**Degrade gracefully when the MCP is absent.** If the Atlassian MCP
tools are not available (no `mcp__atlassian__*` tools in this
session), do not fail: ask the user to paste the Jira ticket's
summary / description / acceptance criteria instead, proceed with the
PRD, and at the end print the comment you *would* have posted so the
user can paste it into Jira themselves.

## Workflow

You may skip steps when they are not necessary for the situation
(e.g. the user already pasted a long problem statement, or there is
no Jira ticket at all).

### 1. Get the Jira ticket (the seed)

Ask the user for the Jira issue key (e.g. `DM-12345`). Fetch it:

```
mcp__atlassian__jira_get_issue(issue_key="DM-12345")
```

Extract the summary, description, and acceptance criteria. Jira
tickets are **high-level** — they describe the stakeholder-facing
problem, not the technical design. Your job across the rest of this
workflow is to translate that high-level ticket into a technical,
actionable PRD. Present a concise read-back of the ticket and confirm
you understand it before drilling in.

If the MCP is unavailable, ask the user to paste the ticket contents
(per the graceful-degradation note above). If there is genuinely no
ticket (a small, ticket-less PRD per AGENTS.md), skip straight to
gathering the seed problem from the user.

### 2. Explore the repo

Read the relevant code to verify the ticket's and the user's
assertions and understand the current state of the system. The PRD's
Scope section should reflect what's actually in the tree, not
assumptions or the ticket's aspirational framing.

### 3. Interview relentlessly

Walk down each branch of the design tree, resolving dependencies
between decisions one-by-one. Push beyond the ticket — high-level
asks usually hide implementation choices the user has already made
implicitly.

Keep going until you have a shared understanding. Do not move on
while ambiguity remains.

### 4. Drill the design tree

Before composing the PRD body, walk every canonical decision axis
below and push on each one until either (a) the user has made the
choice, or (b) the choice is genuinely deferred to a stakeholder
who is not in this conversation. **Silent assumptions on these
axes are how PRDs ship with a non-empty Open Questions section that
nobody owns.**

The canonical axes:

- **Data shape / persistence** — what new state is introduced, where
  it lives (in-memory, file on disk, sqlite, GitHub issue body,
  etc.), and what schema/migration rules apply.
- **Public interface** — the CLI surface, function signature, config
  key, or API endpoint that external callers will touch. Naming,
  argument order, defaults, and whether the interface is stable or
  experimental.
- **Error & failure paths** — what happens on bad input, missing
  dependency, network failure, partial write, or upstream timeout.
  Which errors surface to the user vs. swallow-and-log.
- **Observability** — what logs, metrics, traces, or doctor checks
  let an operator confirm the feature is working (or diagnose why
  it isn't).
- **Migration / backwards compatibility** — for existing installs,
  what must change on upgrade. In-place rewrites, opt-in flags,
  schema bumps, or "no change required."
- **Deprecation** — anything this feature replaces, the deprecation
  window, and how users learn about it (warning on use, doctor
  hint, changelog entry).
- **Test coverage strategy** — unit vs. integration tier per
  affected module, fixtures or live services required, and any
  acceptance criteria the user wants pinned by a specific test.

For any axis where more than one decision is still unresolved,
emit a single `AskUserQuestion` batch covering that axis. Batching
by axis (rather than one question at a time) lets the user resolve
related choices in one pass and surfaces dependencies between them.

Worked example — suppose the PRD adds a new `stoker prune` command
that deletes stale per-task branches. The error-path axis has three
unresolved decisions: behavior on protected branches, behavior on
branches with unmerged commits, and whether `--force` overrides
both. Emit one batched question:

```
AskUserQuestion(
  questions=[
    {
      "question": "What should `stoker prune` do on a branch GitHub
                   marks as protected?",
      "header": "Protected branches",
      "options": [
        {"label": "Skip with a warning",  "description": "..."},
        {"label": "Refuse and exit non-zero", "description": "..."},
        {"label": "Delete anyway",        "description": "..."},
      ],
      "multiSelect": False,
    },
    {
      "question": "What about branches whose tip is ahead of main?",
      "header": "Unmerged commits",
      "options": [
        {"label": "Skip with a warning", "description": "..."},
        {"label": "Refuse and exit",     "description": "..."},
        {"label": "Delete anyway",       "description": "..."},
      ],
      "multiSelect": False,
    },
    {
      "question": "Does `--force` override both of the above?",
      "header": "Force semantics",
      "options": [
        {"label": "Yes, both",     "description": "..."},
        {"label": "Only unmerged", "description": "..."},
        {"label": "Only protected","description": "..."},
        {"label": "Neither — `--force` does something else", "description": "..."},
      ],
      "multiSelect": False,
    },
  ]
)
```

When an axis has only one unresolved decision (or none), skip the
batch — a one-question batch is the same as a plain prose question
and the structured form adds no value. When an axis is fully
resolved by reading the seed ticket or the repo, skip it entirely.

### 5. Sketch the modules

Identify the major modules to build or modify. Actively look for
opportunities to extract **deep modules** — modules that encapsulate
a lot of functionality behind a simple, testable interface that
rarely changes. Confirm with the user that the module sketch matches
their expectations and ask which modules they want test coverage on.

### 6. Compose the PRD body

Use the template below. The section headers must match the Issue Form
field labels exactly so the parser picks them up — including the
`### Jira Key` and `### Jira URL` fields, which `stoker-prd-to-issues`
reads to propagate the ticket onto each task. Within each free-text
section, write narrative prose or bullets.

For a ticket-less PRD, write `### Jira Key` / `### Jira URL` with an
empty value (or omit the fields) — downstream skills treat a missing
key as "no Jira" and skip every Jira touch.

### 7. Self-check: Open questions before filing

Before filing, re-read the drafted **Open questions** section. The
goal state is **zero open questions at filing time** for any
question the user could have answered in-conversation.

- If the section is empty or reads "None — ready to break into
  tasks.", proceed to step 8.
- If any remaining bullet is something the user in this conversation
  could plausibly answer — a design choice, a naming preference, a
  test-coverage call, anything from the axes in step 4 — **loop
  back** to step 4 (or step 3 if the ambiguity is broader) and
  resolve it. Do not file a PRD with answerable questions buried in
  the Open Questions section; the breakdown skill will surface them
  later at higher cost.
- An item may legitimately remain only when it is explicitly
  deferred to a human-decision-required-later party (a stakeholder
  outside this conversation, a vendor response, a legal review, a
  scheduled meeting). In that case, **annotate each remaining
  bullet** with a one-line justification next to it explaining why
  it cannot be resolved in-conversation. A bullet without a
  justification is a signal to loop back, not a signal to ship.

### 8. File the issue

Detect the repo from the current git remote:

```
gh repo view --json nameWithOwner -q .nameWithOwner
```

Ensure the `prd` label exists **before** creating the issue —
downstream tooling (`stoker-prd-to-issues`, `stoker-prd-followup`) finds parent
PRDs by `--label prd`, so a PRD without the label is invisible:

```
gh label create prd --description "Product Requirements Document" --color "0e8a16" --force
```

`--force` makes the command idempotent.

Create the issue, the `prd` label, and assign it to yourself (the
operator). **Title:** prefix with the Jira key when there is one —
`DM-XXXXX: <title>`; for a ticket-less PRD use `PRD: <title>`:

```
gh issue create --repo <owner/repo> --title "DM-XXXXX: <title>" --label "prd" --assignee @me --body "$(cat <<'EOF'
<body>
EOF
)"
```

The `--assignee @me` flag assigns the PRD to you (the operator) by
default. This is load-bearing: the stoker loop's self-scoped
shortlist (`[selection].scope_to_self`, default on) only surfaces a
PRD's tasks to the loop when the operator is one of their assignees
(`stoker-prd-to-issues` likewise defaults each `prd-task` it breaks
out to `--assignee @me`). If the user explicitly asks to file the
PRD **on behalf of** someone else, replace `@me` with that person's
GitHub login (`--assignee <login>`); otherwise always default to
`@me`.

**Never drop the `--label "prd"` flag.** If `gh issue create` fails
with `could not add label: 'prd' not found`, the label step above
was skipped or the label was deleted — re-run
`gh label create prd ... --force` and retry the issue creation
**with the `--label` flag intact**.

Report the issue URL to the user. Do not break the PRD into tasks
yourself — that's `stoker-prd-to-issues`' job, in a follow-up turn.

### 9. Comment back on Jira

When a Jira key exists, post a comment on the ticket linking the PRD
so stakeholders can follow progress:

```
mcp__atlassian__jira_add_comment(
  issue_key="DM-XXXXX",
  body="Technical PRD created: <github_issue_url>"
)
```

If the Atlassian MCP is unavailable, print the comment text and the
ticket key so the user can paste it into Jira themselves. Skip this
step entirely for a ticket-less PRD.

## PRD body template

```markdown
### Jira Key

DM-XXXXX

### Jira URL

https://rubinobs.atlassian.net/browse/DM-XXXXX

### Summary

1–3 sentences describing what this PRD proposes and why.

### Scope

In scope:
- <bullet>
- <bullet>

Out of scope:
- <bullet>

### Acceptance criteria

Manual-QA checklist for verifying the change is complete:

- [ ] <criterion>
- [ ] <criterion>

### Open questions

Anything that needs human decision before tasks can be cut:

- <question>
- <question>

(Or "None — ready to break into tasks.")
```

## Example output

```markdown
### Jira Key

DM-45678

### Jira URL

https://rubinobs.atlassian.net/browse/DM-45678

### Summary

Add a CLI flag to `stoker run` so a single iteration can be locked
to one harness regardless of profile defaults. The current `phases.*`
config is per-phase but can't be overridden at invocation time, which
forces config edits for one-off experiments.

### Scope

In scope:
- New `--harness <name>` flag on `stoker run`.
- Flag overrides `phases.<phase>.harness` for the duration of one
  invocation.
- Validation that the named harness is registered.

Out of scope:
- Per-phase override flags (`--implement-harness`, etc.) — defer.
- Persisting the override to settings.

### Acceptance criteria

- [ ] `stoker run --harness codex 1` runs implement+review with codex.
- [ ] An unknown harness name fails fast with a helpful error.
- [ ] `stoker run` without the flag behaves exactly as before.

### Open questions

- None — ready to break into tasks.
```

## Notes

- The PRD body lives in the issue. Don't paste it back into chat
  unless the user asks — link to the issue URL instead.
- If the host repo doesn't have `.github/ISSUE_TEMPLATE/prd.yml`, the
  fields will still parse correctly (Issue Forms are an authoring
  convenience; the body shape is what matters).
- Jira touches are interactive and host-side only. The sandbox AFK
  loop never calls Jira; it reads the key/URL from the GitHub issue
  metadata `stoker-prd-to-issues` propagates.
