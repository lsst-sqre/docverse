---
name: rubin-create-pr
description: Create a pull request for a Rubin Observatory project with Jira-prefixed title, summary, validation steps, and references linking to PRD/task/Jira issues. Use when user wants to create a PR, open a pull request, or mentions "create PR" in a Rubin project context.
---

# Rubin Create PR

Create a pull request with Rubin conventions: Jira-prefixed title, structured body with references to PRD, task issue, and Jira.

## Workflow

### 1. Gather context

Collect these values. Use the first available source for each:

**Jira key**: current conversation context > branch name (`tickets/{JIRA_KEY}-...`) > ask the user.

**Task issue** (GitHub issue created by rubin-prd-to-issues): current conversation context > auto-detect (see below) > ask the user (optional — may not exist).

**Parent PRD** (GitHub issue): task issue's `Parent PRD` metadata field > ask the user (optional — may not exist).

**Jira URL**: task issue's `Jira URL` metadata field > construct as `https://rubinobs.atlassian.net/browse/{JIRA_KEY}`.

#### Auto-detecting the task issue

If the task issue is not in the current context, search for it:

```
gh issue list --label prd-task --state open --json number,title,body --limit 50
```

Find the issue whose Metadata table `Branch` field matches the current branch name. Extract `Parent PRD` and `Jira URL` from that issue's Metadata table.

### 2. Push the branch

Check if the branch has a remote tracking branch and is up to date:

```
git status -sb
```

If the branch has not been pushed or has unpushed commits, push it:

```
git push -u origin HEAD
```

### 3. Prepare PR content

**Title**: `{JIRA_KEY}: {descriptive title}`

The descriptive title should be concise (under 60 chars after the prefix) and describe what the PR accomplishes.

**Body** uses this template:

```markdown
## Summary

{1-3 bullet points describing what this PR does and why}

## Validation steps

{Bulleted checklist of manual QA actions to verify the changes work correctly. Omit anything that CI runs automatically — no linting, type checking, or automated test suite items. Only include manual verification steps.}

## References

- Closes #{task_issue_number}
- PRD: #{prd_issue_number}
- Jira: {jira_url}
```

Rules for the References section:
- Always use `Closes #{number}` for the task issue to auto-close it on merge
- Omit the PRD line if there is no parent PRD
- Omit the task issue `Closes` line if there is no task issue

### 4. Create the PR

```
gh pr create --title "{JIRA_KEY}: {title}" --body "$(cat <<'EOF'
{body}
EOF
)"
```

Report the PR URL to the user when done.
