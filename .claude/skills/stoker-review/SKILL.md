---
name: stoker-review
description: Read-only PR review primitive — inspect a pull request's diff and emit a single stoker-review JSON sentinel (findings + blocking verdict) for the host to post as a GitHub Review. Use when invoked from the stoker review-phase prompt, or when the user asks to review a PR and emit the stoker review sentinel — and you are working in a stoker-installed repo. Does NOT consume project-mechanics and never modifies code.
---
<!-- stoker-managed: skills:.claude/skills/stoker-review/SKILL.md:6b194bbb952f90e1 -->

# stoker-review — emit a review sentinel for one PR

You are the read-only review primitive of the stoker AFK loop. A prior
selection phase picked one open pull request and the host has already
checked out its head branch. Your job is to judge the PR's diff and emit
**exactly one** review sentinel. The host (not you) posts the GitHub
Review from that sentinel.

**This skill is strictly read-only.** Never edit files, stage, commit,
push, switch branches, post a comment, or run `gh pr review` / any
GitHub-mutating command. You inspect and report; the host posts.

**This skill does NOT consume `project-mechanics`.** A review reads the
diff and reasons about it — it does not run the repo's test / lint /
typing commands. There is no Phase 0.5 prelude. (The `stoker-fixup` and
`stoker-rebase` skills, which change code, are the ones that re-validate
via `project-mechanics`.)

**You are offline except for GitHub.** This review runs inside stoker's
sandbox behind a default-deny egress firewall. You can read the working
tree and reach GitHub, but you **cannot** fetch general documentation —
language / library / framework docs, MDN, Stack Overflow, security
advisories, RFCs. Treat external documentation as unavailable; do not
assume a `WebFetch` of a docs URL will succeed.

## Phase 1: Read the PR

The PR number and branch are in the prompt that invoked you. The branch
is already checked out. Gather the change set with read-only commands:

- `gh pr diff <PR_NUMBER>` — the full unified diff under review.
- `gh pr view <PR_NUMBER> --json title,body,headRefName,baseRefName,additions,deletions,files`
  — the PR's own description and the files it touches.
- Read the surrounding source of any changed file you need more context
  on (the working tree is the PR head).

**Incremental scope.** If the invoking prompt names a **review base**
commit, this PR has already been reviewed through that commit — review only
the commits added since it. Use `git diff <base>..HEAD` (and
`git log <base>..HEAD`) as your change set instead of the full
`gh pr diff <PR_NUMBER>`, so already-reviewed code is not re-reviewed. The
base commit is an ancestor of the checked-out head, so the range is
available locally. When no review base is given, review the full PR diff.

Do not read or rely on issue or PR **comments** — stoker's trust
boundary ingests bodies and review bodies only.

## Phase 2: Judge the diff

Assess the change against, in priority order:

1. **Correctness** — logic errors, off-by-ones, broken edge cases,
   incorrect error handling, race conditions.
2. **Security** — injection, unsafe deserialization, secret leakage,
   missing authz, unsafe defaults.
3. **Tests** — does the change carry tests for the behavior it adds or
   fixes? Are existing tests weakened?
4. **Clarity / maintainability** — naming, dead code, needless
   complexity, misleading comments.

Each concern worth raising becomes one **finding**. Be specific and
actionable; a finding a human (or the fixup phase) can act on is worth
more than a vague worry. Skip nits that don't change the verdict.

When a finding's correctness depends on third-party behavior you can't
confirm from the diff or working tree — a library's exact semantics, an
API contract, a spec or advisory — say so instead of asserting it. Record
it as a `warning` / `info` finding whose `summary` and `suggested_fix`
note that it needs human or external verification, rather than a blocking
`error`. Don't let an unverifiable hunch set `blocking = true`.

Decide whether the review is **blocking**:

- `blocking = true` when at least one finding is a correctness or
  security defect that should be fixed before merge → the host posts the
  Review with event `REQUEST_CHANGES`.
- `blocking = false` when the PR is mergeable as-is (no findings, or only
  non-blocking suggestions) → the host posts the Review with event
  `COMMENT`.

stoker never approves — humans merge. There is no "approve" verdict.

## Phase 3: Emit the sentinel

End your response with **exactly one** sentinel block. The fenced JSON
must match the v1 schema. `stoker_review_version` is `1`, `pr_number`
must equal the PR you reviewed, `blocking` is your verdict, and
`findings` is an array (possibly empty):

````markdown
<!-- stoker-review-begin v1 -->
```json
{
  "stoker_review_version": 1,
  "pr_number": <PR_NUMBER>,
  "blocking": true,
  "summary": "One- or two-sentence overall verdict.",
  "findings": [
    {
      "id": "f1",
      "severity": "error",
      "category": "correctness",
      "file": "src/foo.py",
      "line_start": 42,
      "line_end": 45,
      "summary": "Off-by-one in the range bound.",
      "suggested_fix": "Use range(0, n), not range(0, n + 1)."
    }
  ]
}
```
<!-- stoker-review-end -->
````

Field notes:

- **Required:** `stoker_review_version` (always `1`), `pr_number`,
  `blocking`, `findings` (an array — use `[]` when there are none).
- **Optional but encouraged:** `summary` (the human-readable verdict);
  per-finding `id`, `severity` (`error` / `warning` / `info`),
  `category`, `file`, `line_start`, `line_end`, `summary`,
  `suggested_fix`. Line numbers are carried for the fixup phase even
  though v1 doesn't anchor them in GitHub's UI.
- Emit the block **once**, as the final thing in your response. Do not
  emit `<stoker-status>` or `<stoker-pick>` markers — those belong to
  other phases. Do not post anything yourself; the host reads this
  sentinel and posts the single GitHub Review.
