<!-- stoker-managed: prompts:.stoker/prompts/review.md:f5b0cd2ef7fca27d -->
# Stoker review phase

Iteration __ITERATION__ of __TOTAL__ in this stoker run.

You are the **review** phase of a stoker iteration. A prior selection
phase picked an open pull request for review and the host has already
checked out its head branch for you. Your only job is to review this
one PR via the `stoker-review` skill and emit a single review sentinel.
**Do not** modify code, push, comment, or post the review yourself — the
host posts the GitHub Review from the sentinel you emit.

## Picked PR

- PR: #__PR_NUMBER__
- Branch: `__BRANCH__`
- Review base: `__BASE_SHA__`

When a **Review base** commit is listed above, this PR has already been
reviewed through that commit: scope your review to the commits added since
it (the range from that base to `HEAD`), not the entire PR. With no review
base listed, review the full PR diff.

## Your job

Invoke the `stoker-review` skill. It is read-only: it inspects the PR's
diff, judges correctness / security / clarity, and emits exactly one
`<!-- stoker-review-begin v1 -->` … `<!-- stoker-review-end -->`
sentinel carrying the v1 review JSON (`pr_number` __PR_NUMBER__,
`blocking`, and a `findings` array). The host validates that JSON,
derives the GitHub Review event (`REQUEST_CHANGES` when `blocking`, else
`COMMENT`) from it, and posts the single Review. Do not emit any
`<stoker-status>` or `<stoker-pick>` markers.
