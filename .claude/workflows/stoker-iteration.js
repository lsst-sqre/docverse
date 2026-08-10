// stoker-managed: workflows:.claude/workflows/stoker-iteration.js:33b369ecac1f4604
export const meta = {
  name: 'stoker-iteration',
  description:
    'Run one stoker worker phase (implement, review, fixup, or rebase) as a single schema-forced agent, dispatched to the tier and effort the host resolved for that phase. The smart host session selects the task, checks out the branch, and passes the phase kind, worker tier/effort, and task context in args; this workflow builds the lean per-kind prompt and returns the structured result. The heavy phase discipline lives in the repo-installed stoker-implement / stoker-review / stoker-fixup / stoker-rebase skills the worker invokes.',
  phases: [
    {
      title: 'Dispatch worker',
      detail:
        'Build the per-kind prompt from args and run one agent() on the configured tier/effort with a schema-forced structured result.'
    }
  ]
}

// Structured-output schemas, one per worker phase. These are JSON Schema
// object literals (no Zod, no TypeScript). The review schema is the v1
// review payload MINUS the SHAs: the host stamps head_sha / base_sha in
// Python (stoker workflow post-review) since only the host knows the exact
// commit it checked out, so the worker never emits them.

const IMPLEMENT_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    status: { type: 'string', enum: ['done', 'stuck'] },
    // Null on the stuck path (no PR was opened); an integer once the
    // implement skill has created or updated the PR.
    pr_number: { type: ['integer', 'null'] },
    summary: { type: 'string' }
  },
  required: ['status', 'pr_number', 'summary']
}

const REVIEW_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    // The host validates this with the same v1 validator the sandbox path
    // uses, which requires the version marker; the SHAs it wants are
    // stamped host-side, never here.
    stoker_review_version: { type: 'integer', enum: [1] },
    pr_number: { type: 'integer' },
    blocking: { type: 'boolean' },
    summary: { type: 'string' },
    findings: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        properties: {
          id: { type: 'string' },
          severity: {
            type: 'string',
            enum: ['blocker', 'major', 'minor', 'nit']
          },
          summary: { type: 'string' },
          file: { type: ['string', 'null'] },
          line_start: { type: ['integer', 'null'] },
          line_end: { type: ['integer', 'null'] }
        },
        required: ['id', 'severity', 'summary']
      }
    }
  },
  required: ['stoker_review_version', 'pr_number', 'blocking', 'findings']
}

// fixup and rebase share a shape: what the phase ended up as, the findings
// (or conflicts) it deliberately skipped, and free-form notes for the host.
const APPLY_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    status: { type: 'string', enum: ['done', 'stuck'] },
    skipped_findings: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        properties: {
          id: { type: 'string' },
          reason: { type: 'string' }
        },
        required: ['id', 'reason']
      }
    },
    notes: { type: 'string' }
  },
  required: ['status', 'skipped_findings', 'notes']
}

// Optional parent-PRD context block, shared by the prompt builders. Kept
// out of the template literals so an absent PRD leaves no dangling heading.
function prdBlock(args) {
  if (!args.prd_body) {
    return ''
  }
  return '\n\n## Parent PRD\n\n' + args.prd_body
}

function implementPrompt(args) {
  return (
    '# Stoker implement phase (workflow mode)\n\n' +
    'A prior selection step already picked the task below and the host has ' +
    'checked out its branch. Drive this one task end to end via the ' +
    '`stoker-implement` skill: red/green/refactor, then commit, push, open or ' +
    'update the PR, and close the issue — or take the stuck path on failure.\n\n' +
    '## Picked task\n\n' +
    '- Issue: #' + args.issue_number + '\n' +
    '- Branch: `' + args.branch + '`\n\n' +
    '### Issue body\n\n' +
    args.issue_body +
    prdBlock(args) +
    '\n\n## Your job\n\n' +
    'Invoke the `stoker-implement` skill and take the task to completion. Do ' +
    'not emit any `<stoker-status>` markers — return the structured result ' +
    'instead: `status` ("done" or "stuck"), `pr_number` (the PR you ' +
    'created/updated, or null when stuck), and a one-line `summary`.'
  )
}

function reviewPrompt(args) {
  const baseNote = args.base_sha
    ? 'This PR was already reviewed through `' +
      args.base_sha +
      '`: scope your review to the commits added since it (that base to ' +
      '`HEAD`), not the whole PR.'
    : 'No prior review base: review the full PR diff.'
  return (
    '# Stoker review phase (workflow mode)\n\n' +
    'The host has checked out the head branch of the PR below. Review this ' +
    'one PR via the read-only `stoker-review` skill. Do not modify code, ' +
    'push, or post anything — return the structured review and the host posts ' +
    'the GitHub Review from it.\n\n' +
    '## Picked PR\n\n' +
    '- PR: #' + args.pr_number + '\n' +
    '- Branch: `' + args.branch + '`\n\n' +
    baseNote +
    '\n\n## Your job\n\n' +
    'Invoke the `stoker-review` skill and judge correctness, security, and ' +
    'clarity. Return the structured review: `stoker_review_version` 1, ' +
    '`pr_number` ' + args.pr_number + ', `blocking`, and a `findings` array. ' +
    'Do not include commit SHAs — the host stamps those.'
  )
}

function fixupPrompt(args) {
  return (
    '# Stoker fixup phase (workflow mode)\n\n' +
    'The host has checked out the head branch of the PR below, which carries ' +
    'a trusted blocking stoker review whose findings are still unaddressed. ' +
    'Address that review via the `stoker-fixup` skill: apply the findings you ' +
    'agree with, re-validate against this repo\'s project-mechanics, and push.\n\n' +
    '## Picked PR\n\n' +
    '- PR: #' + args.pr_number + '\n' +
    '- Branch: `' + args.branch + '`\n\n' +
    '## Your job\n\n' +
    'Invoke the `stoker-fixup` skill. For any finding you deliberately skip, ' +
    'post a follow-up review comment explaining why. Return the structured ' +
    'result: `status` ("done" or "stuck"), `skipped_findings` (each with an ' +
    '`id` and a `reason`, empty when you applied everything), and free-form ' +
    '`notes`. Do not emit any `<stoker-status>` markers.'
  )
}

function rebasePrompt(args) {
  return (
    '# Stoker rebase phase (workflow mode)\n\n' +
    'The host has checked out the head branch of the PR below, which is ' +
    'behind `main`; an automated rebase hit a conflict and was aborted, so ' +
    'the branch is back at its own tip, clean. Redo the rebase via the ' +
    '`stoker-rebase` skill, resolving the conflicts by understanding both ' +
    'sides.\n\n' +
    '## Picked PR\n\n' +
    '- PR: #' + args.pr_number + '\n' +
    '- Branch: `' + args.branch + '`\n\n' +
    '## Your job\n\n' +
    'Invoke the `stoker-rebase` skill. Rebase `' + args.branch + '` onto a ' +
    'freshly fetched `origin/main`, re-validate against project-mechanics, and ' +
    'leave the branch rebased, committed, and clean. Do not push — the host ' +
    'force-pushes with lease. If you cannot resolve the conflicts, abort the ' +
    'rebase so the tree is clean. Return the structured result: `status` ' +
    '("done" or "stuck"), `skipped_findings` (empty for rebases unless you ' +
    'left a conflict unresolved), and free-form `notes`.'
  )
}

// Per-kind dispatch table: each worker phase maps to its schema and its lean
// prompt builder. The heavy discipline lives in the repo-installed skills the
// prompts point at, so these stay thin.
const KINDS = {
  implement: { schema: IMPLEMENT_SCHEMA, build: implementPrompt },
  review: { schema: REVIEW_SCHEMA, build: reviewPrompt },
  fixup: { schema: APPLY_SCHEMA, build: fixupPrompt },
  rebase: { schema: APPLY_SCHEMA, build: rebasePrompt }
}

// A host session dispatching this saved workflow may hand `args` through as
// a JSON-encoded string rather than an object, in which case every
// `args.<key>` lookup below would silently be undefined and the dispatch
// would throw "unknown phase kind undefined". Normalize once here and read
// the phase inputs off `phaseArgs` from this point on; an object-valued
// `args` passes straight through untouched.
const phaseArgs = typeof args === 'string' ? JSON.parse(args) : args

const kind = KINDS[phaseArgs.kind]
if (!kind) {
  throw new Error(
    'stoker-iteration: unknown phase kind ' +
      JSON.stringify(phaseArgs.kind) +
      '; expected one of implement, review, fixup, rebase'
  )
}

// One agent() call, on the tier and effort the host resolved for this phase.
// model is always a tier name (haiku|sonnet|opus|fable) supplied in args —
// never a concrete model id, which the Workflow tool would reject.
const options = {
  schema: kind.schema,
  model: phaseArgs.model_tier,
  label: 'stoker-' + phaseArgs.kind,
  phase: phaseArgs.kind
}
if (phaseArgs.effort) {
  options.effort = phaseArgs.effort
}

const result = await agent(kind.build(phaseArgs), options)
// agent() returns null when the user skips the agent mid-run or the subagent
// dies on a terminal API error. Returning that null as-is would break the
// orchestrator's structured-result contract (it expects `status` for
// implement/fixup/rebase and a review payload for review), so map it to a
// stuck-shaped sentinel the orchestrator's stuck path (B7) parks. For the
// review kind — whose schema has no `status` field — this same sentinel is
// still safe: the host validates review payloads before posting, so this
// sentinel can never be posted as a review.
if (result == null) {
  return {
    status: 'stuck',
    summary:
      'The ' +
      phaseArgs.kind +
      ' worker returned no result (the agent was skipped or terminated ' +
      'before producing a structured result).'
  }
}
return result
