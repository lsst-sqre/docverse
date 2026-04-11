---
name: work-it
description: Execute a unit of development work end-to-end using red/green/refactor TDD — plan, implement one test at a time, validate with nox tests and pre-commit, then commit. Use when user wants to implement a feature, fix a bug, refactor code, or do any development task that should go through the full plan-build-test-commit cycle.
---

# Work It — Development Work Cycle

Execute a complete unit of development work using red/green/refactor TDD in tracer-bullet slices.

## Phase 0: Branch safety

**Never do work on the `main` branch.** Before any implementation:

1. Check the current branch with `git branch --show-current`.
2. If already on a non-main feature branch, proceed to Phase 1.
3. If on `main`:
   - If the task or issue specifies a branch name, check it out:
     - If it exists locally, `git checkout <branch>`.
     - If it exists on the remote, `git checkout -b <branch> origin/<branch>`.
     - If it doesn't exist, create it: `git checkout -b <branch> main`.
   - If no branch name is specified, **ask the user** what branch to create or work from before proceeding.

## Phase 1: Understand the task

- Read any referenced plan or PRD.
- Explore the codebase to understand the relevant files, patterns, and conventions.
- If the task is unclear, ask clarifying questions before proceeding.

## Phase 2: Plan the implementation (optional)

If the task has not already been fully planned, create a plan. Break the work into a sequence of **tracer-bullet slices** — each slice is the thinnest vertical cut that exercises one new behavior end-to-end (e.g., route → service → database → response). Order slices so each one builds on the last.

## Phase 3: Implement with red/green/refactor

Work through slices one at a time. For each slice, follow the TDD cycle:

1. **Red** — Write ONE failing test that defines the next behavior. Run it to confirm it fails:
   ```
   TC_HOST=localhost TESTCONTAINERS_RYUK_DISABLED=true uv run --only-group=nox nox -s test -- tests/path/to/test.py::test_name
   ```
2. **Green** — Write the minimum production code to make that test pass. Run the test again to confirm it passes.
3. **Refactor** — Clean up the production code and/or test while keeping the test green. Run lint and type checking:
   ```
   uv run --only-group=lint pre-commit run --all-files
   uv run --only-group=nox nox -s typing
   ```

Repeat this cycle for each behavior in the slice before moving to the next slice.

**Do not batch multiple behaviors into one test. One test, one behavior.**

For client code changes, use the client test runner instead:
```
uv run --only-group=nox nox -s client_test -- client/tests/path/to/test.py::test_name
```

## Phase 4: Final validation

After all slices are complete, run the full suite to catch regressions:

```
uv run --only-group=lint pre-commit run --all-files
uv run --only-group=nox nox -s typing
TC_HOST=localhost TESTCONTAINERS_RYUK_DISABLED=true uv run --only-group=nox nox -s test
uv run --only-group=nox nox -s client_test
```

For Cloudflare Worker changes, also build and test the worker:
```
cd cloudflare-worker && npm run build && npm test
```

Fix any failures and re-run until all pass.

## Phase 5: Commit

Once all validation passes, use `/commit` to create a git commit with a clear message describing the change.
