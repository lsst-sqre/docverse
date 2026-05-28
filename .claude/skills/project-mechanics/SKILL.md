---
name: project-mechanics
description: Project-specific build/test/lint/typing commands for this repo. Read this skill at the start of any phase that runs validation (`stoker-work`, `stoker-fixup`, `stoker-rebase`).
---

# Project mechanics

This file is the source of truth for how this repo runs tests, lint,
and type-checking. Profile-shipped phase skills read it at the start
of each phase and use the named commands verbatim.

Docverse is a uv-workspace monorepo with three components: the
**server** (`src/docverse/`, `tests/`), the **client**
(`client/`), and the **Cloudflare worker** (`cloudflare-worker/`,
Node/TypeScript). The named commands below target the primary
(server) package; see ## Monorepo selectors for per-package routing.

## Test commands

- `focused_test`: `TC_HOST=localhost TESTCONTAINERS_RYUK_DISABLED=true uv run --only-group=nox nox -s test -- tests/path/to/file_test.py::test_name`
- `complete_test`: `TC_HOST=localhost TESTCONTAINERS_RYUK_DISABLED=true uv run --only-group=nox nox -s test client_test`

## Lint

- `lint_touched`: `uv run --only-group=lint pre-commit run --files {files}`
- `lint_all`: `uv run --only-group=lint pre-commit run --all-files`

## Typing

- `typing`: `uv run --only-group=nox nox -s typing`

## Final validation

End-of-task validation runs `complete_test` + `lint_all` + `typing`
in that order. Additional checks by component:

- Worker changes (`cloudflare-worker/`): `cd cloudflare-worker && npm run build && npm test` (run `npm ci` first if dependencies aren't installed).
- deploy-worker changes (client deploy-worker code, which shells out to Node/npm): `uv run --only-group=nox nox -s deploy_worker_test`.
- Client changes also run multi-version compat suites in CI: `uv run --only-group=nox nox -s client_test_compat client_test_oldest` (Python 3.12 + 3.13).

## Monorepo selectors

Scope test/lint commands to the package you changed:

- **Server** (`src/docverse/`, `tests/`, `alembic/`, `noxfile.py`): `TC_HOST=localhost TESTCONTAINERS_RYUK_DISABLED=true uv run --only-group=nox nox -s test -- tests/...`
- **Client** (`client/`): `uv run --only-group=nox nox -s client_test -- client/tests/...`
- **Cloudflare worker** (`cloudflare-worker/`): `cd cloudflare-worker && npm run build && npm test`
- Lint and typing are repo-wide regardless of which package changed (`lint_all` / `typing` above cover all components).

<!-- stoker-onboarded-from: github.com/lsst-sqre/rubin-stoker//profile@u/jonathansick/init
     prompt-hash: 353d09308c405a6aa0360d12547d6955784660346220564052d866ed25ad028c
     onboarded-at: 2026-05-28T22:12:38Z -->
