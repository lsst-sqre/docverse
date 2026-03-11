# Claude instructions

Docverse is a hosting platform for versioned documentation websites.

The Docverse design document is [SQR-112: Docverse documentation hosting platform design](https://sqr-112.lsst.io).

## Migration from LTD Keeper

This repository was originally created for LTD Keeper. During the codebase migration, the original LTD Keeper codebase is in the `keeper` directory. The `keeper` directory will be removed once the migration is complete.

## Development commands

- **Lint all files**: `uv run --only-group=lint pre-commit run --all-files`
- **Lint a specific file**: `uv run --only-group=lint ruff check path/to/file.py`
- **Format a specific file**: `uv run --only-group=lint ruff format path/to/file.py`
- **Type checking**: `uv run --only-group=nox nox -s typing`
- **Server tests**: `uv run --only-group=nox nox -s test`
- **Client tests**: `uv run --only-group=nox nox -s client_test`
- **Running specific tests**: pass pytest args after `--`, e.g. `uv run --only-group=nox nox -s test -- tests/path/to/test_file.py`
