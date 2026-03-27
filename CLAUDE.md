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
- **Server tests**: `TC_HOST=localhost TESTCONTAINERS_RYUK_DISABLED=true uv run --only-group=nox nox -s test`
- **Client tests**: `uv run --only-group=nox nox -s client_test`
- **Running specific tests**: pass pytest args after `--`, e.g. `uv run --only-group=nox nox -s test -- tests/path/to/test_file.py`

## Coding conventions

- SQL table names are **plural** (e.g., `organizations`, `projects`, `builds`)
- SQLAlchemy ORM classes are **singular** with `Sql` prefix (e.g., `SqlOrganization`, `SqlProject`)
- Timestamp columns use `date_` prefix (e.g., `date_created`, `date_updated`)

### Request context pattern

- Handlers use `context: Annotated[RequestContext, Depends(context_dependency)]` as their dependency
- Access logger, factory, and session via `context.*`
- Do not create loggers or `Factory` instances manually in handlers
- Use `context.rebind_logger(key=value)` to add structured logging context

### Transaction management

- **Handlers own the transaction** — services must not call `flush()` or `commit()`
- Write handlers: wrap body in `async with context.session.begin():`, call `await context.session.commit()` before exiting the block
- Read handlers: wrap body in `async with context.session.begin():`, no commit needed
- Services may call `flush()` to get database-generated values (e.g., IDs, timestamps) but must not `commit()`
