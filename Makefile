.PHONY: init
init:
	uv sync --frozen --all-groups
	uv run --only-group=lint prek install

.PHONY: update
update: update-deps init

.PHONY: update-deps
update-deps:
	uv lock --upgrade
	uv run --only-group=lint prek autoupdate
	./scripts/update-uv-version.sh

.PHONY: lint
lint:
	uv run --only-group=lint prek run --all-files

.PHONY: typing
typing:
	uv run --only-group=nox nox -s typing

.PHONY: test
test:
	uv run --only-group=nox nox -s test

.PHONY: run
run:
	uv run uvicorn docverse.main:app --reload --port 8080
