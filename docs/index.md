# Docverse documentation

Standalone Markdown, because the repository does not yet have a
documentation build; a future Sphinx/documenteer or mkdocs tree can
absorb these pages unchanged. The design of the system as a whole is
[SQR-112](https://sqr-112.lsst.io).

## Reference

- [REST API conventions](api-conventions.md) — the conventions new
  endpoints follow: async-action verbs, path parameters, hypermedia
  links, public identifiers, pagination, and the rest.

## Operations

Background work runs on three arq pools. The **maintenance** pool
hosts the cron-driven loops that keep an environment converged —
`lifecycle_eval`, `git_ref_audit`, `inventory_census`,
`purgatory_cleanup`, `edition_reconcile`, and the stuck-run reapers
that back each of them up.

- [Edition reconciliation](edition-reconcile.md) — the
  `edition_reconcile` loop: the drift it detects, its full decision
  table, its configuration knobs and Phalanx values, and how to read
  one tick's outcome.

The other maintenance-pool jobs do not have operations pages yet;
until they do, their module docstrings under
`src/docverse_server/worker/functions/` are the reference, and their
knobs are documented on the fields of
`src/docverse_server/config.py`.
