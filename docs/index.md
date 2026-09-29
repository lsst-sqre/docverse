# Docverse documentation

Standalone Markdown, because the repository does not yet have a
documentation build; a future Sphinx/documenteer or mkdocs tree can
absorb these pages unchanged. The design of the system as a whole is
[SQR-112](https://sqr-112.lsst.io).

## Reference

- [REST API conventions](api-conventions.md) — the conventions new
  endpoints follow: async-action verbs, path parameters, hypermedia
  links, public identifiers, pagination, conditional GET, soft-deleted
  resources, and the rest.
- [Metrics events](metrics.md) — every Sasquatch event Docverse
  publishes: its InfluxDB measurement, each field's type and meaning,
  which fields the Phalanx `influxTags` list makes tags, the
  cardinality rule a new tag must follow, what `ltd_lag` measures, and
  example InfluxQL queries for sync lag, API request volume and
  latency, and GitHub webhook deliveries.

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
- [Scoping the keeper sync](keeper-sync-scope.md) — which LTD products
  an organization syncs: the include/exclude rule, `fullmatch` pattern
  semantics, what happens when a project falls out of scope, and the
  preview → `PATCH` → backfill workflow a wave migration uses.
- [Keeper-sync timestamps](keeper-sync-scope.md#timestamps-mirror-ltd)
  — why a synced edition's and build's dates are LTD's rather than the
  import's: which columns follow LTD, how every sync visit re-asserts
  them, what that means for `draft_inactivity`, what the project clock
  does not do, and the full-org-run backfill.
- [Keeper-sync transport resilience](keeper-sync-transport.md) — how a
  build copy rides out an outage on either end, R2 or the LTD bucket:
  the per-object budget for R2 uploads and its ride-out arithmetic, the
  dedicated copy client, the worker-wide upload cap and shared LTD
  source client that keep the sync worker inside its node's NAT port
  budget, the build-level retry that re-runs a copy after a transport
  error on either end, what happens to an edition that still fails, and
  how to read the `build_content_copied` metrics event.
- [GitHub integration](github-integration.md) — what each GitHub App
  webhook delivery does (`push`, `delete`, `repository` renamed,
  transferred and edited, `organization` renamed, and the
  `installation` events); how a project's `__main` edition follows its
  repository's default branch across a rename, from the webhook, the
  `project_github_resolve` worker, and the daily `git_ref_audit`; the
  guarded rewrite and what it never touches; the audit as the
  post-upgrade backfill; how keeper-sync keeps synced projects
  converged; and the manual `PATCH` for a `__main` the rule leaves
  pinned.

The other maintenance-pool jobs do not have operations pages yet;
until they do, their module docstrings under
`src/docverse_server/worker/functions/` are the reference, and their
knobs are documented on the fields of
`src/docverse_server/config.py`.
