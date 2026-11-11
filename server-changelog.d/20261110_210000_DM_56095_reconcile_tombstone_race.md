### New features

- The `edition_reconcile_completed` metrics event and the `edition_reconcile` job's `progress` gain a `deleted_skipped` count: republishes the tick planned and then dropped because the edition, or its project, was soft-deleted before the enqueue.

### Bug fixes

- `edition_reconcile` no longer enqueues a publish for an edition that was soft-deleted while the tick was running. A branch deleted as its pull request merged could be read as live by the tick's plan and then lose its CDN key to the delete webhook, which looked like a missing pointer; the tick re-drove the publish and `publish_edition` raised `NotFoundError` into Sentry. The apply-time re-check now reads the edition's `date_deleted`, and its project's, under the same row lock as `current_build_id` and drops the republish with nothing written, counting it as `deleted_skipped`. See `docs/edition-reconcile.md`.
- `publish_edition` now completes with `deleted_skipped` in its `progress` when the project or edition its payload names was deleted between the enqueue and the pickup, or when the edition slug now belongs to a different edition. Before, it raised `NotFoundError` and left its `queue_jobs` row `in_progress` until `publish_edition_reaper` failed it four hours later; now the row closes at once and a keeper-sync run waiting on the job still finalises. The job reads its edition under a row lock, so a delete still uncommitted at pickup is waited for and seen rather than published over.
