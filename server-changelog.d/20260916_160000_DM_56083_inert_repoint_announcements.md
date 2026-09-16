### Bug fixes

- An inert edition repoint no longer announces itself. `POST .../editions/{edition}/rollback` to the build already being served, and `PATCH .../editions/{edition}` carrying only a `build` that is already current, now publish no `edition_lifecycle` event and enqueue no `dashboard_build` job — matching the history row, the `publish_status` flip, the `publish_edition` job, and the project clock stamp they already skipped. Both endpoints answer such a request with `200` precisely so a client that lost its connection can retry it, but the retry was not free: the dashboard enqueue dedupes only against a job that is still queued or in flight, and the dashboard worker re-renders and re-uploads the whole dashboard with no content-hash short-circuit, so a retried rollback bought a full rebuild per attempt and emitted a rollback metric with no history row behind it.
- Re-driving a **failed** publish of the served build still announces itself, because it is a real change: it records a history row, returns the edition to `pending`, and enqueues the publish. A rollback or override onto a different build, and a `PATCH` carrying metadata fields (with or without a `build`), are unaffected.

### Other changes

- `EditionService.update` and `EditionService.rollback` now return an `EditionWrite` — the resolved organization, project, and edition, plus a `changed` flag — instead of a bare `(organization, project, edition)` tuple, so their handlers can tell a write that moved something from one whose postcondition already held.
