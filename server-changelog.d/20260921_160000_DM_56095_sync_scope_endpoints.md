### New features

- The per-project keeper-sync endpoints — `GET /orgs/{org}/keeper-sync/projects/{slug}`, its `/editions` collection, and `POST …/projects/{slug}/refresh` — now resolve scope through the config model's shared rule instead of consulting `project_slugs` alone, so they agree with the worker on exactly which LTD products sync. A slug in scope only through `project_slug_patterns` is now inspectable and refreshable, and one removed by `exclude_project_slugs` or `exclude_project_slug_patterns` returns 404 even when `project_slugs` (or the `"*"` wildcard) also admits it.

### Other changes

- The 404 those three endpoints return for an out-of-scope slug now reads "is not in the keeper-sync scope for organization …" rather than naming the `project_slugs` allowlist, which is no longer the whole rule.
