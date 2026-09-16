### Other changes

- `Project.default_edition` is now populated on every row that `DocverseClient.list_projects` returns, not only on single-project responses; the field's description no longer says it is omitted from list responses. It stays `None` only for a soft-deleted project.
