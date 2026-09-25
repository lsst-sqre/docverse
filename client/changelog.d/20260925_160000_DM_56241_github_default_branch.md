### New features

- `ProjectGitHubBinding` gains a read-only `default_branch: str | None`: the repository's default branch as GitHub last reported it, or `None` until Docverse has learned it. `ProjectGitHubBindingCreate` does not accept the field, so a create or update that supplies it fails validation.
