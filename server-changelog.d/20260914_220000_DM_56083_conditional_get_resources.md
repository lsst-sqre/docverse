### New features

- `GET /orgs/{org}/projects/{project}` and `GET /orgs/{org}` support conditional GET, joining the project listing. Each sends a weak `ETag` and answers a matching `If-None-Match` with an empty `304 Not Modified` carrying that tag. The single project's tag hashes three clocks, one per row the response draws on: the project's own `date_updated`, its `__main` edition's (the response embeds that edition, so a retitled or republished default edition retires the tag even when the project row did not move), and the organization's (the embedded edition's `published_url` is built from the org's `base_domain`, `url_scheme`, and `root_path_prefix`, all patchable through `PATCH /orgs/{org}`, which writes the org row and nothing else). The organization's own tag hashes its `date_updated`. `GET /orgs` deliberately carries no validator: it is filtered by the caller's memberships, so its content changes when a membership is granted or revoked without any organization's clock moving, and a validator there would hand a poller a 304 over a listing that had in fact changed.

### Other changes

- The `conditional_get` metrics event now also covers the single-project (`endpoint=project`) and organization (`endpoint=organization`) endpoints.
