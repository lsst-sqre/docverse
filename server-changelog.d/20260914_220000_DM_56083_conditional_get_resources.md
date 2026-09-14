### New features

- `GET /orgs/{org}/projects/{project}` and `GET /orgs/{org}` support conditional GET, joining the project listing. Each sends a weak `ETag` and a `Last-Modified`, and answers a matching `If-None-Match` — or an `If-Modified-Since` at or after the watermark — with an empty `304 Not Modified` carrying both validators. The single project's watermark is the later of the project's own `date_updated` and its `__main` edition's, because the response embeds that edition: a retitled or republished default edition retires the tag even when the project row itself did not move. The organization's watermark is its own `date_updated`. `GET /orgs` deliberately carries no validators: it is filtered by the caller's memberships, so its content changes when a membership is granted or revoked without any organization's clock moving, and a date watermark there would hand a poller a 304 over a listing that had in fact changed.

### Other changes

- The `conditional_get` metrics event now also covers the single-project (`endpoint=project`) and organization (`endpoint=organization`) endpoints.
