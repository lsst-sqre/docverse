### Backwards-incompatible changes

- `POST /orgs/{org}/keeper-sync/scope-preview` no longer accepts `ltd_base_url` in its request body; a body that sets it is a 422 naming the field, and nothing is fetched. The preview always resolves the scope against the LTD instance named by the *stored* config (or the model default when none is stored). Merging a candidate base URL in before the fetch made the endpoint an interactive status oracle: an org admin could aim it at an in-cluster service name or `http://169.254.169.254/` and read the upstream status and failure class back out of the 502. Repointing the LTD instance remains a `PUT` or `PATCH` on `/orgs/{org}/keeper-sync`. Every other field of the body is unchanged, and the published request schema no longer advertises `ltd_base_url`.

### Bug fixes

- The scope preview no longer holds a database transaction open while it waits on LTD. It previously read the organization row — opening the transaction — and then awaited the LTD product listing inside it, pinning a pooled Postgres connection `idle in transaction` for the whole httpx timeout, exactly when a slow LTD has operators retrying previews. The endpoint now takes one short transaction to read the config, fetches with none open, and takes a second to read the keeper-sync state rows.
