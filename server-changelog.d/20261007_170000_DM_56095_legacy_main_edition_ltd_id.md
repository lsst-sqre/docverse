### Other changes

- The `keeper_sync_tier_main` cron now drops the retired `main_edition_ltd_id` annotation from a project's keeper-sync state row on each poll. Earlier releases wrote it beside the cached `main_edition_url` pointer and nothing reads it, but the annotation merge would have carried it forward indefinitely, where a later re-resolve of the URL would leave it naming a different edition.
