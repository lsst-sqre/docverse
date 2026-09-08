### New features

- The daily `resource_inventory` census now reports the storage the `purgatory_cleanup` sweep still owes back to the bucket. Both the org- and the project-scoped row carry `purgatory_build_count` and `purgatory_bytes`, counting builds that are soft-deleted but not yet reclaimed, so an operator can see the reap-pending footprint before a sweep runs and watch it fall to zero after one. The pair is the complement of `build_count` and `total_build_bytes` rather than a subset: a build is counted as live or as reap-pending, never both, so adding them gives what the organization occupies today and reading the purgatory pair alone gives what a sweep would return.

### Other changes

- The org-level purgatory roll-up is aggregated in its own query rather than summed over the census's project rows, and it is the one aggregate that deliberately includes soft-deleted projects. A deleted project contributes no project row, and the project-delete cascade is what stamps `date_deleted` on that project's builds — summing the rows would zero out the single largest reclaim the sweep ever makes. The census also ignores the organization's `purgatory_retention`: the gauge answers "what is still on the store", which is what the storage bill is charged for, while "what is eligible tonight" is the sweep's own `purgatory_cleanup_completed` event.
