### Other changes

- The `KeeperSyncPushedRef` schema description, and that of `KeeperSyncProjectStatus.pushed_refs`, now say that a pushed ref's stamp is kept until its push window closes, however many syncs `tier_main` enqueued for it, so a ref still listed `in_window` after a successful sync is waiting for its window, not stuck. Before, they said a ref left the list once its sync was enqueued. The `KeeperSyncProjectStateSummary.annotations` description names the push hot path's two annotations, `github_pushed_refs` and `github_pushed_refs_enqueued`.
