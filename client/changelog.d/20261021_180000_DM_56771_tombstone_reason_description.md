### Other changes

- The `KeeperSyncTombstoneReason` schema description now says how long each tombstone holds: `manual_delete` is a permanent veto until an operator clears the tombstone or restores the edition, while `lifecycle_delete` and `lifecycle_preemptive` are judged again when LTD rebuilds the edition and cleared once no lifecycle rule matches it.
