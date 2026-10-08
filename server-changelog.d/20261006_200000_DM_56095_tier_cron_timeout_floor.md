### Bug fixes

- A keeper-sync tier cron's derived arq timeout never falls below arq's 300 s default, nor exceeds the tier's own interval. `keeper_sync_tier_main`, whose 5-minute interval less the one-minute margin would otherwise leave 240 s, keeps the full 300 s it ran under before, so a main pass that needs between 240 s and 300 s is not cancelled and restarted from the top of its scope on every tick. The discovery and other tiers keep 1500 s and 3000 s.
