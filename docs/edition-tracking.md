# Edition tracking

When a build completes, `build_processing` hands it to edition tracking
(`EditionTrackingService.track_build` in
`src/docverse_server/services/edition_tracking.py`), which decides
which editions the build advances and which editions it creates. The
`build_processing` worker then enqueues a `publish_edition` job for
every edition the build advanced or created.

This page is for operators: how a build finds its editions, when it
gets an edition of its own, and what an alias edition such as
`__main`, `weekly` or `latest` does alongside it.

## Matched editions

Every live edition of the project whose tracking mode matches the
build's git ref is matched:

| Tracking mode | Matches a build on |
| --- | --- |
| `git_ref` | Exactly `tracking_params.git_ref` |
| `alternate_git_ref` | Exactly `tracking_params.git_ref`, with the edition's alternate name |
| `eups_release` | A final EUPS release (`v30_0_13`, `v17_0`), never a release candidate |
| `eups_major_release` | A two-component EUPS release (`v12_0`) |
| `eups_weekly_release` | An EUPS weekly (`w_2026_46`) |
| `eups_daily_release` | An EUPS daily (`d_2026_11_05`) |
| `semver_release` | A stable semver tag (`v1.0.0`) |
| `semver_major`, `semver_minor` | A stable semver tag in the edition's major (or major and minor) line |
| `lsst_doc` | A `vX.Y` tag, or the default branch until the first tag is published |

A matched edition in a version mode is advanced only when the build's
version is at least the one it serves; a `git_ref` edition is advanced
unless the build is older than the one it serves.

## The ref's own edition

The build's ref also derives an edition slug, through the project's
slug rewrite rules (or the organization's), then the built-in version
rules, then the fallback that turns `/` into `-`. The derived edition
tracks that literal ref in `git_ref` mode (`alternate_git_ref` for a
build with an alternate name) and takes the derived kind: `release`
for a ref a version rule recognises, `draft` otherwise.

Tracking auto-creates that edition **unless a matched edition already
owns the ref**. A matched edition owns it when it either:

- tracks the literal ref, in `git_ref` or `alternate_git_ref` mode, or
- already holds the derived slug (compared case-insensitively).

A match in any other mode is an **alias**: it follows a whole family of
refs, the newest release or the newest weekly, and owns none of them.
An alias is advanced *beside* the ref's own edition, never instead of
it, so every release, weekly and daily keeps its own `/v/<slug>/`
edition, as it did on LTD.

| Build ref | Matched editions | The ref's own edition |
| --- | --- | --- |
| `main` | `__main` (`git_ref` on `main`) | None: `__main` owns `main` |
| `v30_0_13` | `__main` (`eups_release`) | `v30_0_13`, release |
| `v30_0_12_rc3` | None: rcs never match `eups_release` | `v30_0_12_rc3`, draft |
| `w_2026_46` | `weekly` (`eups_weekly_release`) | `w_2026_46`, release |
| `d_2026_11_05` | `daily` (`eups_daily_release`) | `d_2026_11_05`, draft |
| `v1.0.0` | `latest` (`semver_release`), `1`, `1.0` | `v1.0.0`, release |
| `v1.0` | `current` (`lsst_doc`) | `v1.0`, release |
| `main` | `__main` (`lsst_doc`, no tag yet) | `main`, draft |

The ownership rule is what keeps `__main` on `git_ref` from growing a
twin: a `main` build advances `__main` and creates nothing. When the
default branch is renamed, builds on the new name stop matching
`__main` and get their own draft instead, until the default-branch rule
rewrites `__main` and retires that draft (see
[The default branch](github-integration.md#the-default-branch)).
An `lsst_doc` `__main`, by contrast, follows the default branch only
until a version tag lands, so it does not own the branch: each
default-branch build also advances the branch's draft.

There is no setting to turn this off. A ref that should never get an
edition takes an `ignore` slug rewrite rule, which suppresses the build
from tracking altogether.

### Existing, deleted and reserved slugs

- **A live edition already holds the slug.** It is reused, never
  duplicated: a matched one is advanced once, and an unmatched one is
  pointed at the build as the ref's edition. Either way it already
  existed, so it is reported as `updated`.
- **A lifecycle rule deleted the edition.** It is revived in place,
  with its id and build history, and reported as created (see
  [A build for the ref revives the edition](github-integration.md#a-build-for-the-ref-revives-the-edition)).
- **An operator deleted the edition.** The delete reserves the slug,
  so the ref gets no edition and tracking logs `Edition slug reserved
  by manual delete; edition not recreated` at warning level. The
  aliases the build matched still advance as usual.

### Semver aggregates

A stable semver tag also auto-creates its `N` (`semver_major`) and
`N.M` (`semver_minor`) aggregate editions, unless the project's
`edition_autocreation` (or, when the project sets none, the
organization's) has `semver_aggregates` set to `false`. The aggregates
are aliases too, so a project that already has them still gets the
tag's own edition.

## Reading an outcome

`track_build` returns one outcome per edition it touched:

- `created`: the ref's own edition (or an aggregate) was created or
  revived and points at the build. Revived editions are also listed in
  the result's `revived`, and the worker publishes an
  `edition_lifecycle` event with `action` `restore` for each.
- `updated`: an existing edition advanced to the build, including an
  unmatched one reused because it already held the derived slug.
- `skipped`: the version or date guard kept the edition on its current
  build.

An alias that a build does not match, such as `__main` in
`eups_release` for a weekly, has no outcome at all.

Keeper-sync does not go through this path: LTD already supplies a
per-ref edition for every build it imports.
