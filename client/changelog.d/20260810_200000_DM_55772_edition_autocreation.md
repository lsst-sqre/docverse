### New features

- Added `EditionAutocreationConfig` (exported from `docverse.models`), the shape of the new `edition_autocreation` field on the `Organization`, `OrganizationUpdate`, `Project`, and `ProjectUpdate` models. It carries a single knob for now, `semver_aggregates` (default `true`), controlling whether a stable semver release also auto-creates its major (`N`) and minor (`N.M`) aggregate editions. The model forbids unknown keys, so a typo in a `PATCH` payload fails validation client-side rather than silently doing nothing.
