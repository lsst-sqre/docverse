# Dashboard template sets

A dashboard template set is a directory of Jinja templates, assets, and
a `template.toml` that Docverse renders into three files for every
project: the `/v/` dashboard (`__dashboard.html`), the 404 page
(`__404.html`), and the pydata-sphinx-theme version switcher
(`__switcher.json`). The design behind them is in
[SQR-112](https://sqr-112.lsst.io).

An organization binds a template set in a GitHub repository as its
default with `PUT /orgs/{org}/dashboard-template`, and a project
overrides that default with
`PUT /orgs/{org}/projects/{project}/dashboard-template`. Each binding
names `github_owner`, `github_repo`, `github_ref`, and `root_path`, the
directory that holds `template.toml`. A project with neither binding
renders from the set packaged with the server. A push to the bound ref
that touches `root_path` re-syncs the set and, when its files changed,
rebuilds every project rendering from it; `POST
.../dashboard-template/sync` does the same on demand (see
[GitHub integration](github-integration.md)).

## `template.toml`

```toml
[dashboard]
template = "dashboard.html.jinja"

[dashboard.assets]
css = ["dashboard.css"]
js = ["dashboard.js"]
images = ["logo.svg", "favicon.png"]

[error_404]
template = "404.html.jinja"

[error_404.assets]
css = ["404.css"]

[switcher]
include_kinds = ["main", "release", "major", "alternate"]
template = "switcher.json.jinja"
```

Every key is optional. Paths are relative to the directory holding
`template.toml`.

| Table | Key | Default | Meaning |
| --- | --- | --- | --- |
| `[dashboard]` | `template` | `"dashboard.html.jinja"` | Jinja file rendered into `__dashboard.html`. |
| `[dashboard.assets]` | `css` | `[]` | Stylesheets concatenated, in order, into `assets.css`. |
| `[dashboard.assets]` | `js` | `[]` | Scripts concatenated, in order, into `assets.js`. |
| `[dashboard.assets]` | `images` | `[]` | Images inlined into `assets.images`, keyed by basename with dots and hyphens made underscores (`favicon-32x32.png` → `favicon_32x32_png`): SVG as markup, raster formats as `data:` URIs. |
| `[error_404]` | `template` | `"404.html.jinja"` | Jinja file rendered into `__404.html`. Without an `[error_404]` table, a default 404 page packaged with the server renders instead. |
| `[error_404.assets]` | `css` | `[]` | As `[dashboard.assets]`, for the 404 page only. |
| `[error_404.assets]` | `js` | `[]` | As `[dashboard.assets]`, for the 404 page only. |
| `[error_404.assets]` | `images` | `[]` | As `[dashboard.assets]`, for the 404 page only. |
| `[switcher]` | `include_kinds` | `["main", "release", "major", "alternate"]` | Edition kinds the built-in switcher lists. Ignored when `template` is set. |
| `[switcher]` | `template` | unset | Jinja file that renders `__switcher.json` itself; see [Switcher templates](#switcher-templates). Unset, the built-in switcher renders. |

A sync that cannot parse `template.toml` marks the binding's
`last_sync_status` failed with the parse error in `last_sync_error`,
and projects keep rendering from the last set that synced. The sync
does not check that the files `template.toml` names exist; a missing
file fails the next `dashboard_build` instead.

## Template variables

Every Jinja file in a set — the dashboard, the 404 page, and the
switcher template — renders with the same variables:

- `org` — the organization: `slug`, `title`, `base_domain`.
- `project` — the project: `slug`, `title`, `source_repo_url`, and
  `published_url`, the root URL the project's `__main` serves.
- `editions` — the project's live editions, grouped and sorted:
  `main` (the `__main` edition, or none), `releases` (newest version
  first), `drafts` (most recently updated first), `major` and `minor`
  (newest version first), and `alternates` (by title). Each edition
  has `slug`, `title`, `kind`, `alternate_name`, `date_updated`,
  `published_url`, and `build`, the current build (`slug`, `git_ref`,
  `date`) or none.
- `assets` — the inlined `css`, `js`, and `images` of the file's own
  `assets` table.
- `docverse` — the server: `api_url` and `version`.
- `rendered_at` — when the render started, timezone-aware.

Undefined variables and attributes are errors, not empty strings, so a
typo fails the render rather than shipping a blank. The dashboard and
404 templates are HTML-autoescaped; the switcher template is not.

## Switcher templates

Without `[switcher] template`, Docverse builds `__switcher.json` from
the editions of `include_kinds`: `__main` first, then alternates by
title, then the rest by version, newest first, with `preferred` set on
`__main` and the alternates. A set that needs a different list — a
fixed number of recent releases, editions picked by slug, its own
labels — names a Jinja file in `[switcher] template` instead, and
`include_kinds` is then ignored.

The file must render a JSON array whose every entry is an object with:

- `name` — a string, the label the switcher shows;
- `version` — a string, matched against the page's
  `switcher.version_match`;
- `url` — a string, the edition's root URL;
- `preferred` — optionally, a boolean marking the entry the theme
  steers readers to.

No other key is allowed, so a misspelling such as `prefered` fails
rather than being ignored by the theme. An empty array is valid.
Docverse parses the output and re-serializes it in the built-in
switcher's format, so whitespace and key quoting in the template do not
matter. The template is not HTML-autoescaped: write every string with
the `tojson` filter, which quotes and escapes it as JSON.

```jinja
[
  {"name": "Latest release",
   "version": {{ editions.main.slug | tojson }},
   "url": {{ editions.main.published_url | tojson }},
   "preferred": true}
  {%- for release in editions.releases[:3] %},
  {"name": {{ release.title | tojson }},
   "version": {{ release.slug | tojson }},
   "url": {{ release.published_url | tojson }}}
  {%- endfor %}
]
```

Output that is not such an array raises `InvalidSwitcherJsonError`,
whose message names the template, the project and organization, and
the first problem found (`entry 2 has no 'url'`,
`entry 0 'preferred' is a string, not a boolean`). `dashboard_build`
handles it exactly as it handles a broken dashboard template: it
reports the exception to Sentry, fails the job with the message and
type in the job's `errors` (`GET /orgs/{org}/jobs/{job}`), and uploads
nothing, so the project keeps serving its previous dashboard, 404 page,
and `__switcher.json` until a corrected set syncs.
