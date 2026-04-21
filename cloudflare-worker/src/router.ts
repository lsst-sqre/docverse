/**
 * URL routing module.
 *
 * Parses incoming HTTP requests into a structured route. Supports two
 * URL schemes:
 *
 * - **Subdomain**: project slug from the leftmost subdomain label of the
 *   Host header; remaining classification from the URL path.
 * - **Path-prefix**: project slug extracted from the URL path after an
 *   optional configurable root prefix; remaining classification from the
 *   rest of the path.
 *
 * A parsed route is a discriminated union on the `kind` field:
 *
 * - `{kind: 'edition', project, edition, path}` — a build URL resolved via
 *   the KV → R2 hot path. `/page.html` or `/` maps to the `__main` edition;
 *   `/v/{edition}/...` maps to a named edition.
 * - `{kind: 'dashboard', project}` — the project dashboard page. Matches
 *   exactly `/v/` and `/v/index.html`.
 */

/** Edition (build) route — resolved via KV → R2. */
export interface EditionRoute {
  kind: "edition";
  /** Project slug (e.g., "pipelines"). */
  project: string;
  /** Edition name (e.g., "__main", "v1.0", "main"). */
  edition: string;
  /** File path within the edition (e.g., "page.html", "api/index.html"). */
  path: string;
}

/** Dashboard route — served from `{project}/__dashboard.html` in R2. */
export interface DashboardRoute {
  kind: "dashboard";
  /** Project slug (e.g., "pipelines"). */
  project: string;
}

/** Result of parsing a request URL. */
export type Route = EditionRoute | DashboardRoute;

/** Supported URL routing schemes. */
export type UrlScheme = "subdomain" | "path-prefix";

/** Default edition when no `/v/{edition}/` segment is present. */
const DEFAULT_EDITION = "__main";

/**
 * Parse a request URL into a route.
 *
 * @param request - The incoming HTTP request.
 * @param urlScheme - The URL routing scheme: "subdomain" or "path-prefix".
 * @param pathPrefix - Optional root prefix for path-prefix routing
 *   (e.g., "/docs/"). Ignored for subdomain routing.
 * @returns The parsed route, or null if the URL cannot be routed (e.g.,
 *   missing project slug).
 */
export function parseRoute(
  request: Request,
  urlScheme: UrlScheme,
  pathPrefix?: string,
): Route | null {
  switch (urlScheme) {
    case "subdomain":
      return parseSubdomainRoute(request);
    case "path-prefix":
      return parsePathPrefixRoute(request, pathPrefix);
    default:
      throw new Error(`Unknown URL scheme: ${urlScheme as string}`);
  }
}

/**
 * Classify a project-relative path into a Route.
 *
 * Classification order:
 * 1. `v/` or `v/index.html` → dashboard route.
 * 2. Anything else starting with `v/` → named-edition route.
 * 3. Anything else → `__main`-edition route.
 */
function classifyRelativePath(project: string, relativePath: string): Route {
  const stripped = relativePath.startsWith("/")
    ? relativePath.slice(1)
    : relativePath;

  if (stripped === "v/" || stripped === "v/index.html") {
    return { kind: "dashboard", project };
  }

  if (stripped.startsWith("v/")) {
    const afterV = stripped.slice(2); // after "v/"
    const slashIndex = afterV.indexOf("/");
    if (slashIndex === -1) {
      // Path is exactly "v/{edition}" with no trailing slash
      const edition = afterV || DEFAULT_EDITION;
      return { kind: "edition", project, edition, path: "" };
    }
    const edition = afterV.slice(0, slashIndex);
    if (edition === "") {
      // Path is "v//..." with empty edition — treat as __main
      return {
        kind: "edition",
        project,
        edition: DEFAULT_EDITION,
        path: afterV.slice(1),
      };
    }
    const path = afterV.slice(slashIndex + 1);
    return { kind: "edition", project, edition, path };
  }

  return { kind: "edition", project, edition: DEFAULT_EDITION, path: stripped };
}

function parseSubdomainRoute(request: Request): Route | null {
  const url = new URL(request.url);
  const host = url.hostname;

  // Extract project slug from the leftmost subdomain label.
  // E.g., "pipelines.lsst.io" → "pipelines"
  const dotIndex = host.indexOf(".");
  if (dotIndex === -1) {
    // No subdomain — cannot route
    return null;
  }
  const project = host.slice(0, dotIndex);
  if (project === "") {
    return null;
  }

  return classifyRelativePath(project, url.pathname);
}

function parsePathPrefixRoute(
  request: Request,
  pathPrefix?: string,
): Route | null {
  const url = new URL(request.url);
  let pathname = url.pathname;

  // Strip the configured root prefix if present
  if (pathPrefix) {
    const normalizedPrefix = normalizePrefix(pathPrefix);
    if (!pathname.startsWith(normalizedPrefix)) {
      return null;
    }
    pathname = pathname.slice(normalizedPrefix.length);
  }

  // Remove leading slash for splitting
  if (pathname.startsWith("/")) {
    pathname = pathname.slice(1);
  }

  // First segment is the project slug
  const slashIndex = pathname.indexOf("/");
  if (slashIndex === -1) {
    // Path is just the project slug with no trailing slash
    if (pathname === "") {
      return null;
    }
    return classifyRelativePath(pathname, "");
  }

  const project = pathname.slice(0, slashIndex);
  if (project === "") {
    return null;
  }

  const rest = pathname.slice(slashIndex); // includes leading "/"
  return classifyRelativePath(project, rest);
}

/**
 * Normalize a path prefix to ensure it starts and ends with "/".
 */
function normalizePrefix(prefix: string): string {
  let result = prefix;
  if (!result.startsWith("/")) {
    result = "/" + result;
  }
  if (!result.endsWith("/")) {
    result = result + "/";
  }
  return result;
}
