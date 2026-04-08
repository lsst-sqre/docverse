/**
 * URL routing module.
 *
 * Parses incoming HTTP requests into a structured route consisting of
 * project slug, edition name, and file path. Supports two URL schemes:
 *
 * - **Subdomain**: project slug from the leftmost subdomain label of the
 *   Host header; edition and path from the URL path.
 * - **Path-prefix**: project slug extracted from the URL path after an
 *   optional configurable root prefix; edition and path from the remaining
 *   path.
 *
 * Edition URL grammar:
 * - `/page.html` or `/` → `__main` edition
 * - `/v/{edition}/page.html` → named edition
 */

/** Result of parsing a request URL. */
export interface Route {
  /** Project slug (e.g., "pipelines"). */
  project: string;

  /** Edition name (e.g., "__main", "v1.0", "main"). */
  edition: string;

  /** File path within the edition (e.g., "page.html", "api/index.html"). */
  path: string;
}

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
 * Parse edition and path from a URL path relative to the project root.
 *
 * Handles the edition URL grammar:
 * - `/v/{edition}/...` → named edition with remaining path
 * - anything else → `__main` edition
 */
function parseEditionAndPath(relativePath: string): {
  edition: string;
  path: string;
} {
  // Remove leading slash
  const stripped = relativePath.startsWith("/")
    ? relativePath.slice(1)
    : relativePath;

  // Check for /v/{edition}/... pattern
  if (stripped.startsWith("v/")) {
    const afterV = stripped.slice(2); // after "v/"
    const slashIndex = afterV.indexOf("/");
    if (slashIndex === -1) {
      // Path is exactly "v/{edition}" with no trailing slash
      const edition = afterV || DEFAULT_EDITION;
      return { edition, path: "" };
    }
    const edition = afterV.slice(0, slashIndex);
    if (edition === "") {
      // Path is "v/..." with empty edition — treat as __main
      return { edition: DEFAULT_EDITION, path: afterV.slice(1) };
    }
    const path = afterV.slice(slashIndex + 1);
    return { edition, path };
  }

  return { edition: DEFAULT_EDITION, path: stripped };
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

  const { edition, path } = parseEditionAndPath(url.pathname);
  return { project, edition, path };
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
    const { edition, path } = parseEditionAndPath("");
    return { project: pathname, edition, path };
  }

  const project = pathname.slice(0, slashIndex);
  if (project === "") {
    return null;
  }

  const rest = pathname.slice(slashIndex); // includes leading "/"
  const { edition, path } = parseEditionAndPath(rest);
  return { project, edition, path };
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
