/**
 * Route-to-response resolver.
 *
 * Dispatches on `route.kind`:
 *
 * - `edition` routes follow the existing KV → R2 hot path:
 *   1. Look up `{project}/{edition}` in KV to get the build mapping.
 *   2. Construct the R2 object key as `{r2_prefix}{file_path}`.
 *   3. Try exact path, then `{path}/index.html` for directory index
 *      resolution.
 *   4. Return the R2 object with `Content-Type` inferred from the file
 *      extension and `Cache-Control: public, max-age=60`.
 *   5. Route any 404 (missing KV entry, malformed KV JSON, missing R2
 *      object after the index-html fallback) through `notFoundResponse`.
 *
 * - `dashboard` routes delegate to `DashboardStore.getDashboard()`, which
 *   encapsulates the `__`-prefixed R2 key layout. The response body is the
 *   R2 object with `Content-Type: text/html; charset=utf-8` and
 *   `Cache-Control: public, max-age=60`.
 *
 * - `switcher` routes delegate to `DashboardStore.getSwitcher()`, returning
 *   the project's version-switcher JSON with
 *   `Content-Type: application/json; charset=utf-8` and the same
 *   `Cache-Control` as other dashboard-family responses.
 *
 * - `edition_meta` routes delegate to `DashboardStore.getEditionMeta()`,
 *   returning per-edition metadata (canonical URL, `is_canonical`) with the
 *   same JSON `Content-Type` and `Cache-Control` as other dashboard-family
 *   responses.
 *
 * - `redirect` routes return a 301 with `Location` set to `route.to` on the
 *   request's origin, preserving the original query string.
 *
 * 404 handling: every 404-producing branch in this module routes through
 * `notFoundResponse(project, dashboardStore)`, which serves the project's
 * branded `{project}/__404.html` when present and degrades to plain-text
 * `Not Found` otherwise. Both variants apply the same
 * `Cache-Control: public, max-age=60` so dashboard rebuilds and newly
 * published editions become visible within one minute and 404s don't get
 * CDN-stuck. Unroutable requests (no extractable project slug) bypass this
 * helper at the worker entry point, since there's no project to fall back
 * on.
 */

import mime from "mime";
import type { DashboardStore } from "./dashboardStore";
import type { Route, EditionRoute } from "./router";

/** Shape of the JSON value stored in the editions KV namespace. */
interface EditionMapping {
  build_id: string;
  r2_prefix: string;
}

/**
 * Resolve a route to an HTTP response.
 */
export async function resolve(
  route: Route,
  request: Request,
  kv: KVNamespace,
  r2: R2Bucket,
  dashboardStore: DashboardStore,
): Promise<Response> {
  switch (route.kind) {
    case "redirect":
      return resolveRedirect(route.to, request);
    case "dashboard":
      return resolveDashboard(route.project, dashboardStore);
    case "switcher":
      return resolveSwitcher(route.project, dashboardStore);
    case "edition_meta":
      return resolveEditionMeta(route.project, route.edition, dashboardStore);
    case "edition":
      return resolveEdition(route, request, kv, r2, dashboardStore);
    default:
      return route satisfies never;
  }
}

function resolveRedirect(to: string, request: Request): Response {
  const url = new URL(request.url);
  url.pathname = to;
  return Response.redirect(url.toString(), 301);
}

async function resolveDashboard(
  project: string,
  dashboardStore: DashboardStore,
): Promise<Response> {
  const object = await dashboardStore.getDashboard(project);
  if (object === null) {
    return notFoundResponse(project, dashboardStore);
  }
  return new Response(object.body, {
    status: 200,
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      "Content-Length": object.size.toString(),
      "ETag": object.httpEtag,
      "Cache-Control": "public, max-age=60",
    },
  });
}

async function resolveSwitcher(
  project: string,
  dashboardStore: DashboardStore,
): Promise<Response> {
  const object = await dashboardStore.getSwitcher(project);
  if (object === null) {
    return notFoundResponse(project, dashboardStore);
  }
  return new Response(object.body, {
    status: 200,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Content-Length": object.size.toString(),
      "ETag": object.httpEtag,
      "Cache-Control": "public, max-age=60",
    },
  });
}

async function resolveEditionMeta(
  project: string,
  edition: string,
  dashboardStore: DashboardStore,
): Promise<Response> {
  const object = await dashboardStore.getEditionMeta(project, edition);
  if (object === null) {
    return notFoundResponse(project, dashboardStore);
  }
  return new Response(object.body, {
    status: 200,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Content-Length": object.size.toString(),
      "ETag": object.httpEtag,
      "Cache-Control": "public, max-age=60",
    },
  });
}

async function resolveEdition(
  route: EditionRoute,
  request: Request,
  kv: KVNamespace,
  r2: R2Bucket,
  dashboardStore: DashboardStore,
): Promise<Response> {
  // Step 1: Look up the edition mapping in KV
  const kvKey = `${route.project}/${route.edition}`;
  const kvValue = await kv.get(kvKey);
  if (kvValue === null) {
    return notFoundResponse(route.project, dashboardStore);
  }

  let mapping: EditionMapping;
  try {
    mapping = JSON.parse(kvValue);
  } catch {
    return notFoundResponse(route.project, dashboardStore);
  }

  // Normalize r2_prefix to always end with "/"
  const prefix = mapping.r2_prefix.endsWith("/")
    ? mapping.r2_prefix
    : `${mapping.r2_prefix}/`;

  // Step 2: Construct the R2 object key
  const r2Key = `${prefix}${route.path}`;

  // Step 3: Try exact path first
  let object = await r2.get(r2Key);

  // Directory index fallback: try {path}/index.html
  let resolvedPath = route.path;
  if (object === null) {
    const indexKey = r2Key.endsWith("/")
      ? `${r2Key}index.html`
      : `${r2Key}/index.html`;
    object = await r2.get(indexKey);
    if (object !== null) {
      // Redirect to trailing slash so relative links resolve correctly
      if (route.path !== "" && !route.path.endsWith("/")) {
        const url = new URL(request.url);
        url.pathname = `${url.pathname}/`;
        return Response.redirect(url.toString(), 301);
      }
      resolvedPath = indexKey.slice(prefix.length);
    }
  }

  if (object === null) {
    return notFoundResponse(route.project, dashboardStore);
  }

  // Step 4: Infer Content-Type from file extension
  const contentType =
    mime.getType(resolvedPath) ?? "application/octet-stream";

  return new Response(object.body, {
    status: 200,
    headers: {
      "Content-Type": contentType,
      "Content-Length": object.size.toString(),
      "ETag": object.httpEtag,
      "Cache-Control": "public, max-age=60",
    },
  });
}

/**
 * Build a 404 response for a routable request, preferring the project's
 * branded `__404.html` and degrading to plain text on miss. Both variants
 * apply the same `Cache-Control: public, max-age=60` so CDN behavior is
 * uniform across the `/v/` namespace.
 */
export async function notFoundResponse(
  project: string,
  dashboardStore: DashboardStore,
): Promise<Response> {
  const object = await dashboardStore.get404(project);
  if (object !== null) {
    return new Response(object.body, {
      status: 404,
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Content-Length": object.size.toString(),
        "ETag": object.httpEtag,
        "Cache-Control": "public, max-age=60",
      },
    });
  }
  return new Response("Not Found", {
    status: 404,
    headers: {
      "Content-Type": "text/plain",
      "Cache-Control": "public, max-age=60",
    },
  });
}
