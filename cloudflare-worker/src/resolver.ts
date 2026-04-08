/**
 * KV-to-R2 resolution module.
 *
 * Given a parsed route (project, edition, file path), this module:
 * 1. Looks up the KV key `{project}/{edition}` to get the build mapping
 * 2. Constructs the R2 object key as `{r2_prefix}{file_path}`
 * 3. Tries exact path, then `{path}/index.html` for directory index resolution
 * 4. Returns the R2 object with Content-Type inferred from the file extension
 *    and Cache-Control: public, max-age=60
 * 5. Returns a plain text 404 for missing KV entries or missing R2 objects
 */

import mime from "mime";
import type { Route } from "./router";

/** Shape of the JSON value stored in the editions KV namespace. */
interface EditionMapping {
  build_id: string;
  r2_prefix: string;
}

/**
 * Resolve a route to an R2 object and return an HTTP response.
 */
export async function resolve(
  route: Route,
  request: Request,
  kv: KVNamespace,
  r2: R2Bucket,
): Promise<Response> {
  // Step 1: Look up the edition mapping in KV
  const kvKey = `${route.project}/${route.edition}`;
  const kvValue = await kv.get(kvKey);
  if (kvValue === null) {
    return new Response("Not Found", {
      status: 404,
      headers: { "Content-Type": "text/plain" },
    });
  }

  let mapping: EditionMapping;
  try {
    mapping = JSON.parse(kvValue);
  } catch {
    return new Response("Not Found", {
      status: 404,
      headers: { "Content-Type": "text/plain" },
    });
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
    return new Response("Not Found", {
      status: 404,
      headers: { "Content-Type": "text/plain" },
    });
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
