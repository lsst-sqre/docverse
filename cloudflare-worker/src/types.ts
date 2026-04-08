import type { UrlScheme } from "./router";

/** Cloudflare Worker environment bindings. */
export interface Env {
  /** KV namespace for edition-to-build mappings. */
  EDITIONS_KV: KVNamespace;

  /** R2 bucket for build artifacts. */
  BUILDS_R2: R2Bucket;

  /** URL routing scheme: "subdomain" or "path-prefix". */
  URL_SCHEME: UrlScheme;

  /**
   * Root path prefix for path-prefix routing (e.g., "/docs/").
   * Only used when URL_SCHEME is "path-prefix".
   */
  PATH_PREFIX?: string;
}
