/** Cloudflare Worker environment bindings. */
export interface Env {
  /** KV namespace for edition-to-build mappings. */
  EDITIONS_KV: KVNamespace;

  /** R2 bucket for build artifacts. */
  BUILDS_R2: R2Bucket;

  /** URL routing scheme: "subdomain" or "path-prefix". */
  URL_SCHEME: string;
}
