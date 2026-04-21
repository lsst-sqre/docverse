/**
 * Integration tests for the Worker fetch handler.
 *
 * These tests run inside a Miniflare-backed workerd runtime via
 * @cloudflare/vitest-pool-workers. `SELF` sends real HTTP requests through
 * the Worker's fetch handler, and `env` provides KV/R2 bindings backed by
 * Miniflare's local in-memory simulations — not mocks. Each test seeds
 * local KV and R2 with test data and asserts on the full HTTP response.
 *
 * Path-prefix routing tests call `worker.fetch()` directly (rather than
 * `SELF.fetch()`) so they can override `URL_SCHEME` while reusing the
 * same Miniflare-backed KV and R2 from `env`.
 */

import {
  env,
  SELF,
  createExecutionContext,
  waitOnExecutionContext,
} from "cloudflare:test";
import { describe, it, expect, beforeEach, vi } from "vitest";
import worker from "../src/index";
import { notFoundCacheKey } from "../src/resolver";

/**
 * Projects that integration tests route through `notFoundResponse`.
 * `caches.default` persists across tests inside a single Miniflare run,
 * so evict each project's cached 404 entry before every test.
 */
const NOT_FOUND_TEST_PROJECTS = [
  "pipelines",
  "sqr-112",
  "cache-hit-integration",
];

beforeEach(async () => {
  for (const project of NOT_FOUND_TEST_PROJECTS) {
    await caches.default.delete(notFoundCacheKey(project));
  }
});

/**
 * Helper to seed KV with an edition mapping.
 */
async function seedEdition(
  project: string,
  edition: string,
  buildId: string,
  r2Prefix: string,
): Promise<void> {
  const kvKey = `${project}/${edition}`;
  const kvValue = JSON.stringify({ build_id: buildId, r2_prefix: r2Prefix });
  await env.EDITIONS_KV.put(kvKey, kvValue);
}

/**
 * Helper to seed R2 with a test object.
 */
async function seedR2Object(
  key: string,
  body: string,
): Promise<void> {
  await env.BUILDS_R2.put(key, body);
}

describe("Worker integration — subdomain routing", () => {
  const PROJECT = "pipelines";
  const BUILD_ID = "b42";
  const R2_PREFIX = `${PROJECT}/__builds/${BUILD_ID}/`;

  beforeEach(async () => {
    // Seed the __main edition mapping
    await seedEdition(PROJECT, "__main", BUILD_ID, R2_PREFIX);
    // Seed a named edition
    await seedEdition(PROJECT, "v1.0", BUILD_ID, R2_PREFIX);
    // Seed R2 objects
    await seedR2Object(`${R2_PREFIX}index.html`, "<html>root</html>");
    await seedR2Object(
      `${R2_PREFIX}getting-started.html`,
      "<html>getting started</html>",
    );
    await seedR2Object(`${R2_PREFIX}api/core/index.html`, "<html>core</html>");
    await seedR2Object(`${R2_PREFIX}style.css`, "body { color: red; }");
  });

  it("serves the default __main edition for root path", async () => {
    const response = await SELF.fetch("https://pipelines.lsst.io/");

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("text/html");
    expect(response.headers.get("Cache-Control")).toBe("public, max-age=60");
    expect(await response.text()).toBe("<html>root</html>");
  });

  it("serves a specific page under __main", async () => {
    const response = await SELF.fetch(
      "https://pipelines.lsst.io/getting-started.html",
    );

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("text/html");
    expect(await response.text()).toBe("<html>getting started</html>");
  });

  it("serves a named edition via /v/{edition}/", async () => {
    const response = await SELF.fetch("https://pipelines.lsst.io/v/v1.0/");

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("text/html");
    expect(await response.text()).toBe("<html>root</html>");
  });

  it("serves directory index.html automatically", async () => {
    const response = await SELF.fetch(
      "https://pipelines.lsst.io/api/core/",
    );

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("text/html");
    expect(await response.text()).toBe("<html>core</html>");
  });

  it("redirects directory path without trailing slash", async () => {
    const response = await SELF.fetch(
      "https://pipelines.lsst.io/api/core",
      { redirect: "manual" },
    );

    expect(response.status).toBe(301);
    expect(response.headers.get("Location")).toBe(
      "https://pipelines.lsst.io/api/core/",
    );
  });

  it("infers correct Content-Type for CSS", async () => {
    const response = await SELF.fetch(
      "https://pipelines.lsst.io/style.css",
    );

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("text/css");
    expect(await response.text()).toBe("body { color: red; }");
  });

  it("returns 404 for missing path", async () => {
    const response = await SELF.fetch(
      "https://pipelines.lsst.io/nonexistent.html",
    );

    expect(response.status).toBe(404);
    expect(response.headers.get("Content-Type")).toBe("text/plain");
  });

  it("returns 404 for missing edition", async () => {
    const response = await SELF.fetch(
      "https://pipelines.lsst.io/v/nonexistent/",
    );

    expect(response.status).toBe(404);
    expect(response.headers.get("Content-Type")).toBe("text/plain");
  });

  it("returns 404 when route cannot be parsed (bare domain)", async () => {
    const response = await SELF.fetch("https://localhost/");

    expect(response.status).toBe(404);
    expect(response.headers.get("Content-Type")).toBe("text/plain");
  });

  it("serves the project dashboard at /v/ from __dashboard.html", async () => {
    await seedR2Object(
      "sqr-112/__dashboard.html",
      "<html>dashboard</html>",
    );

    const response = await SELF.fetch("https://sqr-112.lsst.io/v/");

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe(
      "text/html; charset=utf-8",
    );
    expect(response.headers.get("Cache-Control")).toBe("public, max-age=60");
    expect(await response.text()).toBe("<html>dashboard</html>");
  });

  it("dashboard response delivers the full body size without an explicit mismatching Content-Length", async () => {
    // Regression guard for the PR #202 review finding: the worker used to
    // manually set `Content-Length: object.size` on dashboard-family
    // responses. That explicit header can disagree with the actual bytes
    // Cloudflare's edge sends (e.g. when gzip is applied) and hang the
    // response. After removing the manual header, any Content-Length the
    // response carries must be the runtime's own — so if it is present it
    // must equal the delivered body size, and the body must be fully
    // streamed without truncation.
    const body = "<html>dashboard with a distinctive length</html>";
    const expectedSize = new TextEncoder().encode(body).byteLength;
    await seedR2Object("sqr-112/__dashboard.html", body);

    const response = await SELF.fetch("https://sqr-112.lsst.io/v/");

    expect(response.status).toBe(200);
    const contentLength = response.headers.get("content-length");
    if (contentLength !== null) {
      expect(Number(contentLength)).toBe(expectedSize);
    }
    expect((await response.arrayBuffer()).byteLength).toBe(expectedSize);
  });

  it("serves the dashboard at /v/index.html", async () => {
    await seedR2Object(
      "sqr-112/__dashboard.html",
      "<html>dashboard</html>",
    );

    const response = await SELF.fetch(
      "https://sqr-112.lsst.io/v/index.html",
    );

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe(
      "text/html; charset=utf-8",
    );
    expect(await response.text()).toBe("<html>dashboard</html>");
  });

  it("serves the version switcher at /v/switcher.json from __switcher.json", async () => {
    await seedR2Object(
      "sqr-112/__switcher.json",
      '[{"name":"main","url":"https://sqr-112.lsst.io/v/main/"}]',
    );

    const response = await SELF.fetch(
      "https://sqr-112.lsst.io/v/switcher.json",
    );

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe(
      "application/json; charset=utf-8",
    );
    expect(response.headers.get("Cache-Control")).toBe("public, max-age=60");
    expect(await response.text()).toBe(
      '[{"name":"main","url":"https://sqr-112.lsst.io/v/main/"}]',
    );
  });

  it("301-redirects /v (no trailing slash) to /v/", async () => {
    const response = await SELF.fetch("https://sqr-112.lsst.io/v", {
      redirect: "manual",
    });

    expect(response.status).toBe(301);
    expect(new URL(response.headers.get("Location") ?? "").pathname).toBe(
      "/v/",
    );
  });

  it("serves the branded __404.html for an unknown edition", async () => {
    await seedR2Object(
      "pipelines/__404.html",
      "<html>branded 404 page</html>",
    );

    const response = await SELF.fetch(
      "https://pipelines.lsst.io/v/nonexistent/",
    );

    expect(response.status).toBe(404);
    expect(response.headers.get("Content-Type")).toBe(
      "text/html; charset=utf-8",
    );
    expect(response.headers.get("Cache-Control")).toBe("public, max-age=60");
    expect(await response.text()).toBe("<html>branded 404 page</html>");
  });

  it("serves a cached 404 on the second request without re-reading __404.html from R2", async () => {
    // Seed a branded 404 and spy on BUILDS_R2.get so we can count how many
    // times the worker reaches into R2 for the __404.html key across two
    // successive requests. The first request populates caches.default;
    // the second request must hit the cache and skip the R2 GET entirely.
    //
    // We use worker.fetch() with createExecutionContext() +
    // waitOnExecutionContext() rather than SELF.fetch so the first
    // request's ctx.waitUntil(caches.default.put(...)) is observable and
    // drained before the second request runs — otherwise the put can
    // race with the cache read in request #2 and the test flakes.
    await seedR2Object(
      "cache-hit-integration/__404.html",
      "<html>cached 404 body</html>",
    );
    const r2Get = vi.spyOn(env.BUILDS_R2, "get");

    const ctx1 = createExecutionContext();
    const first = await worker.fetch(
      new Request(
        "https://cache-hit-integration.lsst.io/nonexistent.html",
      ),
      env,
      ctx1,
    );
    await first.arrayBuffer();
    await waitOnExecutionContext(ctx1);
    const firstHitCount = r2Get.mock.calls.filter(
      (call: unknown[]) => call[0] === "cache-hit-integration/__404.html",
    ).length;

    const ctx2 = createExecutionContext();
    const second = await worker.fetch(
      new Request(
        "https://cache-hit-integration.lsst.io/another-missing.html",
      ),
      env,
      ctx2,
    );
    await waitOnExecutionContext(ctx2);
    const secondHitCount = r2Get.mock.calls.filter(
      (call: unknown[]) => call[0] === "cache-hit-integration/__404.html",
    ).length;

    expect(first.status).toBe(404);
    expect(second.status).toBe(404);
    expect(second.headers.get("Content-Type")).toBe(
      "text/html; charset=utf-8",
    );
    expect(second.headers.get("Cache-Control")).toBe("public, max-age=60");
    expect(await second.text()).toBe("<html>cached 404 body</html>");
    // First request populated the cache — one R2 GET for __404.html.
    expect(firstHitCount).toBe(1);
    // Second request was served from caches.default — no additional GET.
    expect(secondHitCount).toBe(1);

    r2Get.mockRestore();
  });
});

describe("Worker integration — path-prefix routing", () => {
  const PROJECT = "pipelines";
  const BUILD_ID = "b42";
  const R2_PREFIX = `${PROJECT}/__builds/${BUILD_ID}/`;

  /**
   * Call the worker directly with path-prefix env overrides.
   *
   * Uses `createExecutionContext()` + `waitOnExecutionContext()` so any
   * `ctx.waitUntil(caches.default.put(...))` kicked off inside the worker
   * has settled before this helper returns. Otherwise a prior test's 404
   * cache write could race with the next test's `beforeEach` eviction and
   * re-poison `caches.default` for the shared project slug.
   */
  async function fetchPathPrefix(
    url: string,
    init?: RequestInit,
  ): Promise<Response> {
    const request = new Request(url, init);
    const ctx = createExecutionContext();
    const response = await worker.fetch(
      request,
      {
        ...env,
        URL_SCHEME: "path-prefix" as const,
        PATH_PREFIX: "/docs/",
      },
      ctx,
    );
    await waitOnExecutionContext(ctx);
    return response;
  }

  beforeEach(async () => {
    await seedEdition(PROJECT, "__main", BUILD_ID, R2_PREFIX);
    await seedEdition(PROJECT, "main", BUILD_ID, R2_PREFIX);
    await seedR2Object(`${R2_PREFIX}index.html`, "<html>root</html>");
    await seedR2Object(
      `${R2_PREFIX}getting-started.html`,
      "<html>getting started</html>",
    );
    await seedR2Object(`${R2_PREFIX}api/core/index.html`, "<html>core</html>");
  });

  it("serves __main edition at /docs/{project}/", async () => {
    const response = await fetchPathPrefix(
      "https://docs.example.com/docs/pipelines/",
    );

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("text/html");
    expect(await response.text()).toBe("<html>root</html>");
  });

  it("serves a page under __main", async () => {
    const response = await fetchPathPrefix(
      "https://docs.example.com/docs/pipelines/getting-started.html",
    );

    expect(response.status).toBe(200);
    expect(await response.text()).toBe("<html>getting started</html>");
  });

  it("serves a named edition via /docs/{project}/v/{edition}/", async () => {
    const response = await fetchPathPrefix(
      "https://docs.example.com/docs/pipelines/v/main/",
    );

    expect(response.status).toBe(200);
    expect(await response.text()).toBe("<html>root</html>");
  });

  it("serves directory index automatically", async () => {
    const response = await fetchPathPrefix(
      "https://docs.example.com/docs/pipelines/api/core/",
    );

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("text/html");
    expect(await response.text()).toBe("<html>core</html>");
  });

  it("returns 404 when prefix does not match", async () => {
    const response = await fetchPathPrefix(
      "https://docs.example.com/other/pipelines/",
    );

    expect(response.status).toBe(404);
  });

  it("returns 404 for missing path", async () => {
    const response = await fetchPathPrefix(
      "https://docs.example.com/docs/pipelines/nonexistent.html",
    );

    expect(response.status).toBe(404);
  });

  it("serves the project dashboard at /docs/{project}/v/", async () => {
    await seedR2Object(
      "sqr-112/__dashboard.html",
      "<html>dashboard</html>",
    );

    const response = await fetchPathPrefix(
      "https://docs.example.com/docs/sqr-112/v/",
    );

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe(
      "text/html; charset=utf-8",
    );
    expect(response.headers.get("Cache-Control")).toBe("public, max-age=60");
    expect(await response.text()).toBe("<html>dashboard</html>");
  });

  it("serves the switcher at /docs/{project}/v/switcher.json", async () => {
    await seedR2Object(
      "sqr-112/__switcher.json",
      '[{"name":"main","url":"/docs/sqr-112/v/main/"}]',
    );

    const response = await fetchPathPrefix(
      "https://docs.example.com/docs/sqr-112/v/switcher.json",
    );

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe(
      "application/json; charset=utf-8",
    );
    expect(response.headers.get("Cache-Control")).toBe("public, max-age=60");
    expect(await response.text()).toBe(
      '[{"name":"main","url":"/docs/sqr-112/v/main/"}]',
    );
  });

  it("serves edition metadata at /docs/{project}/v/{edition}/_docverse.json", async () => {
    await seedR2Object(
      "sqr-112/__editions/main.json",
      '{"canonical_url":"https://docs.example.com/docs/sqr-112/","is_canonical":true}',
    );

    const response = await fetchPathPrefix(
      "https://docs.example.com/docs/sqr-112/v/main/_docverse.json",
    );

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe(
      "application/json; charset=utf-8",
    );
    expect(response.headers.get("Cache-Control")).toBe("public, max-age=60");
    expect(await response.text()).toBe(
      '{"canonical_url":"https://docs.example.com/docs/sqr-112/","is_canonical":true}',
    );
  });
});
