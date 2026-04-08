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

import { env, SELF } from "cloudflare:test";
import { describe, it, expect, beforeEach } from "vitest";
import worker from "../src/index";

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
});

describe("Worker integration — path-prefix routing", () => {
  const PROJECT = "pipelines";
  const BUILD_ID = "b42";
  const R2_PREFIX = `${PROJECT}/__builds/${BUILD_ID}/`;

  /**
   * Call the worker directly with path-prefix env overrides.
   */
  async function fetchPathPrefix(
    url: string,
    init?: RequestInit,
  ): Promise<Response> {
    const request = new Request(url, init);
    return worker.fetch(
      request,
      {
        ...env,
        URL_SCHEME: "path-prefix" as const,
        PATH_PREFIX: "/docs/",
      },
      { waitUntil: () => {}, passThroughOnException: () => {} } as unknown as ExecutionContext,
    );
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
});
