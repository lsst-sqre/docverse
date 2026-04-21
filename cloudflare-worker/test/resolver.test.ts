import { describe, it, expect, vi } from "vitest";
import { resolve } from "../src/resolver";
import type { DashboardStore } from "../src/dashboardStore";
import type { Route } from "../src/router";

/**
 * Create a mock KV namespace.
 */
function createMockKV(
  store: Record<string, string> = {},
): KVNamespace {
  return {
    get: vi.fn(async (key: string) => store[key] ?? null),
    put: vi.fn(),
    delete: vi.fn(),
    list: vi.fn(),
    getWithMetadata: vi.fn(),
  } as unknown as KVNamespace;
}

/**
 * Create a mock R2 bucket.
 */
function createMockR2(
  store: Record<string, { body: ReadableStream; size: number; httpMetadata?: R2HTTPMetadata }> = {},
): R2Bucket {
  return {
    get: vi.fn(async (key: string) => {
      const obj = store[key];
      if (!obj) return null;
      return {
        body: obj.body,
        size: obj.size,
        httpEtag: `"${key}-etag"`,
        httpMetadata: obj.httpMetadata ?? {},
      } as R2ObjectBody;
    }),
    put: vi.fn(),
    delete: vi.fn(),
    list: vi.fn(),
    head: vi.fn(),
    createMultipartUpload: vi.fn(),
    resumeMultipartUpload: vi.fn(),
  } as unknown as R2Bucket;
}

/**
 * Create a mock DashboardStore backed by in-memory maps keyed by project
 * (for dashboard/switcher) or `{project}/{edition}` (for edition_meta).
 */
function createMockDashboardStore(
  dashboards: Record<string, { body: ReadableStream; size: number }> = {},
  switchers: Record<string, { body: ReadableStream; size: number }> = {},
  editionMetas: Record<string, { body: ReadableStream; size: number }> = {},
  notFounds: Record<string, { body: ReadableStream; size: number }> = {},
): DashboardStore {
  return {
    getDashboard: vi.fn(async (project: string) => {
      const obj = dashboards[project];
      if (!obj) return null;
      return {
        body: obj.body,
        size: obj.size,
        httpEtag: `"${project}-dashboard-etag"`,
        httpMetadata: {},
      } as R2ObjectBody;
    }),
    getSwitcher: vi.fn(async (project: string) => {
      const obj = switchers[project];
      if (!obj) return null;
      return {
        body: obj.body,
        size: obj.size,
        httpEtag: `"${project}-switcher-etag"`,
        httpMetadata: {},
      } as R2ObjectBody;
    }),
    getEditionMeta: vi.fn(async (project: string, edition: string) => {
      const obj = editionMetas[`${project}/${edition}`];
      if (!obj) return null;
      return {
        body: obj.body,
        size: obj.size,
        httpEtag: `"${project}-${edition}-edition-meta-etag"`,
        httpMetadata: {},
      } as R2ObjectBody;
    }),
    get404: vi.fn(async (project: string) => {
      const obj = notFounds[project];
      if (!obj) return null;
      return {
        body: obj.body,
        size: obj.size,
        httpEtag: `"${project}-404-etag"`,
        httpMetadata: {},
      } as R2ObjectBody;
    }),
  };
}

/**
 * Helper to create a ReadableStream from a string.
 */
function streamFromString(s: string): ReadableStream {
  return new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode(s));
      controller.close();
    },
  });
}

describe("resolve — edition routes", () => {
  const route: Route = {
    kind: "edition",
    project: "pipelines",
    edition: "__main",
    path: "getting-started.html",
  };

  it("returns R2 object with correct Content-Type and Cache-Control", async () => {
    const kv = createMockKV({
      "pipelines/__main": JSON.stringify({
        build_id: "b123",
        r2_prefix: "pipelines/__main/b123/",
      }),
    });
    const r2 = createMockR2({
      "pipelines/__main/b123/getting-started.html": {
        body: streamFromString("<html>hello</html>"),
        size: 18,
      },
    });
    const dashboardStore = createMockDashboardStore();
    const request = new Request("https://example.com/pipelines/getting-started.html");

    const response = await resolve(route, request, kv, r2, dashboardStore);

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("text/html");
    expect(response.headers.get("ETag")).toBe(
      '"pipelines/__main/b123/getting-started.html-etag"',
    );
    expect(response.headers.get("Cache-Control")).toBe(
      "public, max-age=60",
    );
    expect(await response.text()).toBe("<html>hello</html>");
  });

  it("returns 404 when KV entry is missing", async () => {
    const kv = createMockKV({});
    const r2 = createMockR2({});
    const dashboardStore = createMockDashboardStore();
    const request = new Request("https://example.com/pipelines/getting-started.html");

    const response = await resolve(route, request, kv, r2, dashboardStore);

    expect(response.status).toBe(404);
    expect(response.headers.get("Content-Type")).toBe("text/plain");
    const body = await response.text();
    expect(body).toContain("Not Found");
  });

  it("returns 404 when KV value is malformed JSON", async () => {
    const kv = createMockKV({
      "pipelines/__main": "not-json",
    });
    const r2 = createMockR2({});
    const dashboardStore = createMockDashboardStore();
    const request = new Request("https://example.com/pipelines/getting-started.html");

    const response = await resolve(route, request, kv, r2, dashboardStore);

    expect(response.status).toBe(404);
    expect(response.headers.get("Content-Type")).toBe("text/plain");
    const body = await response.text();
    expect(body).toContain("Not Found");
  });

  it("returns 404 when R2 object is missing", async () => {
    const kv = createMockKV({
      "pipelines/__main": JSON.stringify({
        build_id: "b123",
        r2_prefix: "pipelines/__main/b123/",
      }),
    });
    const r2 = createMockR2({});
    const dashboardStore = createMockDashboardStore();
    const request = new Request("https://example.com/pipelines/getting-started.html");

    const response = await resolve(route, request, kv, r2, dashboardStore);

    expect(response.status).toBe(404);
    expect(response.headers.get("Content-Type")).toBe("text/plain");
    const body = await response.text();
    expect(body).toContain("Not Found");
  });

  it("redirects to trailing slash for directory paths", async () => {
    const directoryRoute: Route = {
      kind: "edition",
      project: "pipelines",
      edition: "__main",
      path: "api/core",
    };
    const kv = createMockKV({
      "pipelines/__main": JSON.stringify({
        build_id: "b123",
        r2_prefix: "pipelines/__main/b123/",
      }),
    });
    const r2 = createMockR2({
      "pipelines/__main/b123/api/core/index.html": {
        body: streamFromString("<html>index</html>"),
        size: 18,
      },
    });
    const dashboardStore = createMockDashboardStore();
    const request = new Request("https://example.com/pipelines/api/core");

    const response = await resolve(directoryRoute, request, kv, r2, dashboardStore);

    expect(response.status).toBe(301);
    expect(response.headers.get("Location")).toBe(
      "https://example.com/pipelines/api/core/",
    );
  });

  it("serves index.html when path has trailing slash", async () => {
    const directoryRoute: Route = {
      kind: "edition",
      project: "pipelines",
      edition: "__main",
      path: "api/core/",
    };
    const kv = createMockKV({
      "pipelines/__main": JSON.stringify({
        build_id: "b123",
        r2_prefix: "pipelines/__main/b123/",
      }),
    });
    const r2 = createMockR2({
      "pipelines/__main/b123/api/core/index.html": {
        body: streamFromString("<html>index</html>"),
        size: 18,
      },
    });
    const dashboardStore = createMockDashboardStore();
    const request = new Request("https://example.com/pipelines/api/core/");

    const response = await resolve(directoryRoute, request, kv, r2, dashboardStore);

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("text/html");
    expect(response.headers.get("ETag")).toBe(
      '"pipelines/__main/b123/api/core/index.html-etag"',
    );
    expect(await response.text()).toBe("<html>index</html>");
  });

  it("falls back to index.html for empty path", async () => {
    const rootRoute: Route = {
      kind: "edition",
      project: "pipelines",
      edition: "__main",
      path: "",
    };
    const kv = createMockKV({
      "pipelines/__main": JSON.stringify({
        build_id: "b123",
        r2_prefix: "pipelines/__main/b123/",
      }),
    });
    const r2 = createMockR2({
      "pipelines/__main/b123/index.html": {
        body: streamFromString("<html>root index</html>"),
        size: 22,
      },
    });
    const dashboardStore = createMockDashboardStore();
    const request = new Request("https://example.com/pipelines/");

    const response = await resolve(rootRoute, request, kv, r2, dashboardStore);

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("text/html");
    expect(response.headers.get("ETag")).toBe(
      '"pipelines/__main/b123/index.html-etag"',
    );
    expect(await response.text()).toBe("<html>root index</html>");
  });

  it("falls back to index.html for empty path when r2_prefix lacks trailing slash", async () => {
    const rootRoute: Route = {
      kind: "edition",
      project: "pipelines",
      edition: "__main",
      path: "",
    };
    const kv = createMockKV({
      "pipelines/__main": JSON.stringify({
        build_id: "b123",
        r2_prefix: "pipelines/__main/b123",
      }),
    });
    const r2 = createMockR2({
      "pipelines/__main/b123/index.html": {
        body: streamFromString("<html>root index</html>"),
        size: 22,
      },
    });
    const dashboardStore = createMockDashboardStore();
    const request = new Request("https://example.com/pipelines/");

    const response = await resolve(rootRoute, request, kv, r2, dashboardStore);

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("text/html");
    expect(response.headers.get("ETag")).toBe(
      '"pipelines/__main/b123/index.html-etag"',
    );
    expect(await response.text()).toBe("<html>root index</html>");
  });

  it("infers Content-Type for CSS files", async () => {
    const cssRoute: Route = {
      kind: "edition",
      project: "pipelines",
      edition: "__main",
      path: "style.css",
    };
    const kv = createMockKV({
      "pipelines/__main": JSON.stringify({
        build_id: "b123",
        r2_prefix: "pipelines/__main/b123/",
      }),
    });
    const r2 = createMockR2({
      "pipelines/__main/b123/style.css": {
        body: streamFromString("body { color: red; }"),
        size: 20,
      },
    });
    const dashboardStore = createMockDashboardStore();
    const request = new Request("https://example.com/pipelines/style.css");

    const response = await resolve(cssRoute, request, kv, r2, dashboardStore);

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("text/css");
    expect(response.headers.get("ETag")).toBe(
      '"pipelines/__main/b123/style.css-etag"',
    );
    expect(await response.text()).toBe("body { color: red; }");
  });

  it("infers Content-Type for JSON files", async () => {
    const jsonRoute: Route = {
      kind: "edition",
      project: "pipelines",
      edition: "__main",
      path: "data.json",
    };
    const kv = createMockKV({
      "pipelines/__main": JSON.stringify({
        build_id: "b123",
        r2_prefix: "pipelines/__main/b123/",
      }),
    });
    const r2 = createMockR2({
      "pipelines/__main/b123/data.json": {
        body: streamFromString("{}"),
        size: 2,
      },
    });
    const dashboardStore = createMockDashboardStore();
    const request = new Request("https://example.com/pipelines/data.json");

    const response = await resolve(jsonRoute, request, kv, r2, dashboardStore);

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("application/json");
    expect(response.headers.get("ETag")).toBe(
      '"pipelines/__main/b123/data.json-etag"',
    );
    expect(await response.text()).toBe("{}");
  });

  it("uses application/octet-stream for unknown extensions", async () => {
    const unknownRoute: Route = {
      kind: "edition",
      project: "pipelines",
      edition: "__main",
      path: "data.xyz123",
    };
    const kv = createMockKV({
      "pipelines/__main": JSON.stringify({
        build_id: "b123",
        r2_prefix: "pipelines/__main/b123/",
      }),
    });
    const r2 = createMockR2({
      "pipelines/__main/b123/data.xyz123": {
        body: streamFromString("binary"),
        size: 6,
      },
    });
    const dashboardStore = createMockDashboardStore();
    const request = new Request("https://example.com/pipelines/data.xyz123");

    const response = await resolve(unknownRoute, request, kv, r2, dashboardStore);

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe(
      "application/octet-stream",
    );
    expect(response.headers.get("ETag")).toBe(
      '"pipelines/__main/b123/data.xyz123-etag"',
    );
    expect(await response.text()).toBe("binary");
  });

  it("returns 404 when both exact and index.html fallback miss", async () => {
    const directoryRoute: Route = {
      kind: "edition",
      project: "pipelines",
      edition: "__main",
      path: "nonexistent/dir",
    };
    const kv = createMockKV({
      "pipelines/__main": JSON.stringify({
        build_id: "b123",
        r2_prefix: "pipelines/__main/b123/",
      }),
    });
    const r2 = createMockR2({});
    const dashboardStore = createMockDashboardStore();
    const request = new Request("https://example.com/pipelines/nonexistent/dir");

    const response = await resolve(directoryRoute, request, kv, r2, dashboardStore);

    expect(response.status).toBe(404);
  });
});

describe("resolve — dashboard routes", () => {
  it("delegates to DashboardStore and returns the HTML body with dashboard headers", async () => {
    const dashboardRoute: Route = { kind: "dashboard", project: "sqr-112" };
    const kv = createMockKV();
    const r2 = createMockR2();
    const dashboardStore = createMockDashboardStore({
      "sqr-112": {
        body: streamFromString("<html>dashboard</html>"),
        size: 22,
      },
    });
    const request = new Request("https://sqr-112.lsst.io/v/");

    const response = await resolve(
      dashboardRoute,
      request,
      kv,
      r2,
      dashboardStore,
    );

    expect(dashboardStore.getDashboard).toHaveBeenCalledWith("sqr-112");
    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe(
      "text/html; charset=utf-8",
    );
    expect(response.headers.get("Cache-Control")).toBe(
      "public, max-age=60",
    );
    expect(await response.text()).toBe("<html>dashboard</html>");
    // Edition path must not be touched for dashboard dispatch.
    expect(kv.get).not.toHaveBeenCalled();
    expect(r2.get).not.toHaveBeenCalled();
  });

  it("returns 404 text when DashboardStore returns null", async () => {
    const dashboardRoute: Route = { kind: "dashboard", project: "sqr-112" };
    const kv = createMockKV();
    const r2 = createMockR2();
    const dashboardStore = createMockDashboardStore();
    const request = new Request("https://sqr-112.lsst.io/v/");

    const response = await resolve(
      dashboardRoute,
      request,
      kv,
      r2,
      dashboardStore,
    );

    expect(response.status).toBe(404);
    expect(response.headers.get("Content-Type")).toBe("text/plain");
  });
});

describe("resolve — switcher routes", () => {
  it("delegates to DashboardStore and returns JSON with switcher headers", async () => {
    const switcherRoute: Route = { kind: "switcher", project: "sqr-112" };
    const kv = createMockKV();
    const r2 = createMockR2();
    const dashboardStore = createMockDashboardStore(
      {},
      {
        "sqr-112": {
          body: streamFromString('[{"name":"main","url":"/v/main/"}]'),
          size: 34,
        },
      },
    );
    const request = new Request("https://sqr-112.lsst.io/v/switcher.json");

    const response = await resolve(
      switcherRoute,
      request,
      kv,
      r2,
      dashboardStore,
    );

    expect(dashboardStore.getSwitcher).toHaveBeenCalledWith("sqr-112");
    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe(
      "application/json; charset=utf-8",
    );
    expect(response.headers.get("Cache-Control")).toBe(
      "public, max-age=60",
    );
    expect(await response.text()).toBe('[{"name":"main","url":"/v/main/"}]');
    // Edition path must not be touched for switcher dispatch.
    expect(kv.get).not.toHaveBeenCalled();
    expect(r2.get).not.toHaveBeenCalled();
    // Dashboard lookup must not be touched for switcher dispatch.
    expect(dashboardStore.getDashboard).not.toHaveBeenCalled();
  });

  it("returns 404 text when DashboardStore returns null for switcher", async () => {
    const switcherRoute: Route = { kind: "switcher", project: "sqr-112" };
    const kv = createMockKV();
    const r2 = createMockR2();
    const dashboardStore = createMockDashboardStore();
    const request = new Request("https://sqr-112.lsst.io/v/switcher.json");

    const response = await resolve(
      switcherRoute,
      request,
      kv,
      r2,
      dashboardStore,
    );

    expect(response.status).toBe(404);
    expect(response.headers.get("Content-Type")).toBe("text/plain");
  });
});

describe("resolve — edition_meta routes", () => {
  it("delegates to DashboardStore and returns JSON with edition-meta headers", async () => {
    const metaRoute: Route = {
      kind: "edition_meta",
      project: "sqr-112",
      edition: "main",
    };
    const kv = createMockKV();
    const r2 = createMockR2();
    const dashboardStore = createMockDashboardStore(
      {},
      {},
      {
        "sqr-112/main": {
          body: streamFromString(
            '{"canonical_url":"https://sqr-112.lsst.io/","is_canonical":true}',
          ),
          size: 63,
        },
      },
    );
    const request = new Request(
      "https://sqr-112.lsst.io/v/main/_docverse.json",
    );

    const response = await resolve(
      metaRoute,
      request,
      kv,
      r2,
      dashboardStore,
    );

    expect(dashboardStore.getEditionMeta).toHaveBeenCalledWith(
      "sqr-112",
      "main",
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe(
      "application/json; charset=utf-8",
    );
    expect(response.headers.get("Cache-Control")).toBe(
      "public, max-age=60",
    );
    expect(await response.text()).toBe(
      '{"canonical_url":"https://sqr-112.lsst.io/","is_canonical":true}',
    );
    // Edition / dashboard / switcher paths must not be touched for
    // edition_meta dispatch.
    expect(kv.get).not.toHaveBeenCalled();
    expect(r2.get).not.toHaveBeenCalled();
    expect(dashboardStore.getDashboard).not.toHaveBeenCalled();
    expect(dashboardStore.getSwitcher).not.toHaveBeenCalled();
  });

  it("returns 404 text when DashboardStore returns null for edition_meta", async () => {
    const metaRoute: Route = {
      kind: "edition_meta",
      project: "sqr-112",
      edition: "nonexistent",
    };
    const kv = createMockKV();
    const r2 = createMockR2();
    const dashboardStore = createMockDashboardStore();
    const request = new Request(
      "https://sqr-112.lsst.io/v/nonexistent/_docverse.json",
    );

    const response = await resolve(
      metaRoute,
      request,
      kv,
      r2,
      dashboardStore,
    );

    expect(response.status).toBe(404);
    expect(response.headers.get("Content-Type")).toBe("text/plain");
  });
});

describe("resolve — redirect routes", () => {
  it("returns a 301 with Location set to the redirect target", async () => {
    const redirectRoute: Route = { kind: "redirect", to: "/v/" };
    const kv = createMockKV();
    const r2 = createMockR2();
    const dashboardStore = createMockDashboardStore();
    const request = new Request("https://sqr-112.lsst.io/v");

    const response = await resolve(
      redirectRoute,
      request,
      kv,
      r2,
      dashboardStore,
    );

    expect(response.status).toBe(301);
    expect(new URL(response.headers.get("Location") ?? "").pathname).toBe(
      "/v/",
    );
    // Edition / dashboard paths must not be touched for redirect dispatch.
    expect(kv.get).not.toHaveBeenCalled();
    expect(r2.get).not.toHaveBeenCalled();
    expect(dashboardStore.getDashboard).not.toHaveBeenCalled();
  });

  it("preserves query string on redirect", async () => {
    const redirectRoute: Route = { kind: "redirect", to: "/v/" };
    const kv = createMockKV();
    const r2 = createMockR2();
    const dashboardStore = createMockDashboardStore();
    const request = new Request("https://sqr-112.lsst.io/v?foo=bar&baz=1");

    const response = await resolve(
      redirectRoute,
      request,
      kv,
      r2,
      dashboardStore,
    );

    expect(response.status).toBe(301);
    const location = new URL(response.headers.get("Location") ?? "");
    expect(location.pathname).toBe("/v/");
    expect(location.search).toBe("?foo=bar&baz=1");
  });

  it("redirects under path-prefix scheme target", async () => {
    const redirectRoute: Route = {
      kind: "redirect",
      to: "/docs/sqr-112/v/",
    };
    const kv = createMockKV();
    const r2 = createMockR2();
    const dashboardStore = createMockDashboardStore();
    const request = new Request("https://docs.example.com/docs/sqr-112/v");

    const response = await resolve(
      redirectRoute,
      request,
      kv,
      r2,
      dashboardStore,
    );

    expect(response.status).toBe(301);
    expect(new URL(response.headers.get("Location") ?? "").pathname).toBe(
      "/docs/sqr-112/v/",
    );
  });
});

describe("resolve — branded 404 fallback", () => {
  it("serves __404.html with 404 status and HTML headers when dashboard route misses and __404.html is present", async () => {
    const dashboardRoute: Route = { kind: "dashboard", project: "sqr-112" };
    const kv = createMockKV();
    const r2 = createMockR2();
    const dashboardStore = createMockDashboardStore(
      {},
      {},
      {},
      {
        "sqr-112": {
          body: streamFromString("<html>branded 404</html>"),
          size: 24,
        },
      },
    );
    const request = new Request("https://sqr-112.lsst.io/v/");

    const response = await resolve(
      dashboardRoute,
      request,
      kv,
      r2,
      dashboardStore,
    );

    expect(dashboardStore.get404).toHaveBeenCalledWith("sqr-112");
    expect(response.status).toBe(404);
    expect(response.headers.get("Content-Type")).toBe(
      "text/html; charset=utf-8",
    );
    expect(response.headers.get("Cache-Control")).toBe(
      "public, max-age=60",
    );
    expect(await response.text()).toBe("<html>branded 404</html>");
  });

  it("serves __404.html when edition route's KV lookup misses and __404.html is present", async () => {
    const editionRoute: Route = {
      kind: "edition",
      project: "sqr-112",
      edition: "__main",
      path: "page.html",
    };
    const kv = createMockKV({});
    const r2 = createMockR2();
    const dashboardStore = createMockDashboardStore(
      {},
      {},
      {},
      {
        "sqr-112": {
          body: streamFromString("<html>branded 404</html>"),
          size: 24,
        },
      },
    );
    const request = new Request("https://sqr-112.lsst.io/page.html");

    const response = await resolve(
      editionRoute,
      request,
      kv,
      r2,
      dashboardStore,
    );

    expect(dashboardStore.get404).toHaveBeenCalledWith("sqr-112");
    expect(response.status).toBe(404);
    expect(response.headers.get("Content-Type")).toBe(
      "text/html; charset=utf-8",
    );
    expect(response.headers.get("Cache-Control")).toBe(
      "public, max-age=60",
    );
    expect(await response.text()).toBe("<html>branded 404</html>");
  });

  it("falls back to plain-text Not Found with cache headers when __404.html is absent (dashboard branch)", async () => {
    const dashboardRoute: Route = { kind: "dashboard", project: "sqr-112" };
    const kv = createMockKV();
    const r2 = createMockR2();
    const dashboardStore = createMockDashboardStore();
    const request = new Request("https://sqr-112.lsst.io/v/");

    const response = await resolve(
      dashboardRoute,
      request,
      kv,
      r2,
      dashboardStore,
    );

    expect(dashboardStore.get404).toHaveBeenCalledWith("sqr-112");
    expect(response.status).toBe(404);
    expect(response.headers.get("Content-Type")).toBe("text/plain");
    expect(response.headers.get("Cache-Control")).toBe(
      "public, max-age=60",
    );
    expect(await response.text()).toBe("Not Found");
  });

  it("falls back to plain-text Not Found with cache headers when __404.html is absent (edition R2 miss)", async () => {
    const editionRoute: Route = {
      kind: "edition",
      project: "pipelines",
      edition: "__main",
      path: "missing.html",
    };
    const kv = createMockKV({
      "pipelines/__main": JSON.stringify({
        build_id: "b123",
        r2_prefix: "pipelines/__main/b123/",
      }),
    });
    const r2 = createMockR2({});
    const dashboardStore = createMockDashboardStore();
    const request = new Request("https://pipelines.lsst.io/missing.html");

    const response = await resolve(
      editionRoute,
      request,
      kv,
      r2,
      dashboardStore,
    );

    expect(dashboardStore.get404).toHaveBeenCalledWith("pipelines");
    expect(response.status).toBe(404);
    expect(response.headers.get("Content-Type")).toBe("text/plain");
    expect(response.headers.get("Cache-Control")).toBe(
      "public, max-age=60",
    );
    expect(await response.text()).toBe("Not Found");
  });
});
