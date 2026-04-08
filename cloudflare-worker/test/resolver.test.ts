import { describe, it, expect, vi } from "vitest";
import { resolve } from "../src/resolver";
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

describe("resolve", () => {
  const route: Route = {
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
    const request = new Request("https://example.com/pipelines/getting-started.html");

    const response = await resolve(route, request, kv, r2);

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("text/html");
    expect(response.headers.get("Content-Length")).toBe("18");
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
    const request = new Request("https://example.com/pipelines/getting-started.html");

    const response = await resolve(route, request, kv, r2);

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
    const request = new Request("https://example.com/pipelines/getting-started.html");

    const response = await resolve(route, request, kv, r2);

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
    const request = new Request("https://example.com/pipelines/getting-started.html");

    const response = await resolve(route, request, kv, r2);

    expect(response.status).toBe(404);
    expect(response.headers.get("Content-Type")).toBe("text/plain");
    const body = await response.text();
    expect(body).toContain("Not Found");
  });

  it("redirects to trailing slash for directory paths", async () => {
    const directoryRoute: Route = {
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
    const request = new Request("https://example.com/pipelines/api/core");

    const response = await resolve(directoryRoute, request, kv, r2);

    expect(response.status).toBe(301);
    expect(response.headers.get("Location")).toBe(
      "https://example.com/pipelines/api/core/",
    );
  });

  it("serves index.html when path has trailing slash", async () => {
    const directoryRoute: Route = {
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
    const request = new Request("https://example.com/pipelines/api/core/");

    const response = await resolve(directoryRoute, request, kv, r2);

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("text/html");
    expect(response.headers.get("Content-Length")).toBe("18");
    expect(response.headers.get("ETag")).toBe(
      '"pipelines/__main/b123/api/core/index.html-etag"',
    );
    expect(await response.text()).toBe("<html>index</html>");
  });

  it("falls back to index.html for empty path", async () => {
    const rootRoute: Route = {
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
    const request = new Request("https://example.com/pipelines/");

    const response = await resolve(rootRoute, request, kv, r2);

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("text/html");
    expect(response.headers.get("Content-Length")).toBe("22");
    expect(response.headers.get("ETag")).toBe(
      '"pipelines/__main/b123/index.html-etag"',
    );
    expect(await response.text()).toBe("<html>root index</html>");
  });

  it("falls back to index.html for empty path when r2_prefix lacks trailing slash", async () => {
    const rootRoute: Route = {
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
    const request = new Request("https://example.com/pipelines/");

    const response = await resolve(rootRoute, request, kv, r2);

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("text/html");
    expect(response.headers.get("Content-Length")).toBe("22");
    expect(response.headers.get("ETag")).toBe(
      '"pipelines/__main/b123/index.html-etag"',
    );
    expect(await response.text()).toBe("<html>root index</html>");
  });

  it("infers Content-Type for CSS files", async () => {
    const cssRoute: Route = {
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
    const request = new Request("https://example.com/pipelines/style.css");

    const response = await resolve(cssRoute, request, kv, r2);

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("text/css");
    expect(response.headers.get("Content-Length")).toBe("20");
    expect(response.headers.get("ETag")).toBe(
      '"pipelines/__main/b123/style.css-etag"',
    );
    expect(await response.text()).toBe("body { color: red; }");
  });

  it("infers Content-Type for JSON files", async () => {
    const jsonRoute: Route = {
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
    const request = new Request("https://example.com/pipelines/data.json");

    const response = await resolve(jsonRoute, request, kv, r2);

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe("application/json");
    expect(response.headers.get("Content-Length")).toBe("2");
    expect(response.headers.get("ETag")).toBe(
      '"pipelines/__main/b123/data.json-etag"',
    );
    expect(await response.text()).toBe("{}");
  });

  it("uses application/octet-stream for unknown extensions", async () => {
    const unknownRoute: Route = {
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
    const request = new Request("https://example.com/pipelines/data.xyz123");

    const response = await resolve(unknownRoute, request, kv, r2);

    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type")).toBe(
      "application/octet-stream",
    );
    expect(response.headers.get("Content-Length")).toBe("6");
    expect(response.headers.get("ETag")).toBe(
      '"pipelines/__main/b123/data.xyz123-etag"',
    );
    expect(await response.text()).toBe("binary");
  });

  it("returns 404 when both exact and index.html fallback miss", async () => {
    const directoryRoute: Route = {
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
    const request = new Request("https://example.com/pipelines/nonexistent/dir");

    const response = await resolve(directoryRoute, request, kv, r2);

    expect(response.status).toBe(404);
  });
});
