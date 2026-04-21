import { describe, it, expect, vi } from "vitest";
import { createDashboardStore } from "../src/dashboardStore";

/**
 * Create a mock R2 bucket whose `get()` returns the body stored at the
 * given key, or null if the key is not present.
 */
function createMockR2(
  store: Record<
    string,
    { body: ReadableStream; size: number }
  > = {},
): R2Bucket {
  return {
    get: vi.fn(async (key: string) => {
      const obj = store[key];
      if (!obj) return null;
      return {
        body: obj.body,
        size: obj.size,
        httpEtag: `"${key}-etag"`,
        httpMetadata: {},
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

function streamFromString(s: string): ReadableStream {
  return new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode(s));
      controller.close();
    },
  });
}

describe("DashboardStore.getDashboard", () => {
  it("returns the R2 object for {project}/__dashboard.html on hit", async () => {
    const r2 = createMockR2({
      "sqr-112/__dashboard.html": {
        body: streamFromString("<html>dashboard</html>"),
        size: 22,
      },
    });
    const store = createDashboardStore(r2);

    const object = await store.getDashboard("sqr-112");

    expect(object).not.toBeNull();
    expect(r2.get).toHaveBeenCalledWith("sqr-112/__dashboard.html");
    const body = await new Response(object!.body).text();
    expect(body).toBe("<html>dashboard</html>");
  });

  it("returns null on miss", async () => {
    const r2 = createMockR2({});
    const store = createDashboardStore(r2);

    const object = await store.getDashboard("sqr-112");

    expect(object).toBeNull();
  });

  it("never throws when R2.get rejects", async () => {
    const r2 = {
      get: vi.fn(async () => {
        throw new Error("boom");
      }),
    } as unknown as R2Bucket;
    const store = createDashboardStore(r2);

    await expect(store.getDashboard("sqr-112")).resolves.toBeNull();
  });
});
