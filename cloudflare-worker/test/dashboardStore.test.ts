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

describe("DashboardStore.getSwitcher", () => {
  it("returns the R2 object for {project}/__switcher.json on hit", async () => {
    const r2 = createMockR2({
      "sqr-112/__switcher.json": {
        body: streamFromString('[{"name":"main","url":"/v/main/"}]'),
        size: 34,
      },
    });
    const store = createDashboardStore(r2);

    const object = await store.getSwitcher("sqr-112");

    expect(object).not.toBeNull();
    expect(r2.get).toHaveBeenCalledWith("sqr-112/__switcher.json");
    const body = await new Response(object!.body).text();
    expect(body).toBe('[{"name":"main","url":"/v/main/"}]');
  });

  it("returns null on miss", async () => {
    const r2 = createMockR2({});
    const store = createDashboardStore(r2);

    const object = await store.getSwitcher("sqr-112");

    expect(object).toBeNull();
  });

  it("never throws when R2.get rejects", async () => {
    const r2 = {
      get: vi.fn(async () => {
        throw new Error("boom");
      }),
    } as unknown as R2Bucket;
    const store = createDashboardStore(r2);

    await expect(store.getSwitcher("sqr-112")).resolves.toBeNull();
  });
});

describe("DashboardStore.getEditionMeta", () => {
  it("returns the R2 object for {project}/__editions/{edition}.json on hit", async () => {
    const r2 = createMockR2({
      "sqr-112/__editions/main.json": {
        body: streamFromString(
          '{"canonical_url":"https://sqr-112.lsst.io/","is_canonical":true}',
        ),
        size: 63,
      },
    });
    const store = createDashboardStore(r2);

    const object = await store.getEditionMeta("sqr-112", "main");

    expect(object).not.toBeNull();
    expect(r2.get).toHaveBeenCalledWith("sqr-112/__editions/main.json");
    const body = await new Response(object!.body).text();
    expect(body).toBe(
      '{"canonical_url":"https://sqr-112.lsst.io/","is_canonical":true}',
    );
  });

  it("returns null on miss", async () => {
    const r2 = createMockR2({});
    const store = createDashboardStore(r2);

    const object = await store.getEditionMeta("sqr-112", "main");

    expect(object).toBeNull();
  });

  it("never throws when R2.get rejects", async () => {
    const r2 = {
      get: vi.fn(async () => {
        throw new Error("boom");
      }),
    } as unknown as R2Bucket;
    const store = createDashboardStore(r2);

    await expect(
      store.getEditionMeta("sqr-112", "main"),
    ).resolves.toBeNull();
  });
});

describe("DashboardStore.get404", () => {
  it("returns the R2 object for {project}/__404.html on hit", async () => {
    const r2 = createMockR2({
      "sqr-112/__404.html": {
        body: streamFromString("<html>branded 404</html>"),
        size: 24,
      },
    });
    const store = createDashboardStore(r2);

    const object = await store.get404("sqr-112");

    expect(object).not.toBeNull();
    expect(r2.get).toHaveBeenCalledWith("sqr-112/__404.html");
    const body = await new Response(object!.body).text();
    expect(body).toBe("<html>branded 404</html>");
  });

  it("returns null on miss", async () => {
    const r2 = createMockR2({});
    const store = createDashboardStore(r2);

    const object = await store.get404("sqr-112");

    expect(object).toBeNull();
  });

  it("never throws when R2.get rejects", async () => {
    const r2 = {
      get: vi.fn(async () => {
        throw new Error("boom");
      }),
    } as unknown as R2Bucket;
    const store = createDashboardStore(r2);

    await expect(store.get404("sqr-112")).resolves.toBeNull();
  });
});

describe("DashboardStore logging on R2 error", () => {
  function rejectingR2(message: string): R2Bucket {
    return {
      get: vi.fn(async () => {
        throw new Error(message);
      }),
    } as unknown as R2Bucket;
  }

  it.each([
    [
      "getDashboard",
      "sqr-112/__dashboard.html",
      (s: ReturnType<typeof createDashboardStore>) => s.getDashboard("sqr-112"),
    ],
    [
      "getSwitcher",
      "sqr-112/__switcher.json",
      (s: ReturnType<typeof createDashboardStore>) => s.getSwitcher("sqr-112"),
    ],
    [
      "getEditionMeta",
      "sqr-112/__editions/main.json",
      (s: ReturnType<typeof createDashboardStore>) =>
        s.getEditionMeta("sqr-112", "main"),
    ],
    [
      "get404",
      "sqr-112/__404.html",
      (s: ReturnType<typeof createDashboardStore>) => s.get404("sqr-112"),
    ],
  ])(
    "%s emits a structured console.warn with {event, key, error} on R2 rejection",
    async (_name, expectedKey, call) => {
      const warnSpy = vi
        .spyOn(console, "warn")
        .mockImplementation(() => undefined);
      const store = createDashboardStore(rejectingR2("boom"));

      await call(store);

      expect(warnSpy).toHaveBeenCalledTimes(1);
      const payload = JSON.parse(warnSpy.mock.calls[0][0] as string);
      expect(payload).toEqual({
        event: "dashboard_store_r2_error",
        key: expectedKey,
        error: "Error: boom",
      });
      warnSpy.mockRestore();
    },
  );

  it("does not log on hit", async () => {
    const warnSpy = vi
      .spyOn(console, "warn")
      .mockImplementation(() => undefined);
    const r2 = createMockR2({
      "sqr-112/__dashboard.html": {
        body: streamFromString("<html>dashboard</html>"),
        size: 22,
      },
    });
    const store = createDashboardStore(r2);

    await store.getDashboard("sqr-112");

    expect(warnSpy).not.toHaveBeenCalled();
    warnSpy.mockRestore();
  });

  it("does not log on plain miss", async () => {
    const warnSpy = vi
      .spyOn(console, "warn")
      .mockImplementation(() => undefined);
    const store = createDashboardStore(createMockR2({}));

    await store.getDashboard("sqr-112");

    expect(warnSpy).not.toHaveBeenCalled();
    warnSpy.mockRestore();
  });
});
