import { describe, it, expect } from "vitest";
import { parseRoute, UrlScheme } from "../src/router";

/**
 * Helper to create a minimal Request object from a URL string.
 */
function makeRequest(url: string): Request {
  return new Request(url);
}

describe("Subdomain routing", () => {
  const scheme = "subdomain";

  it("routes root path to __main edition", () => {
    const route = parseRoute(
      makeRequest("https://pipelines.lsst.io/"),
      scheme,
    );
    expect(route).toEqual({
      kind: "edition",
      project: "pipelines",
      edition: "__main",
      path: "",
    });
  });

  it("routes a file at the root to __main edition", () => {
    const route = parseRoute(
      makeRequest("https://pipelines.lsst.io/getting-started.html"),
      scheme,
    );
    expect(route).toEqual({
      kind: "edition",
      project: "pipelines",
      edition: "__main",
      path: "getting-started.html",
    });
  });

  it("routes a nested path to __main edition", () => {
    const route = parseRoute(
      makeRequest("https://pipelines.lsst.io/modules/lsst.pipe/index.html"),
      scheme,
    );
    expect(route).toEqual({
      kind: "edition",
      project: "pipelines",
      edition: "__main",
      path: "modules/lsst.pipe/index.html",
    });
  });

  it("routes /v/{edition}/ to named edition with empty path", () => {
    const route = parseRoute(
      makeRequest("https://pipelines.lsst.io/v/main/"),
      scheme,
    );
    expect(route).toEqual({
      kind: "edition",
      project: "pipelines",
      edition: "main",
      path: "",
    });
  });

  it("routes /v/{edition}/page.html to named edition", () => {
    const route = parseRoute(
      makeRequest("https://pipelines.lsst.io/v/v1.0/changelog.html"),
      scheme,
    );
    expect(route).toEqual({
      kind: "edition",
      project: "pipelines",
      edition: "v1.0",
      path: "changelog.html",
    });
  });

  it("routes /v/{edition}/nested/path to named edition", () => {
    const route = parseRoute(
      makeRequest("https://pipelines.lsst.io/v/weekly-2024/api/core.html"),
      scheme,
    );
    expect(route).toEqual({
      kind: "edition",
      project: "pipelines",
      edition: "weekly-2024",
      path: "api/core.html",
    });
  });

  it("routes /v/{edition} without trailing slash", () => {
    const route = parseRoute(
      makeRequest("https://pipelines.lsst.io/v/main"),
      scheme,
    );
    expect(route).toEqual({
      kind: "edition",
      project: "pipelines",
      edition: "main",
      path: "",
    });
  });

  it("extracts project from leftmost subdomain label", () => {
    const route = parseRoute(
      makeRequest("https://sqr-112.lsst.io/"),
      scheme,
    );
    expect(route).toEqual({
      kind: "edition",
      project: "sqr-112",
      edition: "__main",
      path: "",
    });
  });

  it("returns null for bare domain with no subdomain", () => {
    const route = parseRoute(makeRequest("https://localhost/"), scheme);
    expect(route).toBeNull();
  });

  it("returns null for empty subdomain", () => {
    const route = parseRoute(makeRequest("https://.example.com/"), scheme);
    expect(route).toBeNull();
  });

  it("classifies /v/ as a dashboard route", () => {
    const route = parseRoute(
      makeRequest("https://sqr-112.lsst.io/v/"),
      scheme,
    );
    expect(route).toEqual({
      kind: "dashboard",
      project: "sqr-112",
    });
  });

  it("classifies /v/index.html as a dashboard route", () => {
    const route = parseRoute(
      makeRequest("https://sqr-112.lsst.io/v/index.html"),
      scheme,
    );
    expect(route).toEqual({
      kind: "dashboard",
      project: "sqr-112",
    });
  });

  it("classifies /v/switcher.json as a switcher route", () => {
    const route = parseRoute(
      makeRequest("https://sqr-112.lsst.io/v/switcher.json"),
      scheme,
    );
    expect(route).toEqual({
      kind: "switcher",
      project: "sqr-112",
    });
  });

  it("classifies /v/switcher.json/extra as an edition named switcher.json", () => {
    // Locks in classification order: exact-match dashboard-family routes
    // (dashboard, switcher) win over the generic /v/ edition fallback, but
    // anything with extra path segments falls back to edition routing.
    const route = parseRoute(
      makeRequest("https://sqr-112.lsst.io/v/switcher.json/extra"),
      scheme,
    );
    expect(route).toEqual({
      kind: "edition",
      project: "sqr-112",
      edition: "switcher.json",
      path: "extra",
    });
  });

  it("classifies /v (no trailing slash) as a redirect to /v/", () => {
    const route = parseRoute(
      makeRequest("https://sqr-112.lsst.io/v"),
      scheme,
    );
    expect(route).toEqual({
      kind: "redirect",
      to: "/v/",
    });
  });

  it("classifies /v/{edition}/_docverse.json as an edition_meta route", () => {
    const route = parseRoute(
      makeRequest("https://sqr-112.lsst.io/v/main/_docverse.json"),
      scheme,
    );
    expect(route).toEqual({
      kind: "edition_meta",
      project: "sqr-112",
      edition: "main",
    });
  });

  it("classifies /v/{edition}/_docverse.json with a dashy edition name", () => {
    const route = parseRoute(
      makeRequest("https://sqr-112.lsst.io/v/weekly-2024/_docverse.json"),
      scheme,
    );
    expect(route).toEqual({
      kind: "edition_meta",
      project: "sqr-112",
      edition: "weekly-2024",
    });
  });

  it("classifies /v/main/_docverse.json/extra as an edition file path", () => {
    // Locks in classification order: the exact-match edition_meta branch
    // must not swallow deeper paths. /v/main/_docverse.json/extra must
    // continue to route as edition "main", path "_docverse.json/extra".
    const route = parseRoute(
      makeRequest("https://sqr-112.lsst.io/v/main/_docverse.json/extra"),
      scheme,
    );
    expect(route).toEqual({
      kind: "edition",
      project: "sqr-112",
      edition: "main",
      path: "_docverse.json/extra",
    });
  });

  it("classifies /_docverse.json at project root as __main edition_meta", () => {
    const route = parseRoute(
      makeRequest("https://sqr-112.lsst.io/_docverse.json"),
      scheme,
    );
    expect(route).toEqual({
      kind: "edition_meta",
      project: "sqr-112",
      edition: "__main",
    });
  });

  it("classifies /_docverse.json/extra as a regular __main edition path", () => {
    // The reserved name only applies at the exact project root — deeper
    // paths must fall through to the __main edition file route.
    const route = parseRoute(
      makeRequest("https://sqr-112.lsst.io/_docverse.json/extra"),
      scheme,
    );
    expect(route).toEqual({
      kind: "edition",
      project: "sqr-112",
      edition: "__main",
      path: "_docverse.json/extra",
    });
  });

  it("classifies /sub/_docverse.json as a regular __main edition path", () => {
    // Reserved name applies only at the project root, not inside
    // subdirectories of the __main edition.
    const route = parseRoute(
      makeRequest("https://sqr-112.lsst.io/sub/_docverse.json"),
      scheme,
    );
    expect(route).toEqual({
      kind: "edition",
      project: "sqr-112",
      edition: "__main",
      path: "sub/_docverse.json",
    });
  });
});

describe("Path-prefix routing", () => {
  const scheme = "path-prefix";

  describe("without root prefix", () => {
    it("routes /{project}/ to __main edition", () => {
      const route = parseRoute(
        makeRequest("https://docs.example.com/pipelines/"),
        scheme,
      );
      expect(route).toEqual({
        kind: "edition",
        project: "pipelines",
        edition: "__main",
        path: "",
      });
    });

    it("routes /{project}/page.html to __main edition", () => {
      const route = parseRoute(
        makeRequest("https://docs.example.com/pipelines/getting-started.html"),
        scheme,
      );
      expect(route).toEqual({
        kind: "edition",
        project: "pipelines",
        edition: "__main",
        path: "getting-started.html",
      });
    });

    it("routes /{project}/v/{edition}/page.html to named edition", () => {
      const route = parseRoute(
        makeRequest(
          "https://docs.example.com/pipelines/v/main/changelog.html",
        ),
        scheme,
      );
      expect(route).toEqual({
        kind: "edition",
        project: "pipelines",
        edition: "main",
        path: "changelog.html",
      });
    });

    it("routes /{project}/v/{edition}/ to named edition with empty path", () => {
      const route = parseRoute(
        makeRequest("https://docs.example.com/pipelines/v/v2.0/"),
        scheme,
      );
      expect(route).toEqual({
        kind: "edition",
        project: "pipelines",
        edition: "v2.0",
        path: "",
      });
    });

    it("routes /{project}/v/{edition} without trailing slash", () => {
      const route = parseRoute(
        makeRequest("https://docs.example.com/pipelines/v/main"),
        scheme,
      );
      expect(route).toEqual({
        kind: "edition",
        project: "pipelines",
        edition: "main",
        path: "",
      });
    });

    it("routes /{project} without trailing slash", () => {
      const route = parseRoute(
        makeRequest("https://docs.example.com/pipelines"),
        scheme,
      );
      expect(route).toEqual({
        kind: "edition",
        project: "pipelines",
        edition: "__main",
        path: "",
      });
    });

    it("routes nested path under __main", () => {
      const route = parseRoute(
        makeRequest("https://docs.example.com/pipelines/api/core/index.html"),
        scheme,
      );
      expect(route).toEqual({
        kind: "edition",
        project: "pipelines",
        edition: "__main",
        path: "api/core/index.html",
      });
    });

    it("returns null for bare root path", () => {
      const route = parseRoute(
        makeRequest("https://docs.example.com/"),
        scheme,
      );
      expect(route).toBeNull();
    });

    it("classifies /{project}/v/ as a dashboard route", () => {
      const route = parseRoute(
        makeRequest("https://docs.example.com/sqr-112/v/"),
        scheme,
      );
      expect(route).toEqual({
        kind: "dashboard",
        project: "sqr-112",
      });
    });

    it("classifies /{project}/v/index.html as a dashboard route", () => {
      const route = parseRoute(
        makeRequest("https://docs.example.com/sqr-112/v/index.html"),
        scheme,
      );
      expect(route).toEqual({
        kind: "dashboard",
        project: "sqr-112",
      });
    });

    it("classifies /{project}/v/switcher.json as a switcher route", () => {
      const route = parseRoute(
        makeRequest("https://docs.example.com/sqr-112/v/switcher.json"),
        scheme,
      );
      expect(route).toEqual({
        kind: "switcher",
        project: "sqr-112",
      });
    });

    it("classifies /{project}/v/switcher.json/extra as edition switcher.json", () => {
      const route = parseRoute(
        makeRequest("https://docs.example.com/sqr-112/v/switcher.json/extra"),
        scheme,
      );
      expect(route).toEqual({
        kind: "edition",
        project: "sqr-112",
        edition: "switcher.json",
        path: "extra",
      });
    });

    it("classifies /{project}/v (no trailing slash) as a redirect", () => {
      const route = parseRoute(
        makeRequest("https://docs.example.com/sqr-112/v"),
        scheme,
      );
      expect(route).toEqual({
        kind: "redirect",
        to: "/sqr-112/v/",
      });
    });

    it("classifies /{project}/v/{edition}/_docverse.json as edition_meta", () => {
      const route = parseRoute(
        makeRequest("https://docs.example.com/sqr-112/v/main/_docverse.json"),
        scheme,
      );
      expect(route).toEqual({
        kind: "edition_meta",
        project: "sqr-112",
        edition: "main",
      });
    });

    it("classifies /{project}/v/main/_docverse.json/extra as edition path", () => {
      const route = parseRoute(
        makeRequest(
          "https://docs.example.com/sqr-112/v/main/_docverse.json/extra",
        ),
        scheme,
      );
      expect(route).toEqual({
        kind: "edition",
        project: "sqr-112",
        edition: "main",
        path: "_docverse.json/extra",
      });
    });

    it("classifies /{project}/_docverse.json as __main edition_meta", () => {
      const route = parseRoute(
        makeRequest("https://docs.example.com/sqr-112/_docverse.json"),
        scheme,
      );
      expect(route).toEqual({
        kind: "edition_meta",
        project: "sqr-112",
        edition: "__main",
      });
    });

    it("classifies /{project}/_docverse.json/extra as a __main edition path", () => {
      const route = parseRoute(
        makeRequest("https://docs.example.com/sqr-112/_docverse.json/extra"),
        scheme,
      );
      expect(route).toEqual({
        kind: "edition",
        project: "sqr-112",
        edition: "__main",
        path: "_docverse.json/extra",
      });
    });

    it("classifies /{project}/sub/_docverse.json as a __main edition path", () => {
      const route = parseRoute(
        makeRequest("https://docs.example.com/sqr-112/sub/_docverse.json"),
        scheme,
      );
      expect(route).toEqual({
        kind: "edition",
        project: "sqr-112",
        edition: "__main",
        path: "sub/_docverse.json",
      });
    });
  });

  describe("with root prefix", () => {
    const prefix = "/docs/";

    it("routes /docs/{project}/ to __main edition", () => {
      const route = parseRoute(
        makeRequest("https://example.com/docs/pipelines/"),
        scheme,
        prefix,
      );
      expect(route).toEqual({
        kind: "edition",
        project: "pipelines",
        edition: "__main",
        path: "",
      });
    });

    it("routes /docs/{project}/page.html to __main", () => {
      const route = parseRoute(
        makeRequest("https://example.com/docs/pipelines/getting-started.html"),
        scheme,
        prefix,
      );
      expect(route).toEqual({
        kind: "edition",
        project: "pipelines",
        edition: "__main",
        path: "getting-started.html",
      });
    });

    it("routes /docs/{project}/v/{edition}/page.html to named edition", () => {
      const route = parseRoute(
        makeRequest(
          "https://example.com/docs/pipelines/v/main/changelog.html",
        ),
        scheme,
        prefix,
      );
      expect(route).toEqual({
        kind: "edition",
        project: "pipelines",
        edition: "main",
        path: "changelog.html",
      });
    });

    it("returns null when prefix does not match", () => {
      const route = parseRoute(
        makeRequest("https://example.com/other/pipelines/"),
        scheme,
        prefix,
      );
      expect(route).toBeNull();
    });

    it("returns null for the prefix path alone", () => {
      const route = parseRoute(
        makeRequest("https://example.com/docs/"),
        scheme,
        prefix,
      );
      expect(route).toBeNull();
    });

    it("normalizes prefix without trailing slash", () => {
      const route = parseRoute(
        makeRequest("https://example.com/docs/pipelines/index.html"),
        scheme,
        "/docs",
      );
      expect(route).toEqual({
        kind: "edition",
        project: "pipelines",
        edition: "__main",
        path: "index.html",
      });
    });

    it("normalizes prefix without leading slash", () => {
      const route = parseRoute(
        makeRequest("https://example.com/docs/pipelines/index.html"),
        scheme,
        "docs/",
      );
      expect(route).toEqual({
        kind: "edition",
        project: "pipelines",
        edition: "__main",
        path: "index.html",
      });
    });

    it("classifies /docs/{project}/v/ as a dashboard route", () => {
      const route = parseRoute(
        makeRequest("https://example.com/docs/sqr-112/v/"),
        scheme,
        prefix,
      );
      expect(route).toEqual({
        kind: "dashboard",
        project: "sqr-112",
      });
    });

    it("classifies /docs/{project}/v/index.html as a dashboard route", () => {
      const route = parseRoute(
        makeRequest("https://example.com/docs/sqr-112/v/index.html"),
        scheme,
        prefix,
      );
      expect(route).toEqual({
        kind: "dashboard",
        project: "sqr-112",
      });
    });

    it("classifies /docs/{project}/v/switcher.json as a switcher route", () => {
      const route = parseRoute(
        makeRequest("https://example.com/docs/sqr-112/v/switcher.json"),
        scheme,
        prefix,
      );
      expect(route).toEqual({
        kind: "switcher",
        project: "sqr-112",
      });
    });

    it("classifies /docs/{project}/v (no trailing slash) as a redirect", () => {
      const route = parseRoute(
        makeRequest("https://example.com/docs/sqr-112/v"),
        scheme,
        prefix,
      );
      expect(route).toEqual({
        kind: "redirect",
        to: "/docs/sqr-112/v/",
      });
    });

    it("classifies /docs/{project}/_docverse.json as __main edition_meta", () => {
      const route = parseRoute(
        makeRequest("https://example.com/docs/sqr-112/_docverse.json"),
        scheme,
        prefix,
      );
      expect(route).toEqual({
        kind: "edition_meta",
        project: "sqr-112",
        edition: "__main",
      });
    });
  });
});

describe("Invalid URL scheme", () => {
  it("throws on unknown URL scheme", () => {
    expect(() =>
      parseRoute(
        makeRequest("https://example.com/"),
        "bogus" as UrlScheme,
      ),
    ).toThrow("Unknown URL scheme: bogus");
  });
});
