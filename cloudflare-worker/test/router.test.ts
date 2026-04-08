import { describe, it, expect } from "vitest";
import { parseRoute, Route, UrlScheme } from "../src/router";

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

  it("routes /v/ with no edition to __main", () => {
    const route = parseRoute(
      makeRequest("https://pipelines.lsst.io/v/"),
      scheme,
    );
    expect(route).toEqual({
      project: "pipelines",
      edition: "__main",
      path: "",
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
        project: "pipelines",
        edition: "__main",
        path: "index.html",
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
