import { env, SELF } from "cloudflare:test";
import { describe, it, expect } from "vitest";

describe("Worker fetch handler", () => {
  it("returns a response", async () => {
    const response = await SELF.fetch("https://example.com/");
    expect(response.status).toBe(200);
    const text = await response.text();
    expect(text).toContain("docverse");
  });
});
