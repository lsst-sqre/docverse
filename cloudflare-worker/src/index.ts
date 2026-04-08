import { Env } from "./types";

export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<Response> {
    return new Response("docverse worker is running", {
      headers: { "Content-Type": "text/plain" },
    });
  },
} satisfies ExportedHandler<Env>;
