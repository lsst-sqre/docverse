import { createDashboardStore } from "./dashboardStore";
import { resolve } from "./resolver";
import { parseRoute } from "./router";
import { Env } from "./types";

export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<Response> {
    const route = parseRoute(request, env.URL_SCHEME, env.PATH_PREFIX);
    if (route === null) {
      return new Response("Not Found", {
        status: 404,
        headers: { "Content-Type": "text/plain" },
      });
    }
    const dashboardStore = createDashboardStore(env.BUILDS_R2);
    return resolve(
      route,
      request,
      env.EDITIONS_KV,
      env.BUILDS_R2,
      dashboardStore,
    );
  },
} satisfies ExportedHandler<Env>;
