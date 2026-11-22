import type { Hono } from "hono";
import { obj } from "./http.ts";

export function registerAzure(app: Hono): void {
  app.post("/azure/openai/deployments/:model/:operation", async (c) => {
    if (!c.req.header("api-key") || c.req.header("authorization") || !c.req.query("api-version"))
      return c.json({ error: "invalid azure authentication" }, 401);
    const input = obj(await c.req.json());
    input.model = c.req.param("model");
    return app.fetch(
      new Request(`http://fixture/openai/v1/${c.req.param("operation")}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(input),
      }),
    );
  });
  app.post("/azure/openai/deployments/:model/chat/completions", async (c) => {
    if (!c.req.header("api-key") || c.req.header("authorization") || !c.req.query("api-version"))
      return c.json({ error: "invalid azure authentication" }, 401);
    const input = obj(await c.req.json());
    input.model = c.req.param("model");
    return app.fetch(
      new Request("http://fixture/openai/v1/chat/completions", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(input),
      }),
    );
  });
}
