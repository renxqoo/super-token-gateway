import type { Hono } from "hono";

export function registerVertex(app: Hono): void {
  app.post("/vertex/v1/projects/test/locations/test/publishers/google/models/:action", (c) => {
    if (c.req.header("authorization") !== "Bearer fixture-vertex" || c.req.header("x-goog-api-key"))
      return c.json({ error: "invalid vertex authentication" }, 401);
    const url = new URL(c.req.url);
    url.pathname = `/gemini/v1beta/models/${c.req.param("action")}`;
    return app.fetch(new Request(url, c.req.raw));
  });
}
