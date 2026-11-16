import { Hono } from "hono";
import { z } from "zod";
import { GatewayError } from "@gateway/contracts";
import { readJsonBody } from "../request-body.ts";
import type { CatalogApplication } from "./application.ts";
import { versionCommandSchema } from "./application.ts";
export function catalogRoutes(application: CatalogApplication) {
  const app = new Hono();
  app.onError((error, c) => {
    if (error instanceof GatewayError)
      return c.json({ error: { code: error.code, message: error.message } }, error.status as 400);
    if (error instanceof z.ZodError || error instanceof SyntaxError || error instanceof TypeError)
      return c.json(
        { error: { code: "invalid_catalog_input", message: "Invalid catalog input" } },
        400,
      );
    return c.json(
      { error: { code: "catalog_unavailable", message: "Catalog operation failed" } },
      503,
    );
  });
  const actor = (header: string | undefined) => application.authenticate(header);
  const body = (request: Request, limit = 65536) =>
    readJsonBody(request, limit, limit > 65536 ? 120000 : 10000);
  app.get("/plugins", async (c) =>
    c.json({ data: await application.versions(await actor(c.req.header("authorization"))) }),
  );
  app.post("/plugins", async (c) =>
    c.json(
      await application.upload(await actor(c.req.header("authorization")), () =>
        body(c.req.raw, 64 << 20),
      ),
      201,
    ),
  );
  for (const action of ["disable", "quarantine"] as const)
    app.post(`/plugins/:id/${action}`, async (c) =>
      c.json(
        await application[action](
          await actor(c.req.header("authorization")),
          z.uuid().parse(c.req.param("id")),
          await body(c.req.raw),
        ),
      ),
    );
  app.get("/providers/:id/plugin-bindings", async (c) =>
    c.json({
      data: await application.bindings(
        await actor(c.req.header("authorization")),
        z.uuid().parse(c.req.param("id")),
      ),
    }),
  );
  app.post("/plugins/install-bundled-release", async (c) =>
    c.json(
      await application.installBundled(
        await actor(c.req.header("authorization")),
        await body(c.req.raw),
        c.req.raw.signal,
      ),
      201,
    ),
  );
  app.post("/plugins/:id/validate", async (c) =>
    c.json(
      await application.validate(
        await actor(c.req.header("authorization")),
        z.uuid().parse(c.req.param("id")),
        versionCommandSchema.parse(await body(c.req.raw)),
        c.req.raw.signal,
      ),
    ),
  );
  app.post("/plugins/:id/publish", async (c) =>
    c.json(
      await application.publish(
        await actor(c.req.header("authorization")),
        z.uuid().parse(c.req.param("id")),
        versionCommandSchema.parse(await body(c.req.raw)),
      ),
    ),
  );
  app.post("/plugins/:id/recover", async (c) =>
    c.json(
      await application.recover(
        await actor(c.req.header("authorization")),
        z.uuid().parse(c.req.param("id")),
        await body(c.req.raw),
      ),
    ),
  );
  app.get("/plugins/:id", async (c) =>
    c.json(
      await application.truth(
        await actor(c.req.header("authorization")),
        z.uuid().parse(c.req.param("id")),
      ),
    ),
  );
  app.put("/providers/:id/plugin-binding", async (c) =>
    c.json(
      await application.bind(
        await actor(c.req.header("authorization")),
        z.uuid().parse(c.req.param("id")),
        await body(c.req.raw),
      ),
      201,
    ),
  );
  app.get("/providers/:id/plugin-binding", async (c) =>
    c.json(
      await application.binding(
        await actor(c.req.header("authorization")),
        z.uuid().parse(c.req.param("id")),
      ),
    ),
  );
  return app;
}
