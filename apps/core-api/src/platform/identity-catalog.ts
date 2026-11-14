import type { TransactionSQL } from "bun";
import type { Hono } from "hono";
import { z } from "zod";
import {
  keyPatchSchema,
  modelPatchSchema,
  organizationPatchSchema,
  organizationSchema,
  principalPatchSchema,
  principalSchema,
  projectPatchSchema,
} from "@gateway/contracts";
import { validateCidr } from "@gateway/runtime";
import { Store } from "@gateway/storage";
import type { PlatformPermission } from "./authentication.ts";

export function registerIdentityCatalog(
  app: Hono,
  tools: {
    run<T>(
      header: string | undefined,
      permission: PlatformPermission,
      action: (tx: TransactionSQL, actor: string) => Promise<T>,
    ): Promise<T>;
    audit(tx: TransactionSQL, actor: string, action: string, resource: string): Promise<void>;
    body(request: Request): Promise<unknown>;
  },
) {
  const { run, audit, body } = tools;
  const id = (value: string | undefined) => z.uuid().parse(value);
  const store = (tx: TransactionSQL) => new Store(tx);
  const write = <T>(
    header: string | undefined,
    action: string,
    resource: string,
    change: (tx: TransactionSQL) => Promise<T>,
  ) =>
    run(header, "catalog:manage", async (tx, actor) => {
      const result = await change(tx);
      await audit(tx, actor, action, resource);
      return result;
    });
  app.post("/organizations", async (c) => {
    const input = organizationSchema.parse(await body(c.req.raw));
    return c.json(
      await run(c.req.header("authorization"), "catalog:manage", async (tx, actor) => {
        const organization = await store(tx).createOrganization(input);
        await audit(tx, actor, "organization.create", organization);
        return { id: organization };
      }),
      201,
    );
  });
  app.patch("/organizations/:id", async (c) => {
    const organization = id(c.req.param("id")),
      input = organizationPatchSchema.parse(await body(c.req.raw));
    return c.json(
      await write(
        c.req.header("authorization"),
        "organization.update",
        organization,
        async (tx) => {
          await store(tx).updateOrganization(organization, input);
          return { updated: true };
        },
      ),
    );
  });
  app.post("/organizations/:id/principals", async (c) => {
    const organization = id(c.req.param("id")),
      input = principalSchema.parse(await body(c.req.raw));
    return c.json(
      await run(c.req.header("authorization"), "catalog:manage", async (tx, actor) => {
        const principal = await store(tx).createPrincipal(organization, input);
        await audit(tx, actor, "principal.create", principal);
        return { id: principal };
      }),
      201,
    );
  });
  app.patch("/principals/:id", async (c) => {
    const principal = id(c.req.param("id")),
      input = principalPatchSchema.parse(await body(c.req.raw));
    return c.json(
      await write(c.req.header("authorization"), "principal.update", principal, async (tx) => {
        await store(tx).updatePrincipal(principal, input);
        return { updated: true };
      }),
    );
  });
  app.get("/projects", async (c) =>
    c.json({
      data: await run(c.req.header("authorization"), "catalog:read", (tx) => store(tx).projects()),
    }),
  );
  app.patch("/projects/:id", async (c) => {
    const project = id(c.req.param("id")),
      input = projectPatchSchema.parse(await body(c.req.raw));
    return c.json(
      await write(c.req.header("authorization"), "project.update", project, async (tx) => {
        await store(tx).updateProject(project, input);
        return { updated: true };
      }),
    );
  });
  app.get("/projects/:id/keys", async (c) => {
    const project = id(c.req.param("id"));
    return c.json({
      data: await run(c.req.header("authorization"), "catalog:read", (tx) =>
        store(tx).keys(project),
      ),
    });
  });
  app.patch("/keys/:id", async (c) => {
    const key = id(c.req.param("id")),
      input = keyPatchSchema.parse(await body(c.req.raw));
    if (input.allowed_cidrs) input.allowed_cidrs = input.allowed_cidrs.map(validateCidr);
    return c.json(
      await write(c.req.header("authorization"), "key.update", key, async (tx) => {
        await store(tx).updateCredential(key, input);
        return { updated: true };
      }),
    );
  });
  app.delete("/keys/:id", async (c) => {
    const key = id(c.req.param("id"));
    return c.json(
      await write(c.req.header("authorization"), "key.revoke", key, async (tx) => {
        await store(tx).revokeKey(key);
        return { revoked: true };
      }),
    );
  });
  app.get("/models", async (c) =>
    c.json({
      data: await run(c.req.header("authorization"), "catalog:read", (tx) =>
        store(tx).adminModels(),
      ),
    }),
  );
  app.patch("/models/:model", async (c) => {
    const model = z.string().min(1).max(150).parse(c.req.param("model")),
      input = modelPatchSchema.parse(await body(c.req.raw));
    return c.json(
      await write(c.req.header("authorization"), "model.update", model, async (tx) => {
        await store(tx).updateModel(model, input);
        return { updated: true };
      }),
    );
  });
  app.get("/models/:model/routes", async (c) => {
    const model = z.string().min(1).max(150).parse(c.req.param("model"));
    return c.json({
      data: await run(c.req.header("authorization"), "catalog:read", (tx) =>
        store(tx).routeDefinitions(model),
      ),
    });
  });
}
