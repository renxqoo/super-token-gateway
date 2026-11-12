import type { SQL, TransactionSQL } from "bun";
import { Hono } from "hono";
import { z } from "zod";
import {
  GatewayError,
  providerSchema,
  modelSchema,
  projectSchema,
  keySchema,
  routeSchema,
  routePatchSchema,
  tariffSchema,
  creditSchema,
  upstreamRechargeSchema,
} from "@gateway/contracts";
import {
  Store,
  FundsStore,
  recordProvider,
  recordRoute,
  recordRouteUpdate,
  recordBillingTariff,
  createPluginCatalogStore,
  recordFinancialVoucher,
  recordUpstreamRecharge,
  createUpstreamFundingStore,
} from "@gateway/storage";
import { createKey, encrypt, verifier } from "@gateway/runtime/crypto";
import { readBounded, validateCidr } from "@gateway/runtime";
import { createPlatformAuthenticator, type PlatformPermission } from "./authentication.ts";
export function createCoreManagementApplication(options: {
  db: SQL;
  adminKey: string;
  pepper: string;
  encryptionKey: string;
}) {
  const auth = createPlatformAuthenticator(createPluginCatalogStore(options.db), options);
  const app = new Hono();
  const id = (value: string) => z.uuid().parse(value);
  const body = async (request: Request) =>
    JSON.parse(
      new TextDecoder("utf-8", { fatal: true }).decode(
        await readBounded(
          request.body,
          8 << 20,
          AbortSignal.any([request.signal, AbortSignal.timeout(10000)]),
        ),
      ),
    );
  async function run<T>(
    header: string | undefined,
    permission: PlatformPermission,
    action: (tx: TransactionSQL, actor: string) => Promise<T>,
  ) {
    const actor = await auth.authenticate(header),
      proof = auth.authorization(actor, permission);
    return options.db.begin(async (tx) => {
      await tx`SELECT core_platform_authorize_v1(${proof}::jsonb,${permission})`;
      const result = await action(tx, actor.identity);
      await tx`SELECT core_platform_authorize_v1(${proof}::jsonb,${permission})`;
      return result;
    });
  }
  app.onError((error, c) => {
    if (error instanceof GatewayError)
      return c.json({ error: { code: error.code, message: error.message } }, error.status as 400);
    if (error instanceof z.ZodError || error instanceof SyntaxError)
      return c.json(
        { error: { code: "invalid_management_input", message: "Invalid management input" } },
        400,
      );
    if ((error as { errno?: string }).errno === "P0403")
      return c.json(
        { error: { code: "platform_scope_denied", message: "Platform permission required" } },
        403,
      );
    if ((error as { errno?: string }).errno === "23505")
      return c.json(
        { error: { code: "management_conflict", message: "Management object conflicts" } },
        409,
      );
    return c.json(
      { error: { code: "management_unavailable", message: "Management operation failed" } },
      503,
    );
  });
  const audit = async (tx: TransactionSQL, actor: string, action: string, resource: string) => {
    await tx`INSERT INTO audit_events(id,action,resource_id,actor,details) VALUES(${crypto.randomUUID()},${action},${resource},${actor},${{ resource_id: resource }}::jsonb)`;
  };
  app.post("/providers", async (c) => {
    const input = providerSchema
      .extend({ account_id: z.uuid().optional(), brand_id: z.uuid().optional() })
      .superRefine((v, c) => {
        if (Boolean(v.account_id) !== Boolean(v.brand_id))
          c.addIssue({ code: "custom", message: "Account and brand must be provided together" });
      })
      .parse(await body(c.req.raw));
    const url = new URL(input.base_url);
    if (
      url.protocol !== "https:" ||
      url.username ||
      url.password ||
      url.search ||
      url.hash ||
      input.api_key.length > 4096 ||
      [...Buffer.from(input.api_key)].some((byte) => byte < 33 || byte > 126)
    )
      throw new GatewayError(
        400,
        "invalid_provider",
        "HTTPS origin and bounded credential required",
      );
    return c.json(
      await run(c.req.header("authorization"), "catalog:manage", async (tx, actor) => {
        const channel = crypto.randomUUID(),
          secret = encrypt(input.api_key, options.encryptionKey, channel),
          provider = await recordProvider(
            tx,
            channel,
            input,
            secret,
            input.account_id && input.brand_id
              ? { account_id: input.account_id, brand_id: input.brand_id }
              : undefined,
          );
        await audit(tx, actor, "provider.create", channel);
        return provider;
      }),
      201,
    );
  });
  app.get("/providers", async (c) =>
    c.json({
      data: await run(c.req.header("authorization"), "funding:read", (tx) =>
        new Store(tx).providers(),
      ),
    }),
  );
  app.patch("/providers/:id", async (c) => {
    const channel = id(c.req.param("id")),
      input = z
        .strictObject({
          enabled: z.boolean().optional(),
          rpm: z.number().int().positive().max(10000000).optional(),
          concurrency: z.number().int().positive().max(100000).optional(),
          api_key: z
            .string()
            .min(1)
            .max(4096)
            .regex(/^[\x21-\x7e]+$/)
            .optional(),
        })
        .parse(await body(c.req.raw));
    return c.json(
      await run(c.req.header("authorization"), "catalog:manage", async (tx, actor) => {
        await new Store(tx).updateProvider(
          channel,
          input,
          input.api_key ? encrypt(input.api_key, options.encryptionKey, channel) : undefined,
        );
        await audit(tx, actor, "provider.update", channel);
        return { updated: true };
      }),
    );
  });
  app.post("/models", async (c) => {
    const input = modelSchema.parse(await body(c.req.raw));
    return c.json(
      await run(c.req.header("authorization"), "catalog:manage", async (tx, actor) => {
        await new Store(tx).createModel(input);
        await audit(tx, actor, "model.create", input.name);
        return { name: input.name };
      }),
      201,
    );
  });
  app.post("/projects", async (c) => {
    const input = projectSchema.parse(await body(c.req.raw));
    return c.json(
      await run(c.req.header("authorization"), "catalog:manage", async (tx, actor) => {
        const project = await new Store(tx).createProject(input);
        await audit(tx, actor, "project.create", project);
        return { id: project };
      }),
      201,
    );
  });
  app.post("/projects/:id/keys", async (c) => {
    const project = id(c.req.param("id")),
      input = keySchema.parse(await body(c.req.raw));
    input.allowed_cidrs = input.allowed_cidrs.map(validateCidr);
    return c.json(
      await run(c.req.header("authorization"), "catalog:manage", async (tx, actor) => {
        const token = createKey();
        const key = await new Store(tx).createCredential(
          project,
          input,
          verifier(token, options.pepper),
          token.slice(0, 12),
        );
        await audit(tx, actor, "key.create", key);
        return { id: key, key: token };
      }),
      201,
    );
  });
  app.post("/projects/:id/credits", async (c) => {
    const project = id(c.req.param("id")),
      input = creditSchema.parse(await body(c.req.raw));
    return c.json(
      await run(c.req.header("authorization"), "customer:fund", async (tx, actor) => {
        const receipt = await new FundsStore(tx).credit(project, input);
        await audit(tx, actor, "customer.credit", project);
        return receipt;
      }),
      201,
    );
  });
  app.post("/billing/tariffs", async (c) => {
    const input = tariffSchema.parse(await body(c.req.raw));
    return c.json(
      await run(c.req.header("authorization"), "catalog:manage", async (tx, actor) => {
        const receipt = await recordBillingTariff(tx, input);
        await audit(tx, actor, "tariff.set", receipt.id);
        return receipt;
      }),
      201,
    );
  });
  app.post("/models/:model/routes", async (c) => {
    const model = c.req.param("model"),
      input = routeSchema.parse(await body(c.req.raw));
    return c.json(
      await run(c.req.header("authorization"), "catalog:manage", async (tx, actor) => {
        const route = await recordRoute(tx, model, input);
        await audit(tx, actor, "route.create", route);
        return { id: route };
      }),
      201,
    );
  });
  app.patch("/routes/:id", async (c) => {
    const route = id(c.req.param("id")),
      input = routePatchSchema.parse(await body(c.req.raw));
    return c.json(
      await run(c.req.header("authorization"), "catalog:manage", async (tx, actor) => {
        await recordRouteUpdate(tx, route, input);
        await audit(tx, actor, "route.update", route);
        return { updated: true };
      }),
    );
  });
  app.post("/financial-vouchers", async (c) => {
    const input = z
      .strictObject({
        content_base64: z.string().max(7 << 20),
        content_type: z.enum(["image/png", "image/jpeg", "application/pdf"]),
        digest: z.string().regex(/^[a-f0-9]{64}$/),
        expires_at: z.string(),
      })
      .parse(await body(c.req.raw));
    const bytes = Buffer.from(input.content_base64, "base64");
    if (bytes.toString("base64") !== input.content_base64)
      throw new GatewayError(400, "invalid_voucher_content", "Canonical base64 required");
    return c.json(
      await run(c.req.header("authorization"), "funding:recharge", async (tx, actor) => {
        const receipt = await recordFinancialVoucher(
          tx,
          {
            content: bytes,
            content_type: input.content_type,
            digest: input.digest,
            expires_at: input.expires_at,
          },
          actor,
        );
        await audit(tx, actor, "voucher.register", receipt.id);
        return receipt;
      }),
      201,
    );
  });
  app.post("/upstream-accounts/:id/recharges", async (c) => {
    const account = id(c.req.param("id")),
      input = upstreamRechargeSchema.parse(await body(c.req.raw));
    return c.json(
      await run(c.req.header("authorization"), "funding:recharge", (tx, actor) =>
        recordUpstreamRecharge(tx, account, input, actor),
      ),
      201,
    );
  });
  app.get("/upstream-accounts/:id/balances", async (c) =>
    c.json({
      data: await run(c.req.header("authorization"), "funding:read", (tx) =>
        createUpstreamFundingStore(tx).balances(id(c.req.param("id"))),
      ),
    }),
  );
  app.get("/upstream-accounts/:id/recharges/:ref", async (c) =>
    c.json(
      await run(c.req.header("authorization"), "funding:read", (tx) =>
        createUpstreamFundingStore(tx).receipt(id(c.req.param("id")), c.req.param("ref")),
      ),
    ),
  );
  return app;
}
