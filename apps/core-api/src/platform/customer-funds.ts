import type { TransactionSQL } from "bun";
import type { Hono } from "hono";
import { z } from "zod";
import {
  adjustmentSchema,
  creditPolicySchema,
  creditSchema,
  currencySchema,
  refundSchema,
  resolutionSchema,
  tariffSchema,
} from "@gateway/contracts";
import { createCustomerFundsStore } from "@gateway/storage";
import type { PlatformPermission } from "./authentication.ts";

export function registerCustomerFunds(
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
  const wallet = (project: string, currency: string) => `${project}:${currency}`;
  app.post("/projects/:id/credits", async (c) => {
    const project = id(c.req.param("id")),
      input = creditSchema.parse(await body(c.req.raw));
    return c.json(
      await run(c.req.header("authorization"), "customer:fund", async (tx, actor) => {
        const receipt = await createCustomerFundsStore(tx).credit(project, input);
        await audit(tx, actor, "customer.credit", project);
        return receipt;
      }),
      201,
    );
  });
  app.get("/projects/:id/wallets", async (c) => {
    const project = id(c.req.param("id"));
    return c.json({
      data: await run(c.req.header("authorization"), "customer:read", (tx) =>
        createCustomerFundsStore(tx).wallets(project),
      ),
    });
  });
  app.post("/projects/:id/wallets/:currency/credit-policy", async (c) => {
    const project = id(c.req.param("id")),
      currency = currencySchema.parse(c.req.param("currency")),
      input = creditPolicySchema.parse(await body(c.req.raw));
    return c.json(
      await run(c.req.header("authorization"), "customer:fund", async (tx, actor) => {
        const receipt = await createCustomerFundsStore(tx).configureCredit(
          project,
          currency,
          input,
        );
        await audit(tx, actor, "customer.credit_policy", wallet(project, currency));
        return receipt;
      }),
    );
  });
  app.get("/projects/:id/wallets/:currency/lots", async (c) => {
    const project = id(c.req.param("id")),
      currency = currencySchema.parse(c.req.param("currency"));
    return c.json({
      data: await run(c.req.header("authorization"), "customer:read", (tx) =>
        createCustomerFundsStore(tx).lots(project, currency),
      ),
    });
  });
  app.get("/projects/:id/wallets/:currency/journal", async (c) => {
    const project = id(c.req.param("id")),
      currency = currencySchema.parse(c.req.param("currency")),
      before = c.req.query("before") === undefined ? undefined : id(c.req.query("before"));
    return c.json({
      data: await run(c.req.header("authorization"), "customer:read", (tx) =>
        createCustomerFundsStore(tx).journal(project, currency, before),
      ),
    });
  });
  app.patch("/projects/:id/wallets/:currency", async (c) => {
    const project = id(c.req.param("id")),
      currency = currencySchema.parse(c.req.param("currency")),
      input = z
        .strictObject({
          policy: z.enum(["GIFT_FIRST", "CASH_FIRST", "EARLIEST_EXPIRY"]).optional(),
          frozen: z.boolean().optional(),
        })
        .parse(await body(c.req.raw));
    return c.json(
      await run(c.req.header("authorization"), "customer:fund", async (tx, actor) => {
        await createCustomerFundsStore(tx).configure(project, currency, input);
        await audit(tx, actor, "customer.wallet_update", wallet(project, currency));
        return { updated: true };
      }),
    );
  });
  app.get("/tariffs", async (c) => {
    const model = c.req.query("model");
    return c.json({
      data: await run(c.req.header("authorization"), "customer:read", (tx) =>
        createCustomerFundsStore(tx).tariffs(model),
      ),
    });
  });
  app.post("/tariffs", async (c) => {
    const input = tariffSchema.parse(await body(c.req.raw));
    return c.json(
      await run(c.req.header("authorization"), "catalog:manage", async (tx, actor) => {
        const receipt = await createCustomerFundsStore(tx).setTariff(input);
        await audit(tx, actor, "tariff.set", receipt.id);
        return receipt;
      }),
      201,
    );
  });
  app.post("/charges/:id/refunds", async (c) => {
    const charge = id(c.req.param("id")),
      input = refundSchema.parse(await body(c.req.raw));
    return c.json(
      await run(c.req.header("authorization"), "customer:adjust", async (tx, actor) => {
        const receipt = await createCustomerFundsStore(tx).refund(charge, input);
        await audit(tx, actor, "customer.refund", charge);
        return receipt;
      }),
      201,
    );
  });
  app.post("/charges/:id/adjustments", async (c) => {
    const charge = id(c.req.param("id")),
      input = adjustmentSchema.parse(await body(c.req.raw));
    return c.json(
      await run(c.req.header("authorization"), "customer:adjust", async (tx, actor) => {
        const receipt = await createCustomerFundsStore(tx).adjust(charge, input);
        await audit(tx, actor, "customer.adjustment", charge);
        return receipt;
      }),
      201,
    );
  });
  app.post("/requests/:id/resolutions", async (c) => {
    const request = id(c.req.param("id")),
      input = resolutionSchema.parse(await body(c.req.raw));
    return c.json(
      await run(c.req.header("authorization"), "customer:adjust", async (tx, actor) => {
        const receipt = await createCustomerFundsStore(tx).resolve(request, input);
        await audit(tx, actor, "customer.resolution", request);
        return receipt;
      }),
      201,
    );
  });
}
