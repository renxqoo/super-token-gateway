import type { SQL, TransactionSQL } from "bun";
import type { Hono } from "hono";
import { z } from "zod";
import { currencySchema } from "@gateway/contracts";
import {
  createUpstreamAccountStore,
  recordUpstreamAdjustment,
  recordUpstreamReconciliation,
  createUpstreamFundingStore,
} from "@gateway/storage";
import type { PlatformPermission } from "./authentication.ts";

export function registerUpstreamFunds(
  app: Hono,
  tools: {
    run<T>(
      header: string | undefined,
      permission: PlatformPermission,
      action: (tx: TransactionSQL, actor: string) => Promise<T>,
    ): Promise<T>;
    runFunding<T>(
      request: Request,
      permission: PlatformPermission,
      account: string,
      catalog: boolean,
      action: (tx: SQL, actor: string) => Promise<T>,
    ): Promise<T>;
    body(request: Request): Promise<unknown>;
  },
) {
  const { run, runFunding, body } = tools;
  const id = (value: string | undefined) => z.uuid().parse(value);
  const auth = (c: { req: { header(name: string): string | undefined } }) =>
    c.req.header("authorization");
  app.get("/supplier-brands", async (c) =>
    c.json({
      data: await run(auth(c), "funding:read", (tx) => createUpstreamAccountStore(tx).brands()),
    }),
  );
  app.post("/supplier-brands", async (c) => {
    const input = await body(c.req.raw);
    return c.json(
      await run(auth(c), "catalog:manage", (tx, actor) =>
        createUpstreamAccountStore(tx).createBrand(input, actor),
      ),
      201,
    );
  });
  app.patch("/supplier-brands/:id", async (c) => {
    const brand = id(c.req.param("id")),
      input = await body(c.req.raw);
    return c.json(
      await run(auth(c), "catalog:manage", (tx, actor) =>
        createUpstreamAccountStore(tx).updateBrand(brand, input, actor),
      ),
    );
  });
  app.get("/upstream-accounts", async (c) =>
    c.json({
      data: await run(auth(c), "funding:read", (tx) => createUpstreamAccountStore(tx).accounts()),
    }),
  );
  app.post("/upstream-accounts", async (c) => {
    const input = await body(c.req.raw);
    return c.json(
      await run(auth(c), "catalog:manage", (tx, actor) =>
        createUpstreamAccountStore(tx).createAccount(input, actor),
      ),
      201,
    );
  });
  app.get("/upstream-accounts/:id", async (c) => {
    const account = id(c.req.param("id"));
    return c.json(
      await run(auth(c), "funding:read", (tx) => createUpstreamAccountStore(tx).account(account)),
    );
  });
  app.patch("/upstream-accounts/:id", async (c) => {
    const account = id(c.req.param("id")),
      input = await body(c.req.raw);
    return c.json(
      await runFunding(c.req.raw, "funding:adjust", account, true, (tx, actor) =>
        createUpstreamAccountStore(tx).updateAccount(account, input, actor),
      ),
    );
  });
  app.get("/upstream-accounts/:id/recharges", async (c) => {
    const account = id(c.req.param("id"));
    return c.json({
      data: await run(auth(c), "funding:read", (tx) =>
        createUpstreamAccountStore(tx).recharges(account),
      ),
    });
  });
  app.post("/upstream-accounts/:id/adjustments", async (c) => {
    const account = id(c.req.param("id")),
      input = await body(c.req.raw);
    return c.json(
      await runFunding(c.req.raw, "funding:adjust", account, true, (tx, actor) =>
        recordUpstreamAdjustment(tx, account, input as never, actor),
      ),
      201,
    );
  });
  const ledgerFilter = (c: { req: { query(name: string): string | undefined } }) => {
    const currency = c.req.query("currency"),
      channel = c.req.query("channel_id"),
      before = c.req.query("before");
    return {
      ...(currency === undefined ? {} : { currency: currencySchema.parse(currency) }),
      ...(channel === undefined ? {} : { channel: id(channel) }),
      ...(before === undefined ? {} : { before: id(before) }),
    };
  };
  app.get("/upstream-accounts/:id/holds", async (c) => {
    const account = id(c.req.param("id")),
      filter = ledgerFilter(c);
    return c.json({
      data: await run(auth(c), "funding:read", (tx) =>
        createUpstreamAccountStore(tx).holds(account, filter),
      ),
    });
  });
  app.get("/upstream-accounts/:id/journal", async (c) => {
    const account = id(c.req.param("id")),
      filter = ledgerFilter(c);
    return c.json({
      data: await run(auth(c), "funding:read", (tx) =>
        createUpstreamAccountStore(tx).journal(account, filter),
      ),
    });
  });
  app.get("/upstream-accounts/:id/reconciliations", async (c) => {
    const account = id(c.req.param("id"));
    return c.json({
      data: await run(auth(c), "funding:read", (tx) =>
        createUpstreamAccountStore(tx).reconciliations(account),
      ),
    });
  });
  app.post("/upstream-accounts/:id/reconciliations", async (c) => {
    const account = id(c.req.param("id")),
      input = await body(c.req.raw);
    const result = await runFunding(c.req.raw, "funding:adjust", account, true, (tx, actor) =>
      recordUpstreamReconciliation(tx, account, input as never, actor),
    );
    return c.json(result, result.replayed ? 200 : 201);
  });
  app.get("/financial-vouchers/:id", async (c) => {
    const voucher = id(c.req.param("id"));
    const found = await run(auth(c), "funding:read", (tx) =>
      createUpstreamFundingStore(tx).voucher(voucher),
    );
    return new Response(new Uint8Array(found.content), {
      headers: {
        "content-type": found.content_type,
        "content-disposition": `attachment; filename="voucher-${voucher}"`,
        "x-content-type-options": "nosniff",
        "cache-control": "no-store",
      },
    });
  });
}
