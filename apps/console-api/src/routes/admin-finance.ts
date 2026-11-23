import { Hono } from "hono";
import { z } from "zod";
import { GatewayError } from "@gateway/contracts";
import { recordAuditLog } from "../auth.ts";
import {
  currencySchema,
  nonNegativeAtomsSchema,
  operationEnum,
  parseJsonBody,
  parseUuid,
  positiveAtomsSchema,
  signedAtomsSchema,
  type ConsoleDeps,
  type ConsoleEnv,
} from "./shared.ts";

const topupSchema = z
  .object({
    currency: currencySchema.optional(),
    external_ref: z.string().min(1).max(200),
    amount_atoms: positiveAtomsSchema,
    reason: z.string().max(500).optional(),
  })
  .strict();

const grantSchema = z
  .object({
    currency: currencySchema.optional(),
    external_ref: z.string().min(1).max(200),
    amount_atoms: positiveAtomsSchema,
    priority: z.number().int().min(0).max(10_000).optional(),
    expires_at: z.iso.datetime({ offset: true }).nullable().optional(),
    models: z.array(z.string().min(1).max(150)).max(1000).optional(),
    operations: z.array(operationEnum).max(32).optional(),
    reason: z.string().max(500).optional(),
  })
  .strict();

const creditPolicySchema = z
  .object({
    currency: currencySchema.optional(),
    expected_version: nonNegativeAtomsSchema,
    credit_limit_atoms: nonNegativeAtomsSchema,
    policy: z.enum(["GIFT_FIRST", "CASH_FIRST", "EARLIEST_EXPIRY"]).optional(),
  })
  .strict();

const refundSchema = z
  .object({
    external_ref: z.string().min(1).max(200),
    amount_atoms: positiveAtomsSchema,
    reason: z.string().min(1).max(500),
  })
  .strict();

const adjustmentSchema = z
  .object({
    external_ref: z.string().min(1).max(200),
    delta_atoms: signedAtomsSchema,
    reason: z.string().min(1).max(500),
  })
  .strict();

const jsonObjectSchema = z.record(z.string(), z.unknown());

function resolveProjectCurrency(requested: string | undefined, projectCurrency: string): string {
  const effective = requested ?? projectCurrency;
  if (effective !== projectCurrency) {
    throw new GatewayError(
      400,
      "currency_mismatch",
      `Project currency is ${projectCurrency}; received ${effective}`,
    );
  }
  return effective;
}

export function createAdminFinanceRoutes(deps: ConsoleDeps) {
  const app = new Hono<ConsoleEnv>();

  app.post("/finance/projects/:projectId/topups", async (c) => {
    const actor = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const body = await parseJsonBody(c, topupSchema);
    const proj = await deps.sync.ensureProjectSynced(projectId);
    const currency = resolveProjectCurrency(body.currency, proj.currency);
    const res = await deps.gateway.creditWallet(proj.gatewayProjectId, {
      currency,
      external_ref: body.external_ref,
      amount_atoms: body.amount_atoms,
      kind: "CASH",
      ...(body.reason ? { reason: body.reason } : {}),
    });
    await recordAuditLog(deps.orm, {
      actor,
      orgId: proj.orgId,
      action: "admin.finance.topup",
      targetType: "project",
      targetId: projectId,
      metadata: { currency, amount_atoms: body.amount_atoms, external_ref: body.external_ref },
    });
    return c.json(res);
  });

  app.post("/finance/projects/:projectId/grants", async (c) => {
    const actor = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const body = await parseJsonBody(c, grantSchema);
    const proj = await deps.sync.ensureProjectSynced(projectId);
    const currency = resolveProjectCurrency(body.currency, proj.currency);
    const res = await deps.gateway.creditWallet(proj.gatewayProjectId, {
      currency,
      external_ref: body.external_ref,
      amount_atoms: body.amount_atoms,
      kind: "GRANT",
      ...(body.priority !== undefined ? { priority: body.priority } : {}),
      ...(body.expires_at !== undefined
        ? { expires_at: body.expires_at ? new Date(body.expires_at).toISOString() : null }
        : {}),
      ...(body.models ? { models: body.models } : {}),
      ...(body.operations ? { operations: body.operations } : {}),
      ...(body.reason ? { reason: body.reason } : {}),
    });
    await recordAuditLog(deps.orm, {
      actor,
      orgId: proj.orgId,
      action: "admin.finance.grant",
      targetType: "project",
      targetId: projectId,
      metadata: { currency, amount_atoms: body.amount_atoms, external_ref: body.external_ref },
    });
    return c.json(res);
  });

  app.post("/finance/projects/:projectId/credit-policy", async (c) => {
    const actor = c.get("user");
    const projectId = parseUuid(c.req.param("projectId"), "projectId");
    const body = await parseJsonBody(c, creditPolicySchema);
    const proj = await deps.sync.ensureProjectSynced(projectId);
    const currency = resolveProjectCurrency(body.currency, proj.currency);
    const res = await deps.gateway.configureCreditPolicy(proj.gatewayProjectId, {
      currency,
      expected_version: body.expected_version,
      credit_limit_atoms: body.credit_limit_atoms,
      ...(body.policy ? { policy: body.policy } : {}),
    });
    await recordAuditLog(deps.orm, {
      actor,
      orgId: proj.orgId,
      action: "admin.finance.credit_policy",
      targetType: "project",
      targetId: projectId,
      metadata: { currency, credit_limit_atoms: body.credit_limit_atoms },
    });
    return c.json(res);
  });

  app.post("/finance/charges/:chargeId/refunds", async (c) => {
    const actor = c.get("user");
    const chargeId = parseUuid(c.req.param("chargeId"), "chargeId");
    const body = await parseJsonBody(c, refundSchema);
    const res = await deps.gateway.refundCharge(chargeId, body);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.finance.refund",
      targetType: "charge",
      targetId: chargeId,
      metadata: { amount_atoms: body.amount_atoms, external_ref: body.external_ref },
    });
    return c.json(res);
  });

  app.post("/finance/charges/:chargeId/adjustments", async (c) => {
    const actor = c.get("user");
    const chargeId = parseUuid(c.req.param("chargeId"), "chargeId");
    const body = await parseJsonBody(c, adjustmentSchema);
    const res = await deps.gateway.adjustCharge(chargeId, body);
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.finance.adjustment",
      targetType: "charge",
      targetId: chargeId,
      metadata: { delta_atoms: body.delta_atoms, external_ref: body.external_ref },
    });
    return c.json(res);
  });

  app.get("/finance/holds", async (c) => {
    return c.json(
      await deps.gateway.listHolds({
        state: c.req.query("state"),
        project_id: c.req.query("project_id"),
        limit: c.req.query("limit"),
        offset: c.req.query("offset"),
      }),
    );
  });

  app.post("/finance/requests/:requestId/resolve", async (c) => {
    const actor = c.get("user");
    const requestId = parseUuid(c.req.param("requestId"), "requestId");
    const body = await parseJsonBody(c, jsonObjectSchema);
    const res = await deps.gateway.resolveRequest(requestId, {
      ...body,
      actor: actor.email.slice(0, 120),
    });
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.finance.resolve_request",
      targetType: "request",
      targetId: requestId,
    });
    return c.json(res);
  });

  app.post("/finance/requests/:requestId/attempts/:attemptId/resolve", async (c) => {
    const actor = c.get("user");
    const requestId = parseUuid(c.req.param("requestId"), "requestId");
    const attemptId = parseUuid(c.req.param("attemptId"), "attemptId");
    const body = await parseJsonBody(c, jsonObjectSchema);
    const res = await deps.gateway.resolveSupplierAttempt(requestId, attemptId, {
      ...body,
      actor: actor.email.slice(0, 120),
    });
    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.finance.resolve_supplier_attempt",
      targetType: "supplier_attempt",
      targetId: `${requestId}:${attemptId}`,
    });
    return c.json(res);
  });

  app.get("/finance/async-tasks", async (c) => {
    return c.json(
      await deps.gateway.listAsyncTasks({
        project_id: c.req.query("project_id"),
        status: c.req.query("status"),
        refund_eligibility: c.req.query("refund_eligibility"),
        limit: c.req.query("limit"),
        offset: c.req.query("offset"),
      }),
    );
  });

  app.get("/finance/conservation", async (c) => {
    const audit = await deps.gateway.getConservationAudit();
    return c.json(audit.data, audit.status as 200 | 500);
  });

  return app;
}
