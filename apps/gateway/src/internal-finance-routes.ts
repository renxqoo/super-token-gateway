import {
  creditWalletSchema,
  creditPolicySchema,
  adjustWalletSchema,
  setWalletFreezeSchema,
  refundChargeSchema,
  adjustChargeSchema,
  rechargeSupplierSchema,
  adjustSupplierBalanceSchema,
  resolveRequestSchema,
  resolveSupplierAttemptSchema,
  reconciliationReceiptSchema,
  reconciliationPolicySchema,
  billingAccountFilter,
  claimEventsSchema,
  requireConsumer,
  ackEventsSchema,
} from "./internal-finance-schemas.ts";
import {
  parseUuid,
  parseBody,
  currencySchema,
  nonNegativeAtomsSchema,
} from "./internal-request.ts";
import type {
  LotKind,
  WalletPolicy,
  AsyncTaskStatus,
  AsyncTaskRefundEligibility,
} from "@gateway/ledger";
import type { Usage } from "@gateway/kernel";
import { z } from "zod";
import { usageSchema, assertKnownQueryParameters } from "@gateway/contracts";
import type { InternalApiDeps, InternalJsonRoute, InternalRoute } from "./internal-route-types.ts";

export function registerInternalFinanceRoutes({
  jsonRoute,
  deps,
  route,
  onConfigUpdated,
}: {
  jsonRoute: InternalJsonRoute;
  deps: InternalApiDeps;
  route: InternalRoute;
  onConfigUpdated: (epoch: number) => Promise<void>;
}) {
  jsonRoute(
    "POST",
    "/internal/v1/wallets/:billingAccountId/credits",
    "finance",
    creditWalletSchema,
    async (c, body) => {
      const billingAccountId = parseUuid(c.req.param("billingAccountId"), "billingAccountId");
      const res = await deps.ledger.creditWallet({
        billing_account_id: billingAccountId,
        currency: body.currency,
        external_ref: body.external_ref,
        amount_atoms: body.amount_atoms,
        kind: body.kind as LotKind,
        ...(body.priority !== undefined ? { priority: body.priority } : {}),
        expires_at: body.expires_at ?? null,
        ...(body.models ? { models: body.models } : {}),
        ...(body.operations ? { operations: body.operations } : {}),
        ...(body.reason ? { reason: body.reason } : {}),
      });
      return c.json(res);
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/wallets/:billingAccountId/credit-policy",
    "finance",
    creditPolicySchema,
    async (c, body) => {
      const billingAccountId = parseUuid(c.req.param("billingAccountId"), "billingAccountId");
      const res = await deps.ledger.configureWalletCredit({
        billing_account_id: billingAccountId,
        currency: body.currency,
        expected_version: body.expected_version,
        credit_limit_atoms: body.credit_limit_atoms,
        ...(body.policy ? { policy: body.policy as WalletPolicy } : {}),
      });
      return c.json(res);
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/wallets/:billingAccountId/adjustments",
    "finance",
    adjustWalletSchema,
    async (c, body) => {
      const billingAccountId = parseUuid(c.req.param("billingAccountId"), "billingAccountId");
      const res = await deps.ledger.adjustWallet({
        billing_account_id: billingAccountId,
        currency: body.currency,
        external_ref: body.external_ref,
        delta_atoms: body.delta_atoms,
        ...(body.lot_kind ? { lot_kind: body.lot_kind as LotKind } : {}),
        ...(body.force_debt !== undefined ? { force_debt: body.force_debt } : {}),
        reason: body.reason,
        actor: c.get("principal").id,
      });
      return c.json(res);
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/wallets/:billingAccountId/freeze",
    "finance",
    setWalletFreezeSchema,
    async (c, body) => {
      const billingAccountId = parseUuid(c.req.param("billingAccountId"), "billingAccountId");
      const res = await deps.ledger.setWalletFreeze({
        billing_account_id: billingAccountId,
        currency: body.currency,
        frozen: body.frozen,
        reason: body.reason,
        actor: c.get("principal").id,
      });
      return c.json(res);
    },
  );

  route("GET", "/internal/v1/wallets/:billingAccountId", "auditor", async (c) => {
    const billingAccountId = parseUuid(c.req.param("billingAccountId"), "billingAccountId");
    const currency = parseBody(currencySchema, c.req.query("currency") ?? "USD");
    const includeLots = c.req.query("include_lots") === "true";
    const wallet = includeLots
      ? await deps.ledger.getWalletWithLots(billingAccountId, currency)
      : await deps.ledger.getWallet(billingAccountId, currency);
    if (!wallet) return c.json({ error: { code: "not_found", message: "Wallet not found" } }, 404);
    return c.json(wallet);
  });

  jsonRoute(
    "POST",
    "/internal/v1/charges/:chargeId/refunds",
    "finance",
    refundChargeSchema,
    async (c, body) => {
      const chargeId = parseUuid(c.req.param("chargeId"), "chargeId");
      const res = await deps.ledger.refundCharge({
        charge_id: chargeId,
        external_ref: body.external_ref,
        amount_atoms: body.amount_atoms,
        reason: body.reason,
      });
      return c.json(res);
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/charges/:chargeId/adjustments",
    "finance",
    adjustChargeSchema,
    async (c, body) => {
      const chargeId = parseUuid(c.req.param("chargeId"), "chargeId");
      const res = await deps.ledger.adjustCharge({
        charge_id: chargeId,
        external_ref: body.external_ref,
        delta_atoms: body.delta_atoms,
        reason: body.reason,
      });
      return c.json(res);
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/upstream-accounts/:accountId/recharges",
    "finance",
    rechargeSupplierSchema,
    async (c, body) => {
      const accountId = parseUuid(c.req.param("accountId"), "accountId");
      const res = await deps.ledger.rechargeSupplier({
        account_id: accountId,
        currency: body.currency,
        external_ref: body.external_ref,
        amount_atoms: body.amount_atoms,
        ...(body.reason ? { reason: body.reason } : {}),
      });
      return c.json(res);
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/upstream-accounts/:accountId/adjustments",
    "finance",
    adjustSupplierBalanceSchema,
    async (c, body) => {
      const accountId = parseUuid(c.req.param("accountId"), "accountId");
      const res = await deps.ledger.adjustSupplierBalance({
        account_id: accountId,
        currency: body.currency,
        external_ref: body.external_ref,
        delta_atoms: body.delta_atoms,
        ...(body.allow_negative !== undefined ? { allow_negative: body.allow_negative } : {}),
        reason: body.reason,
        actor: c.get("principal").id,
      });
      if (res.config_epoch !== undefined) {
        await onConfigUpdated(res.config_epoch);
      }
      return c.json(res);
    },
  );

  route("GET", "/internal/v1/upstream-accounts/:accountId/balance", "auditor", async (c) => {
    const accountId = parseUuid(c.req.param("accountId"), "accountId");
    const currency = parseBody(currencySchema, c.req.query("currency") ?? "USD");
    const bal = await deps.ledger.getSupplierBalance(accountId, currency);
    return c.json(bal);
  });

  jsonRoute(
    "POST",
    "/internal/v1/requests/:requestId/resolve",
    "finance",
    resolveRequestSchema,
    async (c, body) => {
      const requestId = parseUuid(c.req.param("requestId"), "requestId");
      const res = await deps.ledger.resolveRequest({
        request_id: requestId,
        action: body.action,
        ...(body.billing_price_id ? { billing_price_id: body.billing_price_id } : {}),
        ...(body.usage ? { usage: body.usage as Usage } : {}),
        ...(body.supplier_cost_atoms ? { supplier_cost_atoms: body.supplier_cost_atoms } : {}),
        ...(body.supplier_cost_version_id
          ? { supplier_cost_version_id: body.supplier_cost_version_id }
          : {}),
        reason: body.reason,
        actor: c.get("principal").id,
      });
      return c.json(res);
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/requests/:requestId/attempts/:attemptId/resolve",
    "finance",
    resolveSupplierAttemptSchema,
    async (c, body) => {
      const requestId = parseUuid(c.req.param("requestId"), "requestId");
      const attemptId = parseUuid(c.req.param("attemptId"), "attemptId");
      const result = await deps.ledger.resolveSupplierAttempt({
        ...body,
        actor: c.get("principal").id,
        request_id: requestId,
        attempt_id: attemptId,
      });
      if (result.ok) return c.json({ replayed: result.replayed, state: result.state });
      const status =
        result.code === "supplier_attempt_not_found"
          ? 404
          : result.code === "supplier_attempt_conflict"
            ? 409
            : 422;
      return c.json(
        { error: { code: result.code, message: "Supplier attempt resolution failed" } },
        status,
      );
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/reconciliation/receipts",
    "finance",
    z
      .object({
        receipts: z.array(reconciliationReceiptSchema).min(1).max(100),
      })
      .strict(),
    async (c, body) => {
      const actor = c.get("principal").id;
      const results = [];
      for (const receipt of body.receipts) {
        const result = await deps.ledger.submitReconciliationReceipt({
          ...receipt,
          ...(receipt.verdict === "FINAL" && receipt.customer_usage !== undefined
            ? { customer_usage: receipt.customer_usage as Usage }
            : {}),
          actor,
        });
        results.push({ attempt_id: receipt.attempt_id, ...result });
      }
      return c.json({ results });
    },
  );

  jsonRoute(
    "PUT",
    "/internal/v1/reconciliation/budgets/:currency",
    "finance",
    z.object({ limit_atoms: nonNegativeAtomsSchema }).strict(),
    async (c, body) => {
      const currency = parseBody(currencySchema, c.req.param("currency"));
      const result = await deps.ledger.putReconciliationBudget(currency, body.limit_atoms);
      return result
        ? c.json(result)
        : c.json(
            {
              error: {
                code: "reconciliation_budget_conflict",
                message: "Budget is below recorded commitments",
              },
            },
            409,
          );
    },
  );

  jsonRoute(
    "POST",
    "/internal/v1/reconciliation/receipts/:attemptId/review",
    "finance",
    z
      .object({
        action: z.enum(["RESUME", "DISMISS"]),
        review_evidence_ref: z.string().trim().min(1).max(512),
        reason: z.string().trim().min(1).max(500),
        customer_usage: usageSchema.nullable().optional(),
      })
      .strict(),
    async (c, body) => {
      const result = await deps.ledger.reviewReconciliationReceipt({
        ...body,
        attempt_id: parseUuid(c.req.param("attemptId"), "attemptId"),
        customer_usage: body.customer_usage as Usage | null | undefined,
        actor: c.get("principal").id,
      });
      return result
        ? c.json(result)
        : c.json(
            {
              error: {
                code: "reconciliation_review_conflict",
                message: "Only receipts awaiting review can be decided",
              },
            },
            409,
          );
    },
  );

  jsonRoute(
    "PUT",
    "/internal/v1/reconciliation/policies",
    "finance",
    reconciliationPolicySchema,
    async (c, body) => {
      const result = await deps.ledger.putReconciliationPolicy(body);
      return result
        ? c.json(result)
        : c.json(
            {
              error: {
                code: "reconciliation_policy_conflict",
                message: "Policy scope or available budget is invalid",
              },
            },
            409,
          );
    },
  );

  route("GET", "/internal/v1/reconciliation/cases", "auditor", async (c) => {
    const limit = parseBody(z.coerce.number().int().min(1).max(500), c.req.query("limit") ?? "100");
    const days = parseBody(
      z.coerce.number().int().min(0).max(36500),
      c.req.query("min_age_days") ?? "0",
    );
    return c.json({ data: await deps.ledger.listReconciliationCases(limit, days) });
  });

  route("GET", "/internal/v1/charges", "auditor", async (c) => {
    const billingAccountId = billingAccountFilter(
      c,
      "api_key_id",
      "outcome",
      "from",
      "to",
      "limit",
      "offset",
    );
    const apiKeyId = c.req.query("api_key_id");
    const outcome = c.req.query("outcome");
    const from = c.req.query("from");
    const to = c.req.query("to");
    const limit = c.req.query("limit") ? Number(c.req.query("limit")) : undefined;
    const offset = c.req.query("offset") ? Number(c.req.query("offset")) : undefined;
    const res = await deps.queries.listCharges({
      ...(billingAccountId ? { billing_account_id: billingAccountId } : {}),
      ...(apiKeyId ? { api_key_id: parseUuid(apiKeyId, "api_key_id") } : {}),
      ...(outcome
        ? {
            outcome: parseBody(
              z.enum(["SETTLED", "RELEASED", "DEBT", "MODERATION_BLOCKED", "UNKNOWN_RESOLVED"]),
              outcome,
            ),
          }
        : {}),
      ...(from ? { from } : {}),
      ...(to ? { to } : {}),
      ...(limit !== undefined && Number.isFinite(limit) ? { limit } : {}),
      ...(offset !== undefined && Number.isFinite(offset) ? { offset } : {}),
    });
    return c.json(res);
  });

  route("GET", "/internal/v1/async-tasks", "auditor", async (c) => {
    const billingAccountId = billingAccountFilter(
      c,
      "status",
      "refund_eligibility",
      "limit",
      "offset",
    );
    const status = c.req.query("status");
    const eligibility = c.req.query("refund_eligibility");
    const limit = c.req.query("limit") ? Number(c.req.query("limit")) : undefined;
    const offset = c.req.query("offset") ? Number(c.req.query("offset")) : undefined;
    const res = await deps.asyncTasks.listTasks({
      ...(billingAccountId ? { billing_account_id: billingAccountId } : {}),
      ...(status
        ? {
            status: parseBody(
              z.enum(["QUEUED", "PROCESSING", "SUCCEEDED", "FAILED", "CANCELLED"]),
              status,
            ) as AsyncTaskStatus,
          }
        : {}),
      ...(eligibility
        ? {
            refund_eligibility: parseBody(
              z.enum([
                "PENDING",
                "AUTO_REFUNDED",
                "DENIED_USER_CANCELLED",
                "DENIED_POLICY_OR_CLIENT",
                "DENIED_UPSTREAM_BILLED",
                "DENIED_ABUSE_LIMIT",
                "MANUAL_REVIEW",
                "NOT_APPLICABLE",
              ]),
              eligibility,
            ) as AsyncTaskRefundEligibility,
          }
        : {}),
      ...(limit !== undefined && Number.isFinite(limit) ? { limit } : {}),
      ...(offset !== undefined && Number.isFinite(offset) ? { offset } : {}),
    });
    return c.json(res);
  });

  route("GET", "/internal/v1/funding-operations", "auditor", async (c) => {
    const side = c.req.query("side");
    const entityId = c.req.query("entity_id");
    const billingAccountId = billingAccountFilter(
      c,
      "side",
      "entity_id",
      "account_id",
      "kind",
      "currency",
      "from",
      "to",
      "limit",
      "offset",
    );
    const accountId = c.req.query("account_id");
    const kind = c.req.query("kind");
    const currency = c.req.query("currency");
    const from = c.req.query("from");
    const to = c.req.query("to");
    const limit = c.req.query("limit") ? Number(c.req.query("limit")) : undefined;
    const offset = c.req.query("offset") ? Number(c.req.query("offset")) : undefined;
    const res = await deps.queries.listFundingOperations({
      ...(side ? { side: parseBody(z.enum(["CUSTOMER", "SUPPLIER"]), side) } : {}),
      ...(entityId ? { entity_id: parseUuid(entityId, "entity_id") } : {}),
      ...(billingAccountId ? { billing_account_id: billingAccountId } : {}),
      ...(accountId ? { account_id: parseUuid(accountId, "account_id") } : {}),
      ...(kind
        ? {
            kind: parseBody(
              z.enum(["CREDIT", "REFUND", "ADJUST", "SUPPLIER_RECHARGE", "SUPPLIER_ADJUST"]),
              kind,
            ),
          }
        : {}),
      ...(currency ? { currency: parseBody(currencySchema, currency) } : {}),
      ...(from ? { from } : {}),
      ...(to ? { to } : {}),
      ...(limit !== undefined && Number.isFinite(limit) ? { limit } : {}),
      ...(offset !== undefined && Number.isFinite(offset) ? { offset } : {}),
    });
    return c.json(res);
  });

  route("GET", "/internal/v1/finance/summary", "auditor", async (c) => {
    const billingAccountId = billingAccountFilter(c, "currency", "from", "to");
    const currency = c.req.query("currency");
    const from = c.req.query("from");
    const to = c.req.query("to");
    const res = await deps.queries.getFinanceSummary({
      ...(billingAccountId ? { billing_account_id: billingAccountId } : {}),
      ...(currency ? { currency: parseBody(currencySchema, currency) } : {}),
      ...(from ? { from } : {}),
      ...(to ? { to } : {}),
    });
    return c.json(res);
  });

  route("GET", "/internal/v1/ledger-entries", "auditor", async (c) => {
    assertKnownQueryParameters(c.req.query(), [
      "side",
      "entity_id",
      "request_id",
      "kind",
      "from",
      "to",
      "limit",
      "offset",
    ]);
    const side = c.req.query("side");
    const entityId = c.req.query("entity_id");
    const requestId = c.req.query("request_id");
    const kind = c.req.query("kind");
    const from = c.req.query("from");
    const to = c.req.query("to");
    const limit = c.req.query("limit") ? Number(c.req.query("limit")) : undefined;
    const offset = c.req.query("offset") ? Number(c.req.query("offset")) : undefined;
    const res = await deps.queries.listLedgerEntries({
      ...(side ? { side: parseBody(z.enum(["CUSTOMER", "SUPPLIER"]), side) } : {}),
      ...(entityId ? { entity_id: parseUuid(entityId, "entity_id") } : {}),
      ...(requestId ? { request_id: parseUuid(requestId, "request_id") } : {}),
      ...(kind ? { kind } : {}),
      ...(from ? { from } : {}),
      ...(to ? { to } : {}),
      ...(limit !== undefined && Number.isFinite(limit) ? { limit } : {}),
      ...(offset !== undefined && Number.isFinite(offset) ? { offset } : {}),
    });
    return c.json(res);
  });

  route("GET", "/internal/v1/holds", "auditor", async (c) => {
    const state = c.req.query("state");
    const billingAccountId = billingAccountFilter(c, "state", "limit", "offset");
    const limit = c.req.query("limit") ? Number(c.req.query("limit")) : undefined;
    const offset = c.req.query("offset") ? Number(c.req.query("offset")) : undefined;
    const res = await deps.queries.listHolds({
      ...(state
        ? {
            state: parseBody(z.enum(["HELD", "SETTLED", "RELEASED", "UNKNOWN", "CONFLICT"]), state),
          }
        : {}),
      ...(billingAccountId ? { billing_account_id: billingAccountId } : {}),
      ...(limit !== undefined && Number.isFinite(limit) ? { limit } : {}),
      ...(offset !== undefined && Number.isFinite(offset) ? { offset } : {}),
    });
    return c.json(res);
  });

  route("GET", "/internal/v1/requests/:requestId", "auditor", async (c) => {
    const requestId = parseUuid(c.req.param("requestId"), "requestId");
    const res = await deps.queries.getRequestBilling(requestId);
    if (!res.hold && !res.charge) {
      return c.json({ error: { code: "not_found", message: "Request billing not found" } }, 404);
    }
    return c.json(res);
  });

  jsonRoute("POST", "/internal/v1/events/claim", "events", claimEventsSchema, async (c, body) => {
    requireConsumer(c, body.consumer);
    const res = await deps.ledger.claimOutboxEvents(body.consumer, body.limit);
    return c.json(res);
  });

  jsonRoute("POST", "/internal/v1/events/ack", "events", ackEventsSchema, async (c, body) => {
    requireConsumer(c, body.consumer);
    const ok = await deps.ledger.ackOutboxEvents(body.consumer, body.lease_token, body.through_seq);
    return c.json({ ok });
  });

  route("GET", "/internal/v1/audit/conservation", "auditor", async (c) => {
    const audit = await deps.ledger.auditConservation();
    return c.json(audit, audit.ok ? 200 : 500);
  });
}
