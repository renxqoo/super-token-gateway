import { platformCreditDefaults, billingAccounts, organizations } from "../db/schema.ts";
import { eq, sql, asc, inArray } from "drizzle-orm";
import { AMOUNT_DECIMALS } from "@gateway/kernel";
import {
  safeFormatAtoms,
  parseJsonBody,
  resolveAtomsAmount,
  type ConsoleDeps,
  type ConsoleEnv,
} from "./shared.ts";
import {
  defaultCreditPolicySchema,
  batchCreditPolicySchema,
} from "./admin-wallet-policy-schemas.ts";
import { recordAuditLog } from "../auth.ts";
import { GatewayError } from "@gateway/contracts";
import { Hono } from "hono";

export function registerAdminCreditPolicyRoutes({
  app,
  deps,
}: {
  app: Hono<ConsoleEnv>;
  deps: ConsoleDeps;
}) {
  app.get("/finance/credit-policy/default", async (c) => {
    const [row] = await deps.orm
      .select()
      .from(platformCreditDefaults)
      .where(eq(platformCreditDefaults.id, 1))
      .limit(1);
    const limitAtoms = row?.defaultCreditLimitAtoms ?? "0";
    const policy = row?.defaultWalletPolicy ?? "GIFT_FIRST";
    return c.json({
      scale: AMOUNT_DECIMALS,
      default_credit_limit_atoms: limitAtoms,
      default_credit_limit_decimal: safeFormatAtoms(limitAtoms),
      default_wallet_policy: policy,
      updated_by: row?.updatedBy ?? null,
      updated_at: row?.updatedAt ? row.updatedAt.toISOString() : null,
    });
  });

  app.put("/finance/credit-policy/default", async (c) => {
    const actor = c.get("user");
    const body = await parseJsonBody(c, defaultCreditPolicySchema);
    const [existing] = await deps.orm
      .select()
      .from(platformCreditDefaults)
      .where(eq(platformCreditDefaults.id, 1))
      .limit(1);
    const hasLimit =
      body.default_credit_limit_atoms !== undefined ||
      body.default_credit_limit_decimal !== undefined;
    const nextLimitAtoms = hasLimit
      ? resolveAtomsAmount(body.default_credit_limit_atoms, body.default_credit_limit_decimal, {
          field: "default_credit_limit",
          allowNegative: false,
          allowZero: true,
        })
      : (existing?.defaultCreditLimitAtoms ?? "0");
    const nextPolicy = body.default_wallet_policy ?? existing?.defaultWalletPolicy ?? "GIFT_FIRST";

    const [saved] = await deps.orm
      .insert(platformCreditDefaults)
      .values({
        id: 1,
        defaultCreditLimitAtoms: nextLimitAtoms,
        defaultWalletPolicy: nextPolicy,
        updatedBy: actor.id,
      })
      .onConflictDoUpdate({
        target: platformCreditDefaults.id,
        set: {
          defaultCreditLimitAtoms: nextLimitAtoms,
          defaultWalletPolicy: nextPolicy,
          updatedBy: actor.id,
          updatedAt: sql`now()`,
        },
      })
      .returning();

    await recordAuditLog(deps.orm, {
      actor,
      action: "admin.finance.default_credit_policy",
      targetType: "platform_credit_defaults",
      targetId: "1",
      metadata: {
        default_credit_limit_atoms: nextLimitAtoms,
        default_wallet_policy: nextPolicy,
      },
    });
    return c.json({
      scale: AMOUNT_DECIMALS,
      default_credit_limit_atoms: saved!.defaultCreditLimitAtoms,
      default_credit_limit_decimal: safeFormatAtoms(saved!.defaultCreditLimitAtoms),
      default_wallet_policy: saved!.defaultWalletPolicy,
      updated_by: saved!.updatedBy,
      updated_at: saved!.updatedAt.toISOString(),
    });
  });

  app.post("/finance/credit-policy/batch", async (c) => {
    const actor = c.get("user");
    const body = await parseJsonBody(c, batchCreditPolicySchema);
    const creditLimitAtoms = resolveAtomsAmount(
      body.credit_limit_atoms,
      body.credit_limit_decimal,
      {
        field: "credit_limit",
        allowNegative: false,
        allowZero: true,
      },
    );
    const nextLimitBig = BigInt(creditLimitAtoms);

    const maxBatchSize = 500;
    let targets: (typeof billingAccounts.$inferSelect)[] = [];
    if (body.scope === "ALL_BILLING_ACCOUNTS") {
      targets = await deps.orm
        .select()
        .from(billingAccounts)
        .orderBy(asc(billingAccounts.createdAt), asc(billingAccounts.id))
        .limit(maxBatchSize + 1);
    } else if (body.scope === "ORGANIZATION") {
      if (!body.org_id) {
        throw new GatewayError(
          400,
          "validation_error",
          "org_id is required for ORGANIZATION scope",
        );
      }
      targets = await deps.orm
        .select()
        .from(billingAccounts)
        .where(eq(billingAccounts.orgId, body.org_id))
        .orderBy(asc(billingAccounts.createdAt), asc(billingAccounts.id))
        .limit(maxBatchSize + 1);
    } else if (body.scope === "USER") {
      if (!body.user_id) {
        throw new GatewayError(400, "validation_error", "user_id is required for USER scope");
      }
      const ownedOrgs = await deps.orm
        .select({ id: organizations.id, kind: organizations.kind })
        .from(organizations)
        .where(eq(organizations.ownerUserId, body.user_id));
      const orgIds = ownedOrgs.map((o) => o.id);
      if (orgIds.length > 0) {
        const personalOrg = body.only_default_billing_account
          ? ownedOrgs.find((o) => o.kind === "PERSONAL")
          : undefined;
        if (personalOrg) {
          targets = await deps.orm
            .select()
            .from(billingAccounts)
            .where(eq(billingAccounts.orgId, personalOrg.id))
            .orderBy(asc(billingAccounts.createdAt), asc(billingAccounts.id))
            .limit(1);
        }
        if (targets.length === 0) {
          targets = await deps.orm
            .select()
            .from(billingAccounts)
            .where(inArray(billingAccounts.orgId, orgIds))
            .orderBy(asc(billingAccounts.createdAt), asc(billingAccounts.id))
            .limit(body.only_default_billing_account ? 1 : maxBatchSize + 1);
        }
      }
    } else if (body.scope === "BILLING_ACCOUNTS") {
      if (!body.billing_account_ids || body.billing_account_ids.length === 0) {
        throw new GatewayError(
          400,
          "validation_error",
          "billing_account_ids is required for BILLING_ACCOUNTS scope",
        );
      }
      targets = await deps.orm
        .select()
        .from(billingAccounts)
        .where(inArray(billingAccounts.id, body.billing_account_ids))
        .orderBy(asc(billingAccounts.createdAt), asc(billingAccounts.id));
    }

    if (targets.length > maxBatchSize) {
      throw new GatewayError(
        400,
        "validation_error",
        "Credit policy batch matches more than 500 billing accounts; select a smaller scope",
      );
    }

    const items: Record<string, unknown>[] = [];
    let updatedCount = 0;
    let failedCount = 0;

    for (const target of targets) {
      try {
        if (body.dry_run) {
          let currentLimit = "0";
          let currentDebt = "0";
          let currentPolicy = "GIFT_FIRST";
          if (target.gatewaySynced) {
            const w = await deps.gateway.getWallet(target.gatewayBillingAccountId, target.currency);
            currentLimit = String(w.credit_limit_atoms);
            currentDebt = String(w.debt_atoms);
            currentPolicy = String(w.policy);
          }
          items.push({
            billing_account_id: target.id,
            org_id: target.orgId,
            name: target.name,
            currency: target.currency,
            current_credit_limit_atoms: currentLimit,
            current_credit_limit_decimal: safeFormatAtoms(currentLimit),
            next_credit_limit_atoms: creditLimitAtoms,
            next_credit_limit_decimal: safeFormatAtoms(creditLimitAtoms),
            current_debt_atoms: currentDebt,
            current_debt_decimal: safeFormatAtoms(currentDebt),
            current_policy: currentPolicy,
            next_policy: body.policy ?? currentPolicy,
            will_freeze_due_to_debt: BigInt(currentDebt) > nextLimitBig,
          });
          continue;
        }

        const syncedAccount = await deps.sync.admission.ensureBillingAccountSynced(target.id);
        let applied: Record<string, unknown> | null = null;
        for (let attempt = 0; attempt < 2; attempt++) {
          const w = await deps.gateway.getWallet(
            syncedAccount.gatewayBillingAccountId,
            syncedAccount.currency,
          );
          try {
            applied = await deps.gateway.configureCreditPolicy(
              syncedAccount.gatewayBillingAccountId,
              {
                currency: syncedAccount.currency,
                expected_version: String(w.credit_policy_version ?? "0"),
                credit_limit_atoms: creditLimitAtoms,
                ...(body.policy ? { policy: body.policy } : {}),
              },
            );
            break;
          } catch (err) {
            if (attempt === 0 && err instanceof GatewayError && err.status === 400) {
              continue;
            }
            throw err;
          }
        }
        updatedCount++;
        items.push({
          billing_account_id: syncedAccount.id,
          org_id: syncedAccount.orgId,
          name: syncedAccount.name,
          currency: syncedAccount.currency,
          ok: true,
          credit_limit_atoms: String(applied?.credit_limit_atoms ?? creditLimitAtoms),
          credit_limit_decimal: safeFormatAtoms(applied?.credit_limit_atoms ?? creditLimitAtoms),
          debt_atoms: String(applied?.debt_atoms ?? "0"),
          debt_decimal: safeFormatAtoms(applied?.debt_atoms ?? "0"),
          freeze_reason: applied?.freeze_reason ?? null,
        });
      } catch (err) {
        failedCount++;
        items.push({
          billing_account_id: target.id,
          org_id: target.orgId,
          name: target.name,
          currency: target.currency,
          ok: false,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }

    if (!body.dry_run) {
      await recordAuditLog(deps.orm, {
        actor,
        action: "admin.finance.credit_policy_batch",
        targetType: body.scope.toLowerCase(),
        targetId: body.org_id ?? body.user_id ?? "batch",
        metadata: {
          scope: body.scope,
          matched_count: targets.length,
          updated_count: updatedCount,
          failed_count: failedCount,
          credit_limit_atoms: creditLimitAtoms,
          policy: body.policy ?? null,
          reason: body.reason,
        },
      });
    }

    return c.json({
      dry_run: body.dry_run,
      scale: AMOUNT_DECIMALS,
      scope: body.scope,
      matched_count: targets.length,
      updated_count: updatedCount,
      failed_count: failedCount,
      items,
    });
  });
}
