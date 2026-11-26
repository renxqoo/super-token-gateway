import { and, eq, sql } from "drizzle-orm";
import { GatewayError } from "@gateway/contracts";
import {
  apiKeyRecords,
  organizations,
  platformCreditDefaults,
  billingAccounts,
  type ConsoleDrizzle,
} from "./db/schema.ts";
import type { GatewayClient } from "./gateway-client.ts";
import { errMsg, isStaleRevisionError, LOCK_NS, type EntityLocker } from "./sync-state.ts";

export function createConsoleAdmissionSync(
  orm: ConsoleDrizzle,
  gateway: GatewayClient,
  withEntityLock: EntityLocker,
) {
  async function ensureBillingAccountSynced(billingAccountId: string) {
    return withEntityLock(LOCK_NS.billingAccount, billingAccountId, async (orm) => {
      const [billingAccount] = await orm
        .select()
        .from(billingAccounts)
        .where(eq(billingAccounts.id, billingAccountId))
        .limit(1);
      if (!billingAccount) throw new GatewayError(404, "not_found", "Billing account not found");
      if (
        billingAccount.gatewaySynced &&
        billingAccount.syncStatus === "SYNCED" &&
        billingAccount.syncedRevision >= billingAccount.desiredRevision
      ) {
        return billingAccount;
      }
      const targetRev = billingAccount.desiredRevision;
      try {
        const res = await gateway.upsertBillingAccount({
          id: billingAccount.gatewayBillingAccountId,
          external_ref: `console:org:${billingAccount.orgId}:billing-account:${billingAccount.id}`,
          name: billingAccount.name,
          currency: billingAccount.currency,
          rpm: billingAccount.rpm,
          tpm: billingAccount.tpm,
          concurrency: billingAccount.concurrency,
          enabled: billingAccount.enabled,
          source_revision: targetRev,
        });
        if (!billingAccount.gatewaySynced) {
          const [defaults] = await orm
            .select()
            .from(platformCreditDefaults)
            .where(eq(platformCreditDefaults.id, 1))
            .limit(1);
          if (
            defaults &&
            (defaults.defaultCreditLimitAtoms !== "0" ||
              defaults.defaultWalletPolicy !== "GIFT_FIRST")
          ) {
            const wallet = await gateway.getWallet(
              billingAccount.gatewayBillingAccountId,
              billingAccount.currency,
            );
            if (wallet.credit_policy_version === "0") {
              await gateway.configureCreditPolicy(billingAccount.gatewayBillingAccountId, {
                currency: billingAccount.currency,
                expected_version: "0",
                credit_limit_atoms: defaults.defaultCreditLimitAtoms,
                policy: defaults.defaultWalletPolicy,
              });
            }
          }
        }
        const acceptedRev = Math.max(targetRev, res.source_revision ?? targetRev);
        const [updated] = await orm
          .update(billingAccounts)
          .set({
            gatewaySynced: true,
            syncedRevision: sql`GREATEST(${billingAccounts.syncedRevision}, ${acceptedRev})`,
            syncStatus: sql`CASE WHEN ${billingAccounts.desiredRevision} <= ${acceptedRev} THEN 'SYNCED' ELSE 'PENDING' END`,
            lastSyncError: sql`CASE WHEN ${billingAccounts.desiredRevision} <= ${acceptedRev} THEN NULL ELSE ${billingAccounts.lastSyncError} END`,
            updatedAt: sql`now()`,
          })
          .where(eq(billingAccounts.id, billingAccount.id))
          .returning();
        return updated ?? billingAccount;
      } catch (err) {
        if (isStaleRevisionError(err)) {
          const [latest] = await orm
            .select()
            .from(billingAccounts)
            .where(eq(billingAccounts.id, billingAccount.id))
            .limit(1);
          return latest ?? billingAccount;
        }
        await orm
          .update(billingAccounts)
          .set({
            syncStatus: sql`CASE WHEN ${billingAccounts.desiredRevision} <= ${targetRev} THEN 'FAILED' ELSE 'PENDING' END`,
            lastSyncError: errMsg(err),
            updatedAt: sql`now()`,
          })
          .where(eq(billingAccounts.id, billingAccount.id));
        throw err;
      }
    });
  }

  async function changeOrganizationStatus(organizationId: string, status: "ACTIVE" | "SUSPENDED") {
    return withEntityLock(LOCK_NS.organization, organizationId.toLowerCase(), async (orm) => {
      const [organization] = await orm
        .update(organizations)
        .set({
          inferenceTargetStatus: status,
          inferenceRevision: sql`CASE WHEN ${organizations.inferenceTargetStatus} <> ${status}
          THEN ${organizations.inferenceRevision} + 1 ELSE ${organizations.inferenceRevision} END`,
          updatedAt: sql`now()`,
        })
        .where(eq(organizations.id, organizationId))
        .returning();
      if (!organization) throw new GatewayError(404, "not_found", "Organization not found");
      return applyOrganizationAdmission(orm, organization).catch(() => organization);
    });
  }

  async function applyOrganizationAdmission(
    orm: ConsoleDrizzle,
    organization: typeof organizations.$inferSelect,
  ) {
    const revision = organization.inferenceRevision;
    if (revision === organization.inferenceSyncedRevision) return organization;
    try {
      const applied = await gateway.setOrganizationAdmission(
        organization.id,
        organization.inferenceTargetStatus === "SUSPENDED",
        revision,
      );
      if (applied?.source_revision !== revision) {
        throw new GatewayError(
          502,
          "gateway_invalid_response",
          "Gateway did not confirm organization admission",
        );
      }
    } catch (err) {
      await orm
        .update(organizations)
        .set({ updatedAt: sql`now()` })
        .where(eq(organizations.id, organization.id));
      throw err;
    }
    const [confirmed] = await orm
      .update(organizations)
      .set({ status: organization.inferenceTargetStatus, inferenceSyncedRevision: revision })
      .where(
        and(eq(organizations.id, organization.id), eq(organizations.inferenceRevision, revision)),
      )
      .returning();
    return confirmed ?? organization;
  }

  async function syncApiKey(keyId: string) {
    const [initial] = await orm
      .select({ billingAccountId: apiKeyRecords.billingAccountId })
      .from(apiKeyRecords)
      .where(eq(apiKeyRecords.id, keyId))
      .limit(1);
    if (!initial) throw new GatewayError(404, "not_found", "API key not found");
    const billingAccount = await ensureBillingAccountSynced(initial.billingAccountId);

    return withEntityLock(LOCK_NS.apiKey, keyId, async (orm) => {
      const [rec] = await orm
        .select()
        .from(apiKeyRecords)
        .where(eq(apiKeyRecords.id, keyId))
        .limit(1);
      if (!rec) throw new GatewayError(404, "not_found", "API key not found");
      const targetRev = rec.desiredRevision;
      const isRevoked = Boolean(rec.revokedAt);
      const expiresAtIso = isRevoked ? null : rec.expiresAt ? rec.expiresAt.toISOString() : null;
      try {
        const gwRes = await gateway.upsertApiKey({
          id: rec.id,
          billing_account_id: billingAccount.gatewayBillingAccountId,
          ...(rec.rotatedFromKeyId && !isRevoked
            ? { rotated_from_key_id: rec.rotatedFromKeyId }
            : {}),
          name: rec.name,
          role: rec.role,
          allowed_cidrs: isRevoked ? [] : rec.allowedCidrs,
          models: rec.models,
          operations: rec.operations,
          enabled: rec.enabled && !isRevoked,
          expires_at: expiresAtIso,
          spend_limit_atoms: rec.spendLimitAtoms,
          spend_limit_window: rec.spendLimitWindow,
          source_revision: targetRev,
        });
        const acceptedRev = Math.max(targetRev, gwRes.source_revision ?? targetRev);
        const [updated] = await orm
          .update(apiKeyRecords)
          .set({
            keyPrefix: gwRes.prefix || rec.keyPrefix,
            syncedRevision: sql`GREATEST(${apiKeyRecords.syncedRevision}, ${acceptedRev})`,
            syncStatus: sql`CASE WHEN ${apiKeyRecords.desiredRevision} <= ${acceptedRev} THEN 'SYNCED' ELSE 'PENDING' END`,
            lastSyncError: sql`CASE WHEN ${apiKeyRecords.desiredRevision} <= ${acceptedRev} THEN NULL ELSE ${apiKeyRecords.lastSyncError} END`,
            updatedAt: sql`now()`,
          })
          .where(eq(apiKeyRecords.id, rec.id))
          .returning();
        return { record: updated ?? rec, key: gwRes.key };
      } catch (err) {
        if (isStaleRevisionError(err)) {
          const [latest] = await orm
            .select()
            .from(apiKeyRecords)
            .where(eq(apiKeyRecords.id, rec.id))
            .limit(1);
          return { record: latest ?? rec, key: undefined };
        }
        await orm
          .update(apiKeyRecords)
          .set({
            syncStatus: sql`CASE WHEN ${apiKeyRecords.desiredRevision} <= ${targetRev} THEN 'FAILED' ELSE 'PENDING' END`,
            lastSyncError: errMsg(err),
            updatedAt: sql`now()`,
          })
          .where(eq(apiKeyRecords.id, rec.id));
        throw err;
      }
    });
  }

  async function syncOrganizationAdmission(id: string) {
    await withEntityLock(LOCK_NS.organization, id, async (orm) => {
      const [organization] = await orm.select().from(organizations).where(eq(organizations.id, id));
      if (organization) await applyOrganizationAdmission(orm, organization);
    });
  }

  return {
    ensureBillingAccountSynced,
    changeOrganizationStatus,
    syncApiKey,
    syncOrganizationAdmission,
  };
}

export type ConsoleAdmissionSync = ReturnType<typeof createConsoleAdmissionSync>;
