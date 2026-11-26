import { and, asc, eq, gt, inArray, isNotNull, lt, or } from "drizzle-orm";
import {
  apiKeyRecords,
  channels,
  modelCatalog,
  organizations,
  pricingOverrides,
  pricingTemplates,
  billingAccounts,
  routes,
  upstreamAccounts,
  type ConsoleDrizzle,
} from "./db/schema.ts";
import type { ConsoleCatalogSync } from "./sync-catalog.ts";
import type { ConsoleAdmissionSync } from "./sync-admission.ts";
import type { SQL } from "bun";

const RECONCILE_LEADER_LOCK = 88421002;

export function createConsoleReconciler(
  db: SQL,
  orm: ConsoleDrizzle,
  admission: Pick<
    ConsoleAdmissionSync,
    "syncOrganizationAdmission" | "ensureBillingAccountSynced" | "syncApiKey"
  >,
  catalog: Pick<
    ConsoleCatalogSync,
    | "syncUpstreamAccount"
    | "syncChannel"
    | "syncPricingTemplate"
    | "syncModel"
    | "syncRoute"
    | "syncPricingOverride"
  >,
) {
  async function reconcilePending(
    signal?: AbortSignal,
  ): Promise<{ synced: number; failed: number }> {
    const conn = await db.reserve();
    let leader = false;
    try {
      const [row] = await conn`SELECT pg_try_advisory_lock(${RECONCILE_LEADER_LOCK}) AS locked`;
      leader = Boolean(row?.locked);
      if (!leader) return { synced: 0, failed: 0 };

      let synced = 0;
      let failed = 0;
      const states: ("PENDING" | "FAILED")[] = ["PENDING", "FAILED"];
      const batchLimit = 100;

      const pendingOrganizations = await orm
        .select({ id: organizations.id })
        .from(organizations)
        .where(lt(organizations.inferenceSyncedRevision, organizations.inferenceRevision))
        .orderBy(asc(organizations.updatedAt), asc(organizations.id))
        .limit(batchLimit);
      for (const { id } of pendingOrganizations) {
        if (signal?.aborted) return { synced, failed };
        try {
          await admission.syncOrganizationAdmission(id);
          synced++;
        } catch {
          failed++;
        }
      }

      const accs = await orm
        .select({ id: upstreamAccounts.id })
        .from(upstreamAccounts)
        .where(
          or(
            inArray(upstreamAccounts.syncStatus, states),
            lt(upstreamAccounts.syncedRevision, upstreamAccounts.desiredRevision),
          ),
        )
        .limit(batchLimit);
      for (const a of accs) {
        if (signal?.aborted) return { synced, failed };
        try {
          await catalog.syncUpstreamAccount(a.id);
          synced++;
        } catch {
          failed++;
        }
      }

      const chs = await orm
        .select({ id: channels.id })
        .from(channels)
        .where(
          and(
            gt(channels.syncedRevision, 0),
            eq(channels.secretSyncPending, false),
            or(
              inArray(channels.syncStatus, states),
              lt(channels.syncedRevision, channels.desiredRevision),
            ),
          ),
        )
        .limit(batchLimit);
      for (const c of chs) {
        if (signal?.aborted) return { synced, failed };
        try {
          await catalog.syncChannel(c.id);
          synced++;
        } catch {
          failed++;
        }
      }

      const tpls = await orm
        .select({ id: pricingTemplates.id })
        .from(pricingTemplates)
        .where(
          or(
            inArray(pricingTemplates.syncStatus, states),
            lt(pricingTemplates.syncedRevision, pricingTemplates.desiredRevision),
          ),
        )
        .limit(batchLimit);
      for (const t of tpls) {
        if (signal?.aborted) return { synced, failed };
        try {
          await catalog.syncPricingTemplate(t.id);
          synced++;
        } catch {
          failed++;
        }
      }

      const mods = await orm
        .select({ name: modelCatalog.name })
        .from(modelCatalog)
        .where(
          or(
            inArray(modelCatalog.syncStatus, states),
            lt(modelCatalog.syncedRevision, modelCatalog.desiredRevision),
          ),
        )
        .limit(batchLimit);
      for (const m of mods) {
        if (signal?.aborted) return { synced, failed };
        try {
          await catalog.syncModel(m.name);
          synced++;
        } catch {
          failed++;
        }
      }

      const rts = await orm
        .select({ id: routes.id })
        .from(routes)
        .where(
          or(inArray(routes.syncStatus, states), lt(routes.syncedRevision, routes.desiredRevision)),
        )
        .limit(batchLimit);
      for (const r of rts) {
        if (signal?.aborted) return { synced, failed };
        try {
          await catalog.syncRoute(r.id);
          synced++;
        } catch {
          failed++;
        }
      }

      const pendingBillingAccounts = await orm
        .select({ id: billingAccounts.id })
        .from(billingAccounts)
        .where(
          or(
            eq(billingAccounts.syncStatus, "FAILED"),
            and(
              eq(billingAccounts.gatewaySynced, true),
              or(
                inArray(billingAccounts.syncStatus, states),
                lt(billingAccounts.syncedRevision, billingAccounts.desiredRevision),
              ),
            ),
          ),
        )
        .limit(batchLimit);
      for (const p of pendingBillingAccounts) {
        if (signal?.aborted) return { synced, failed };
        try {
          await admission.ensureBillingAccountSynced(p.id);
          synced++;
        } catch {
          failed++;
        }
      }

      const ovrs = await orm
        .select({ id: pricingOverrides.id })
        .from(pricingOverrides)
        .where(
          or(
            inArray(pricingOverrides.syncStatus, states),
            lt(pricingOverrides.syncedRevision, pricingOverrides.desiredRevision),
          ),
        )
        .limit(batchLimit);
      for (const o of ovrs) {
        if (signal?.aborted) return { synced, failed };
        try {
          await catalog.syncPricingOverride(o.id);
          synced++;
        } catch {
          failed++;
        }
      }

      const keys = await orm
        .select({ id: apiKeyRecords.id })
        .from(apiKeyRecords)
        .where(
          and(
            or(gt(apiKeyRecords.syncedRevision, 0), isNotNull(apiKeyRecords.revokedAt)),
            or(
              inArray(apiKeyRecords.syncStatus, states),
              lt(apiKeyRecords.syncedRevision, apiKeyRecords.desiredRevision),
            ),
          ),
        )
        .limit(batchLimit);
      for (const k of keys) {
        if (signal?.aborted) return { synced, failed };
        try {
          await admission.syncApiKey(k.id);
          synced++;
        } catch {
          failed++;
        }
      }

      if (!signal?.aborted) {
        await db`
          DELETE FROM public.sessions
          WHERE (revoked_at IS NOT NULL AND revoked_at < now() - interval '30 days')
             OR refresh_expires_at < now() - interval '7 days'
        `.catch(() => undefined);
      }

      return { synced, failed };
    } finally {
      if (leader) {
        await conn`SELECT pg_advisory_unlock(${RECONCILE_LEADER_LOCK})`.catch(() => undefined);
      }
      conn.release();
    }
  }

  return reconcilePending;
}
