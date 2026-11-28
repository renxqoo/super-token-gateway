import { DomainError } from "@gateway/kernel";
import type { BillingAccountRecord, SpendLimitWindow, ApiKeyRecord } from "./types.ts";
import { sql, and, eq } from "drizzle-orm";
import { billingAccounts, wallets, apiKeys, type LedgerDrizzle } from "./schema.ts";
import { bumpEpoch } from "./catalog-epoch.ts";
import type { SQL } from "bun";

export function createCatalogAdmissionOperations({ db, orm }: { db: SQL; orm: LedgerDrizzle }) {
  return {
    async setConsoleOrganizationAdmission(
      organizationId: string,
      suspended: boolean,
      revision: number,
    ) {
      const rows = await db`
        INSERT INTO public.console_organization_admission (organization_id, suspended, source_revision)
        VALUES (${organizationId}, ${suspended}, ${revision})
        ON CONFLICT (organization_id) DO UPDATE SET
          suspended = EXCLUDED.suspended,
          source_revision = EXCLUDED.source_revision
        WHERE console_organization_admission.source_revision < EXCLUDED.source_revision
           OR (console_organization_admission.source_revision = EXCLUDED.source_revision
               AND console_organization_admission.suspended = EXCLUDED.suspended)
        RETURNING source_revision
      `;
      if (!rows[0])
        throw new DomainError(
          "stale_revision",
          "Organization admission revision conflicts with Gateway",
        );
      return { source_revision: revision };
    },

    async getConsoleAdmission(
      organizationId: string,
      externalRef: string,
      billingAccountId: string,
      key: Pick<
        ApiKeyRecord,
        | "id"
        | "verifier"
        | "role"
        | "allowed_cidrs"
        | "models"
        | "operations"
        | "spend_limit_atoms"
        | "spend_limit_window"
      >,
    ) {
      const [row] = await db<
        {
          billing_account_enabled: boolean;
          key_enabled: boolean;
          key_policy_matches: boolean;
          suspended: boolean;
        }[]
      >`SELECT p.enabled AS billing_account_enabled,
          k.enabled AND (k.expires_at IS NULL OR k.expires_at > now()) AS key_enabled,
          (k.verifier = ${key.verifier} AND k.role = ${key.role}
            AND k.allowed_cidrs = ${key.allowed_cidrs}::text[]
            AND k.models = ${key.models}::text[]
            AND k.operations = ${key.operations}::text[]
            AND k.spend_limit_atoms IS NOT DISTINCT FROM ${key.spend_limit_atoms}::numeric
            AND k.spend_limit_window = ${key.spend_limit_window}) AS key_policy_matches,
          coalesce(o.suspended, false) AS suspended
        FROM public.billing_accounts p
        JOIN public.api_keys k ON k.billing_account_id = p.id AND k.id = ${key.id}
        LEFT JOIN public.console_organization_admission o ON o.organization_id = ${organizationId}
        WHERE p.id = ${billingAccountId} AND p.external_ref = ${externalRef}`;
      return row;
    },

    async upsertBillingAccount(input: {
      id?: string;
      external_ref?: string | null;
      customer_group_id?: string | null;
      name: string;
      currency?: string;
      rpm: number;
      tpm: number;
      concurrency: number;
      enabled?: boolean;
      source_revision?: number;
    }): Promise<BillingAccountRecord & { config_epoch: number; source_revision: number }> {
      const id = input.id ?? crypto.randomUUID();
      const currency = input.currency ?? "USD";
      const externalRef = input.external_ref ?? null;
      const customerGroupId = input.customer_group_id ?? null;
      const enabled = input.enabled ?? true;
      const sourceRev = input.source_revision;
      const bindingChangeAllowed = sql`(${billingAccounts.externalRef} IS NULL
        OR ${billingAccounts.externalRef} NOT LIKE 'console:org:%'
        OR ${billingAccounts.externalRef} IS NOT DISTINCT FROM ${externalRef})`;
      const canUpdate = and(
        sourceRev !== undefined
          ? sql`${billingAccounts.sourceRevision} <= ${sourceRev}`
          : undefined,
        input.external_ref !== undefined ? bindingChangeAllowed : undefined,
      );
      return orm.transaction(async (tx) => {
        const rows = await tx
          .insert(billingAccounts)
          .values({
            id,
            externalRef,
            customerGroupId,
            name: input.name,
            currency,
            rpm: input.rpm,
            tpm: input.tpm,
            concurrency: input.concurrency,
            enabled,
            ...(sourceRev !== undefined ? { sourceRevision: sourceRev } : {}),
          })
          .onConflictDoUpdate({
            target: billingAccounts.id,
            ...(canUpdate ? { setWhere: canUpdate } : {}),
            set: {
              externalRef:
                input.external_ref !== undefined
                  ? externalRef
                  : sql`CASE WHEN ${billingAccounts.externalRef} LIKE 'console:org:%' THEN ${billingAccounts.externalRef} ELSE NULL END`,
              ...(input.customer_group_id !== undefined ? { customerGroupId } : {}),
              name: input.name,
              currency,
              rpm: input.rpm,
              tpm: input.tpm,
              concurrency: input.concurrency,
              enabled,
              ...(sourceRev !== undefined ? { sourceRevision: sourceRev } : {}),
              updatedAt: sql`now()`,
            },
          })
          .returning();
        if (!rows[0]) {
          const [existing] = await tx
            .select({ externalRef: billingAccounts.externalRef })
            .from(billingAccounts)
            .where(eq(billingAccounts.id, id));
          if (
            input.external_ref !== undefined &&
            existing?.externalRef?.startsWith("console:org:") &&
            existing.externalRef !== externalRef
          ) {
            throw new DomainError(
              "console_billing_account_binding_conflict",
              "Console billing account organization binding is immutable",
            );
          }
          throw new DomainError(
            "stale_revision",
            "Incoming billing account revision is older than current Gateway revision",
          );
        }
        await tx
          .insert(wallets)
          .values({
            id: crypto.randomUUID(),
            billingAccountId: id,
            currency,
          })
          .onConflictDoNothing({ target: [wallets.billingAccountId, wallets.currency] });
        const epoch = await bumpEpoch(tx);
        const r = rows[0]!;
        return {
          id: r.id,
          external_ref: r.externalRef,
          ...(r.customerGroupId !== null ? { customer_group_id: r.customerGroupId } : {}),
          name: r.name,
          currency: r.currency,
          rpm: r.rpm,
          tpm: r.tpm,
          concurrency: r.concurrency,
          enabled: r.enabled,
          source_revision: r.sourceRevision,
          config_epoch: epoch,
        };
      });
    },

    async upsertApiKey(input: {
      id?: string;
      billing_account_id: string;
      rotated_from_key_id?: string;
      name: string;
      verifier: string;
      prefix: string;
      role?: "ADMIN" | "BILLING" | "INVOKER" | "VIEWER";
      allowed_cidrs?: string[];
      models?: string[];
      operations?: string[];
      enabled?: boolean;
      expires_at?: string | null;
      spend_limit_atoms?: string | null;
      spend_limit_window?: SpendLimitWindow;
      source_revision?: number;
    }): Promise<
      ApiKeyRecord & { config_epoch: number; created: boolean; source_revision: number }
    > {
      const id = input.id?.toLowerCase() ?? crypto.randomUUID();
      const billingAccountId = input.billing_account_id.toLowerCase();
      const role = input.role ?? "INVOKER";
      const allowedCidrs = input.allowed_cidrs ?? [];
      const keyModels = input.models ?? [];
      const keyOperations = input.operations ?? [];
      const enabled = input.enabled ?? true;
      const expiry = input.expires_at ?? null;
      const expiresAt = expiry === null ? null : new Date(expiry);
      const spendLimitAtoms = input.spend_limit_atoms ?? null;
      const spendLimitWindow = input.spend_limit_window ?? "LIFETIME";
      const sourceRev = input.source_revision;
      return orm.transaction(async (tx) => {
        const [existed] = await tx
          .select({
            id: apiKeys.id,
            billingAccountId: apiKeys.billingAccountId,
            scope: apiKeys.resourceScopeId,
            sourceRevision: apiKeys.sourceRevision,
          })
          .from(apiKeys)
          .where(eq(apiKeys.id, id))
          .for("update");
        if (existed && sourceRev !== undefined && existed.sourceRevision > sourceRev) {
          throw new DomainError(
            "stale_revision",
            "Incoming API key revision is older than current Gateway revision",
          );
        }
        if (existed && existed.billingAccountId !== billingAccountId) {
          throw new DomainError("api_key_scope_conflict", "API key billing account is immutable");
        }
        let resourceScopeId = existed?.scope ?? null;
        if (input.rotated_from_key_id) {
          const [parent] = await tx
            .select({
              id: apiKeys.id,
              scope: apiKeys.resourceScopeId,
              billingAccountId: apiKeys.billingAccountId,
            })
            .from(apiKeys)
            .where(eq(apiKeys.id, input.rotated_from_key_id))
            .for("share");
          const parentScope = parent?.scope ?? parent?.id;
          if (
            !parent ||
            parent.billingAccountId !== billingAccountId ||
            parent.id === id ||
            parentScope === id ||
            (existed && resourceScopeId !== parentScope)
          ) {
            throw new DomainError("api_key_scope_conflict", "Invalid API key rotation scope");
          }
          resourceScopeId = parentScope!;
        }
        const sameScope =
          resourceScopeId === null ? undefined : eq(apiKeys.resourceScopeId, resourceScopeId);
        const canUpdate = and(
          eq(apiKeys.billingAccountId, billingAccountId),
          sameScope,
          sourceRev === undefined ? undefined : sql`${apiKeys.sourceRevision} <= ${sourceRev}`,
        );
        const rows = await tx
          .insert(apiKeys)
          .values({
            id,
            billingAccountId,
            resourceScopeId,
            name: input.name,
            verifier: input.verifier,
            prefix: input.prefix,
            role,
            allowedCidrs,
            models: keyModels,
            operations: keyOperations,
            enabled,
            expiresAt,
            spendLimitAtoms,
            spendLimitWindow,
            ...(sourceRev !== undefined ? { sourceRevision: sourceRev } : {}),
          })
          .onConflictDoUpdate({
            target: apiKeys.id,
            setWhere: canUpdate!,
            set: {
              name: input.name,
              role,
              allowedCidrs,
              models: keyModels,
              operations: keyOperations,
              enabled,
              expiresAt,
              spendLimitAtoms,
              spendLimitWindow,
              ...(sourceRev !== undefined ? { sourceRevision: sourceRev } : {}),
            },
          })
          .returning();
        if (!rows[0]) {
          throw new DomainError(
            "stale_revision",
            "Incoming API key revision is older than current Gateway revision",
          );
        }
        const epoch = await bumpEpoch(tx);
        const r = rows[0]!;
        return {
          id: r.id,
          billing_account_id: r.billingAccountId,
          name: r.name,
          verifier: r.verifier,
          prefix: r.prefix,
          role: r.role,
          allowed_cidrs: r.allowedCidrs,
          models: r.models,
          operations: r.operations,
          enabled: r.enabled,
          expires_at: r.expiresAt?.toISOString() ?? null,
          spend_limit_atoms: r.spendLimitAtoms,
          spend_limit_window: r.spendLimitWindow,
          source_revision: r.sourceRevision,
          config_epoch: epoch,
          created: !existed,
        };
      });
    },
  };
}
