import type { TransactionSQL } from "bun";
import {
  GatewayError,
  supplierBrandSchema,
  supplierBrandPatchSchema,
  upstreamAccountSchema,
  upstreamAccountPatchSchema,
} from "@gateway/contracts";
import { fundingActor, fundingFailure, fundingId, fundingInput } from "./validation.ts";

export function createUpstreamAccountStore(tx: TransactionSQL) {
  async function guard<T>(action: () => Promise<T>): Promise<T> {
    try {
      return await action();
    } catch (error) {
      if ((error as { errno?: string }).errno === "23505")
        throw new GatewayError(409, "upstream_identity_conflict", "Name already exists");
      fundingFailure(error);
    }
  }
  async function account(id: string) {
    const row = (
      await tx`SELECT id,brand_id,name,mode,state,revision::text,created_at FROM upstream_accounts WHERE id=${fundingId(id)}`
    )[0];
    if (!row)
      throw new GatewayError(404, "upstream_account_not_found", "Upstream account not found");
    return row as Record<string, unknown>;
  }
  const before = (value: string | undefined) => (value === undefined ? null : fundingId(value));
  return {
    brands: () =>
      guard(
        () =>
          tx`SELECT id,name,enabled,created_at FROM supplier_brands ORDER BY name LIMIT 1000` as Promise<
            unknown[]
          >,
      ),
    createBrand: (input: unknown, actor: string) =>
      guard(async () => {
        const parsed = fundingInput(supplierBrandSchema, input);
        const id = crypto.randomUUID();
        await tx`INSERT INTO supplier_brands(id,name) VALUES(${id},${parsed.name})`;
        await tx`INSERT INTO upstream_audit_events(id,actor,action,reason) VALUES(${crypto.randomUUID()},${fundingActor(actor)},'BRAND_CREATE',${`brand:${id}`})`;
        return { id, name: parsed.name, enabled: true };
      }),
    updateBrand: (id: string, input: unknown, actor: string) =>
      guard(async () => {
        const parsed = fundingInput(supplierBrandPatchSchema, input);
        const rows =
          await tx`UPDATE supplier_brands SET enabled=${parsed.enabled} WHERE id=${fundingId(id)} RETURNING id,name,enabled`;
        if (!rows[0])
          throw new GatewayError(404, "supplier_brand_not_found", "Supplier brand not found");
        await tx`INSERT INTO upstream_audit_events(id,actor,action,reason) VALUES(${crypto.randomUUID()},${fundingActor(actor)},${parsed.enabled ? "BRAND_ENABLE" : "BRAND_DISABLE"},${`brand:${rows[0].id}`})`;
        return rows[0] as Record<string, unknown>;
      }),
    accounts: () =>
      guard(
        () =>
          tx`SELECT id,brand_id,name,mode,state,revision::text,created_at FROM upstream_accounts ORDER BY created_at,id LIMIT 1000` as Promise<
            unknown[]
          >,
      ),
    account: (id: string) => guard(() => account(id)),
    createAccount: (input: unknown, actor: string) =>
      guard(async () => {
        const parsed = fundingInput(upstreamAccountSchema, input);
        const brand = (
          await tx`SELECT id,enabled FROM supplier_brands WHERE id=${parsed.brand_id} FOR SHARE`
        )[0];
        if (!brand?.enabled)
          throw new GatewayError(
            404,
            "supplier_brand_not_found",
            "Enabled supplier brand not found",
          );
        const id = crypto.randomUUID();
        await tx`INSERT INTO upstream_accounts(id,brand_id,name) VALUES(${id},${parsed.brand_id},${parsed.name})`;
        await tx`INSERT INTO upstream_audit_events(id,account_id,actor,action,revision,reason) VALUES(${crypto.randomUUID()},${id},${fundingActor(actor)},'ACCOUNT_CREATE',0,${parsed.name})`;
        return account(id);
      }),
    updateAccount: (id: string, input: unknown, actor: string) =>
      guard(async () => {
        const parsed = fundingInput(upstreamAccountPatchSchema, input);
        const current = (
          await tx`SELECT state FROM upstream_accounts WHERE id=${fundingId(id)} FOR NO KEY UPDATE`
        )[0];
        if (!current)
          throw new GatewayError(404, "upstream_account_not_found", "Upstream account not found");
        if (current.state !== parsed.state) {
          if (current.state !== "ACTIVE" && current.state !== "DISABLED")
            throw new GatewayError(
              409,
              "upstream_account_state_locked",
              "Reconciliation or quarantine must be resolved by its own process",
            );
          await tx`UPDATE upstream_accounts SET state=${parsed.state},revision=revision+1 WHERE id=${fundingId(id)}`;
          await tx`INSERT INTO upstream_audit_events(id,account_id,actor,action,reason) VALUES(${crypto.randomUUID()},${fundingId(id)},${fundingActor(actor)},${parsed.state === "ACTIVE" ? "ACCOUNT_ENABLE" : "ACCOUNT_DISABLE"},${parsed.reason})`;
        }
        return account(id);
      }),
    holds: (id: string, filter: { currency?: string; channel?: string; before?: string }) =>
      guard(async () => {
        await account(id);
        const channel = filter.channel === undefined ? null : fundingId(filter.channel),
          cursor = before(filter.before);
        return tx`SELECT h.attempt_id,h.channel_id,h.currency,h.estimated_atoms::text,h.final_cost_atoms::text,h.state,h.cost_version_id,h.created_at FROM upstream_holds h WHERE h.account_id=${fundingId(id)} AND (${filter.currency ?? null}::text IS NULL OR h.currency=${filter.currency ?? null}) AND (${channel}::uuid IS NULL OR h.channel_id=${channel}) AND (${cursor}::uuid IS NULL OR (h.created_at,h.attempt_id)<(SELECT created_at,attempt_id FROM upstream_holds WHERE attempt_id=${cursor}::uuid AND account_id=${fundingId(id)})) ORDER BY h.created_at DESC,h.attempt_id DESC LIMIT 100` as Promise<
          unknown[]
        >;
      }),
    journal: (id: string, filter: { currency?: string; channel?: string; before?: string }) =>
      guard(async () => {
        await account(id);
        const channel = filter.channel === undefined ? null : fundingId(filter.channel),
          cursor = before(filter.before);
        return tx`SELECT j.id,j.kind,j.currency,j.channel_id,j.attempt_id,j.operation_id,j.reference,j.created_at,(SELECT jsonb_agg(jsonb_build_object('account',e.account,'atoms',e.atoms::text) ORDER BY e.ordinal) FROM upstream_entries e WHERE e.journal_id=j.id) AS entries FROM upstream_journals j WHERE j.account_id=${fundingId(id)} AND (${filter.currency ?? null}::text IS NULL OR j.currency=${filter.currency ?? null}) AND (${channel}::uuid IS NULL OR j.channel_id=${channel}) AND (${cursor}::uuid IS NULL OR (j.created_at,j.id)<(SELECT created_at,id FROM upstream_journals WHERE id=${cursor}::uuid AND account_id=${fundingId(id)})) ORDER BY j.created_at DESC,j.id DESC LIMIT 100` as Promise<
          unknown[]
        >;
      }),
    recharges: (id: string) =>
      guard(async () => {
        await account(id);
        return (
          await tx`SELECT receipt FROM upstream_funding_operations WHERE account_id=${fundingId(id)} AND kind='RECHARGE' ORDER BY created_at DESC,id DESC LIMIT 100`
        ).map((row: { receipt: unknown }) => row.receipt);
      }),
    reconciliations: (id: string) =>
      guard(async () => {
        await account(id);
        return tx`SELECT id,currency,revision::text AS balance_revision,book_atoms::text,observed_atoms::text,invoice_atoms::text,difference_atoms::text,voucher_id,adjustment_id AS adjustment_operation_id,actor,reason,created_at FROM reconciliations WHERE account_id=${fundingId(id)} ORDER BY created_at DESC,id DESC LIMIT 100` as Promise<
          unknown[]
        >;
      }),
  };
}
