import type { SQL } from "bun";
import {
  GatewayError,
  upstreamReconciliationSchema,
  type UpstreamReconciliationInput,
} from "@gateway/contracts";
import { fundingActor, fundingFailure, fundingId, fundingInput } from "./validation.ts";
import { lockVoucherReference } from "./vouchers.ts";
import { withDatabaseSavepoint } from "../transaction.ts";

async function view(tx: SQL, id: string) {
  return (
    await tx`SELECT r.id,r.account_id,r.currency,r.revision::text AS balance_revision,r.book_atoms::text,r.observed_atoms::text,r.invoice_atoms::text,r.difference_atoms::text,r.voucher_id,r.adjustment_id AS adjustment_operation_id,r.actor,r.reason,r.created_at,a.state AS account_state FROM reconciliations r JOIN upstream_accounts a ON a.id=r.account_id WHERE r.id=${id}`
  )[0] as Record<string, unknown>;
}

export async function recordUpstreamReconciliation(
  tx: SQL,
  accountId: string,
  input: UpstreamReconciliationInput,
  actor: string,
) {
  const account = fundingId(accountId);
  const parsed = fundingInput(upstreamReconciliationSchema, input);
  const trustedActor = fundingActor(actor);
  try {
    return await withDatabaseSavepoint(tx, async (tx) => {
      const accounts =
        await tx`SELECT id,state FROM upstream_accounts WHERE id=${account} FOR NO KEY UPDATE NOWAIT`;
      if (!accounts.length)
        throw new GatewayError(404, "upstream_account_not_found", "Upstream account not found");
      const balances = await tx<
        { currency: string; balance_atoms: string; reserved_atoms: string; revision: string }[]
      >`SELECT currency,balance_atoms::text,reserved_atoms::text,revision::text FROM core_upstream_balances_v1 WHERE account_id=${account} ORDER BY currency`;
      const balance = balances.find((row) => row.currency === parsed.currency);
      if (!balance)
        throw new GatewayError(
          404,
          "upstream_balance_not_found",
          "Upstream currency balance not found",
        );
      const voucher = parsed.voucher_id ?? null,
        adjustment = parsed.adjustment_operation_id ?? null;
      const existing = (
        await tx`SELECT id,observed_atoms::text,invoice_atoms::text,voucher_id,adjustment_id,reason FROM reconciliations WHERE account_id=${account} AND currency=${parsed.currency} AND revision=${parsed.expected_revision}`
      )[0];
      if (existing) {
        if (
          existing.observed_atoms !== (parsed.observed_atoms ?? null) ||
          existing.invoice_atoms !== (parsed.invoice_atoms ?? null) ||
          existing.voucher_id !== voucher ||
          existing.adjustment_id !== adjustment ||
          existing.reason !== parsed.reason
        )
          throw new GatewayError(
            409,
            "upstream_reconciliation_conflict",
            "A different reconciliation exists for this balance revision",
          );
        return { ...(await view(tx, String(existing.id))), replayed: true };
      }
      if (balance.revision !== parsed.expected_revision)
        throw new GatewayError(
          409,
          "upstream_revision_conflict",
          "Upstream balance changed since the expected revision",
        );
      if (parsed.voucher_id && parsed.voucher_digest)
        await lockVoucherReference(tx, { id: parsed.voucher_id, digest: parsed.voucher_digest });
      if (
        adjustment &&
        !(
          await tx`SELECT 1 FROM upstream_funding_operations WHERE id=${adjustment} AND account_id=${account} AND kind='ADJUSTMENT' AND credited_currency=${parsed.currency}`
        ).length
      )
        throw new GatewayError(404, "upstream_adjustment_not_found", "Linked adjustment not found");
      const book = BigInt(balance.balance_atoms),
        compared = parsed.invoice_atoms ?? parsed.observed_atoms,
        difference = compared === undefined ? 0n : BigInt(compared) - book;
      if (parsed.resume) {
        if (accounts[0].state !== "RECONCILE_REQUIRED")
          throw new GatewayError(
            409,
            "upstream_account_not_reconcilable",
            "Account is not awaiting reconciliation",
          );
        if (balances.some((row) => BigInt(row.balance_atoms) < BigInt(row.reserved_atoms)))
          throw new GatewayError(
            409,
            "upstream_balance_insufficient",
            "Every currency balance must cover its reservations before resuming",
          );
      }
      const id = crypto.randomUUID();
      await tx`INSERT INTO reconciliations(id,account_id,currency,revision,book_atoms,observed_atoms,invoice_atoms,difference_atoms,voucher_id,adjustment_id,actor,reason) VALUES(${id},${account},${parsed.currency},${parsed.expected_revision},${book.toString()},${parsed.observed_atoms ?? null},${parsed.invoice_atoms ?? null},${difference.toString()},${voucher},${adjustment},${trustedActor},${parsed.reason})`;
      if (parsed.resume) {
        await tx`SELECT core_upstream_normalize_v1(${account}::uuid)`;
        await tx`UPDATE upstream_accounts SET state='ACTIVE',revision=revision+1 WHERE id=${account}`;
      }
      await tx`INSERT INTO upstream_audit_events(id,account_id,actor,action,revision,reason) VALUES(${crypto.randomUUID()},${account},${trustedActor},${parsed.resume ? "RECONCILED_RESUME" : "RECONCILED"},${parsed.expected_revision},${parsed.reason})`;
      return { ...(await view(tx, id)), replayed: false };
    });
  } catch (error) {
    fundingFailure(error);
  }
}
