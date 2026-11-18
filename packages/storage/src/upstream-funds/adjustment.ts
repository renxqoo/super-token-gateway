import type { SQL } from "bun";
import {
  GatewayError,
  upstreamAdjustmentSchema,
  type UpstreamAdjustmentInput,
  type UpstreamAdjustmentReceipt,
} from "@gateway/contracts";
import { fundingActor, fundingFailure, fundingId, fundingInput } from "./validation.ts";
import { lockVoucherReference } from "./vouchers.ts";
import { withDatabaseSavepoint } from "../transaction.ts";

const maxAtoms = 99999999999999999999999999999999999999n;

function conflict(): never {
  throw new GatewayError(
    409,
    "upstream_adjustment_conflict",
    "Adjustment reference has a different business payload",
  );
}

export async function recordUpstreamAdjustment(
  tx: SQL,
  accountId: string,
  input: UpstreamAdjustmentInput,
  actor: string,
): Promise<UpstreamAdjustmentReceipt> {
  const account = fundingId(accountId);
  const parsed = fundingInput(upstreamAdjustmentSchema, input);
  const trustedActor = fundingActor(actor);
  const payload = { account_id: account, kind: "ADJUSTMENT" as const, ...parsed };
  const digest = new Bun.CryptoHasher("sha256").update(JSON.stringify(payload)).digest("hex");
  try {
    return await withDatabaseSavepoint(tx, async (tx) => {
      const accounts =
        await tx`SELECT id,state FROM upstream_accounts WHERE id=${account} FOR NO KEY UPDATE NOWAIT`;
      if (!accounts.length)
        throw new GatewayError(404, "upstream_account_not_found", "Upstream account not found");
      const prior = (
        await tx`SELECT payload_digest,receipt FROM upstream_funding_operations WHERE account_id=${account} AND kind='ADJUSTMENT' AND external_ref=${parsed.external_ref}`
      )[0];
      if (prior) {
        if (prior.payload_digest !== digest) conflict();
        return prior.receipt as UpstreamAdjustmentReceipt;
      }
      const balance = (
        await tx`SELECT balance_atoms::text,reserved_atoms::text,adjustment_atoms::text,revision::text FROM core_upstream_balances_v1 WHERE account_id=${account} AND currency=${parsed.currency}`
      )[0] ?? { balance_atoms: "0", reserved_atoms: "0", adjustment_atoms: "0", revision: "0" };
      if (balance.revision !== parsed.expected_revision)
        throw new GatewayError(
          409,
          "upstream_revision_conflict",
          "Upstream balance changed since the expected revision",
        );
      if (parsed.voucher_id && parsed.voucher_digest)
        await lockVoucherReference(tx, { id: parsed.voucher_id, digest: parsed.voucher_digest });
      const delta = BigInt(parsed.delta_atoms);
      const amount = BigInt(balance.balance_atoms) + delta,
        adjusted = BigInt(balance.adjustment_atoms) + delta,
        revision = BigInt(balance.revision) + 1n;
      for (const value of [amount, adjusted, revision])
        if (value > maxAtoms || value < -maxAtoms)
          throw new GatewayError(
            409,
            "upstream_amount_overflow",
            "Upstream amount exceeds storage precision",
          );
      const state = String(accounts[0].state);
      const freezes = state === "ACTIVE" && amount < BigInt(balance.reserved_atoms);
      const operation = crypto.randomUUID(),
        journal = crypto.randomUUID();
      const receipt: UpstreamAdjustmentReceipt = {
        operation_id: operation,
        journal_id: journal,
        ...payload,
        actor: trustedActor,
        payload_digest: digest,
        balance_atoms: amount.toString(),
        balance_revision: revision.toString(),
        account_state: freezes ? "RECONCILE_REQUIRED" : state,
      };
      await tx`INSERT INTO upstream_funding_operations(id,account_id,kind,external_ref,payload_digest,credited_currency,credited_atoms,voucher_id,voucher_digest,completed_at,actor,reason,receipt) VALUES(${operation},${account},'ADJUSTMENT',${parsed.external_ref},${digest},${parsed.currency},${parsed.delta_atoms},${parsed.voucher_id ?? null},${parsed.voucher_digest ?? null},clock_timestamp(),${trustedActor},${parsed.reason},${receipt}::jsonb)`;
      if (freezes)
        await tx`UPDATE upstream_accounts SET state='RECONCILE_REQUIRED',revision=revision+1 WHERE id=${account}`;
      await tx`SELECT core_upstream_fund_v1(${account}::uuid,${parsed.currency},${parsed.delta_atoms}::numeric,true)`;
      await tx`INSERT INTO upstream_journals(id,account_id,operation_id,currency,kind,reference) VALUES(${journal},${account},${operation},${parsed.currency},'ADJUSTMENT',${`adjustment:${operation}`})`;
      await tx`INSERT INTO upstream_entries(journal_id,ordinal,currency,account,atoms) VALUES(${journal},0,${parsed.currency},'upstream:available',${parsed.delta_atoms}),(${journal},1,${parsed.currency},'external:adjustment',${(-delta).toString()})`;
      await tx`INSERT INTO upstream_audit_events(id,account_id,actor,action,operation_id,revision,new_digest,reason) VALUES(${crypto.randomUUID()},${account},${trustedActor},'ADJUSTMENT',${operation},${receipt.balance_revision},${digest},${parsed.reason})`;
      await tx`SET CONSTRAINTS balanced_upstream_journal,balanced_upstream_entries IMMEDIATE`;
      await tx`SET CONSTRAINTS balanced_upstream_journal,balanced_upstream_entries DEFERRED`;
      return receipt;
    });
  } catch (error) {
    fundingFailure(error);
  }
}
