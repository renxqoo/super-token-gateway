import type { TransactionSQL } from "bun";
import {
  GatewayError,
  upstreamRechargeSchema,
  type UpstreamRechargeInput,
  type UpstreamRechargeReceipt,
} from "@gateway/contracts";
import { fundingActor, fundingFailure, fundingId, fundingInput } from "./validation.ts";

function conflict(): never {
  throw new GatewayError(
    409,
    "upstream_recharge_conflict",
    "Recharge reference has a different business payload",
  );
}
export async function recordUpstreamRecharge(
  tx: TransactionSQL,
  accountId: string,
  input: UpstreamRechargeInput,
  actor: string,
): Promise<UpstreamRechargeReceipt> {
  const account = fundingId(accountId);
  const parsed = fundingInput(upstreamRechargeSchema, input);
  const trustedActor = fundingActor(actor);
  const payload = { account_id: account, kind: "RECHARGE" as const, ...parsed };
  const digest = new Bun.CryptoHasher("sha256").update(JSON.stringify(payload)).digest("hex");
  try {
    return await tx.savepoint(async (tx) => {
      const accounts = await tx`SELECT id FROM upstream_accounts WHERE id=${account} FOR SHARE`;
      if (!accounts.length)
        throw new GatewayError(404, "upstream_account_not_found", "Upstream account not found");
      const prior = (
        await tx`SELECT payload_digest,receipt FROM upstream_funding_operations WHERE account_id=${account} AND kind='RECHARGE' AND external_ref=${parsed.external_ref}`
      )[0];
      if (prior) {
        if (prior.payload_digest !== digest) conflict();
        return prior.receipt as UpstreamRechargeReceipt;
      }
      await tx`INSERT INTO upstream_account_balances(account_id,currency) VALUES(${account},${parsed.credited_currency}) ON CONFLICT DO NOTHING`;
      const balance = (
        await tx`SELECT balance_atoms::text,reserved_atoms::text,spent_atoms::text,revision::text FROM upstream_account_balances WHERE account_id=${account} AND currency=${parsed.credited_currency} FOR NO KEY UPDATE`
      )[0]!;
      const old = (
        await tx`SELECT payload_digest,receipt FROM upstream_funding_operations WHERE account_id=${account} AND kind='RECHARGE' AND external_ref=${parsed.external_ref}`
      )[0];
      if (old) {
        if (old.payload_digest !== digest) conflict();
        return old.receipt as UpstreamRechargeReceipt;
      }
      const voucher = (
        await tx`SELECT digest FROM financial_vouchers WHERE id=${parsed.voucher_id} FOR UPDATE`
      )[0];
      if (!voucher) throw new GatewayError(404, "voucher_not_found", "Financial voucher not found");
      if (voucher.digest !== parsed.voucher_digest)
        throw new GatewayError(
          409,
          "voucher_digest_mismatch",
          "Voucher reference and digest do not match",
        );
      const eligibility = (
        await tx`SELECT expires_at<=clock_timestamp() AS expired,EXISTS(SELECT 1 FROM upstream_funding_operations WHERE voucher_id=v.id) AS referenced FROM financial_vouchers v WHERE id=${parsed.voucher_id}`
      )[0]!;
      if (eligibility.expired && !eligibility.referenced)
        throw new GatewayError(409, "voucher_expired", "Unreferenced voucher staging has expired");
      if (
        !(await tx`SELECT ${parsed.completed_at}::timestamptz<=clock_timestamp() AS completed`)[0]
          .completed
      )
        throw new GatewayError(
          400,
          "recharge_not_completed",
          "Recharge completion time is in the future",
        );
      const amount = BigInt(balance.balance_atoms) + BigInt(parsed.credited_atoms);
      const revision = BigInt(balance.revision) + 1n;
      if (
        amount > 99999999999999999999999999999999999999n ||
        revision > 99999999999999999999999999999999999999n
      )
        throw new GatewayError(
          409,
          "upstream_amount_overflow",
          "Upstream amount exceeds storage precision",
        );
      const operation = crypto.randomUUID(),
        journal = crypto.randomUUID();
      const receipt: UpstreamRechargeReceipt = {
        operation_id: operation,
        journal_id: journal,
        ...payload,
        actor: trustedActor,
        payload_digest: digest,
        balance_atoms: amount.toString(),
        balance_revision: revision.toString(),
      };
      const inserted =
        await tx`INSERT INTO upstream_funding_operations(id,account_id,kind,external_ref,payload_digest,payment_currency,payment_atoms,credited_currency,credited_atoms,voucher_id,voucher_digest,completed_at,actor,reason,receipt) VALUES(${operation},${account},'RECHARGE',${parsed.external_ref},${digest},${parsed.payment_currency},${parsed.payment_atoms},${parsed.credited_currency},${parsed.credited_atoms},${parsed.voucher_id},${parsed.voucher_digest},${parsed.completed_at}::timestamptz,${trustedActor},${parsed.reason},${receipt}::jsonb) ON CONFLICT(account_id,kind,external_ref) DO NOTHING RETURNING id`;
      if (!inserted.length) {
        const existing = (
          await tx`SELECT payload_digest,receipt FROM upstream_funding_operations WHERE account_id=${account} AND kind='RECHARGE' AND external_ref=${parsed.external_ref}`
        )[0];
        if (!existing || existing.payload_digest !== digest) conflict();
        return existing.receipt as UpstreamRechargeReceipt;
      }
      await tx`UPDATE upstream_account_balances SET balance_atoms=${receipt.balance_atoms},revision=${receipt.balance_revision} WHERE account_id=${account} AND currency=${parsed.credited_currency}`;
      await tx`INSERT INTO upstream_journals(id,account_id,operation_id,currency,kind,reference) VALUES(${journal},${account},${operation},${parsed.credited_currency},'RECHARGE',${`recharge:${operation}`})`;
      await tx`INSERT INTO upstream_entries(journal_id,ordinal,currency,account,atoms) VALUES(${journal},0,${parsed.credited_currency},'upstream:available',${parsed.credited_atoms}),(${journal},1,${parsed.credited_currency},'external:funding',${(-BigInt(parsed.credited_atoms)).toString()})`;
      await tx`INSERT INTO upstream_audit_events(id,account_id,actor,action,operation_id,revision,new_digest,reason) VALUES(${crypto.randomUUID()},${account},${trustedActor},'RECHARGE',${operation},${receipt.balance_revision},${digest},${parsed.reason})`;
      await tx`SET CONSTRAINTS balanced_upstream_journal,balanced_upstream_entries IMMEDIATE`;
      await tx`SET CONSTRAINTS balanced_upstream_journal,balanced_upstream_entries DEFERRED`;
      return receipt;
    });
  } catch (error) {
    fundingFailure(error);
  }
}
