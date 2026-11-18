import type { SQL } from "bun";
import {
  upstreamRechargeSchema,
  type UpstreamRechargeInput,
  type UpstreamRechargeReceipt,
} from "@gateway/contracts";

export async function seedHistoricalRecharge(
  db: SQL,
  account: string,
  input: UpstreamRechargeInput,
  actor: string,
): Promise<UpstreamRechargeReceipt> {
  const parsed = upstreamRechargeSchema.parse(input);
  const payload = { account_id: account, kind: "RECHARGE" as const, ...parsed };
  const digest = new Bun.CryptoHasher("sha256").update(JSON.stringify(payload)).digest("hex");
  return db.begin(async (tx) => {
    await tx`INSERT INTO upstream_account_balances(account_id,currency) VALUES(${account},${parsed.credited_currency}) ON CONFLICT DO NOTHING`;
    const balance = (
      await tx`SELECT balance_atoms::text,revision::text FROM upstream_account_balances WHERE account_id=${account} AND currency=${parsed.credited_currency} FOR NO KEY UPDATE`
    )[0];
    const operation = crypto.randomUUID();
    const journal = crypto.randomUUID();
    const receipt: UpstreamRechargeReceipt = {
      operation_id: operation,
      journal_id: journal,
      ...payload,
      actor,
      payload_digest: digest,
      balance_atoms: (BigInt(balance.balance_atoms) + BigInt(parsed.credited_atoms)).toString(),
      balance_revision: (BigInt(balance.revision) + 1n).toString(),
    };
    await tx`INSERT INTO upstream_funding_operations(id,account_id,kind,external_ref,payload_digest,payment_currency,payment_atoms,credited_currency,credited_atoms,voucher_id,voucher_digest,completed_at,actor,reason,receipt) VALUES(${operation},${account},'RECHARGE',${parsed.external_ref},${digest},${parsed.payment_currency},${parsed.payment_atoms},${parsed.credited_currency},${parsed.credited_atoms},${parsed.voucher_id},${parsed.voucher_digest},${parsed.completed_at}::timestamptz,${actor},${parsed.reason},${receipt}::jsonb)`;
    await tx`UPDATE upstream_account_balances SET balance_atoms=${receipt.balance_atoms},revision=${receipt.balance_revision} WHERE account_id=${account} AND currency=${parsed.credited_currency}`;
    await tx`INSERT INTO upstream_journals(id,account_id,operation_id,currency,kind,reference) VALUES(${journal},${account},${operation},${parsed.credited_currency},'RECHARGE',${`recharge:${operation}`})`;
    await tx`INSERT INTO upstream_entries(journal_id,ordinal,currency,account,atoms) VALUES(${journal},0,${parsed.credited_currency},'upstream:available',${parsed.credited_atoms}),(${journal},1,${parsed.credited_currency},'external:funding',${(-BigInt(parsed.credited_atoms)).toString()})`;
    await tx`INSERT INTO upstream_audit_events(id,account_id,actor,action,operation_id,revision,new_digest,reason) VALUES(${crypto.randomUUID()},${account},${actor},'RECHARGE',${operation},${receipt.balance_revision},${digest},${parsed.reason})`;
    return receipt;
  });
}
