import type { SQL } from "bun";
import type { ChargeLotAllocation } from "./types.ts";
import {
  computeFreezeReason,
  sortLots,
  lotEligible,
  toWalletRow,
  toLotRow,
  insertJournalLines,
  flushDirtyWallets,
  flushDirtyLots,
  expireLotsInMemory,
  type JournalLine,
} from "./funds.ts";

export async function collectDebtInTx(tx: SQL, walletId: string, nowMs: number): Promise<number> {
  const wRows = await tx`
      SELECT id, billing_account_id, currency, policy,
        available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
        debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
        credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
      FROM public.wallets WHERE id = ${walletId} FOR NO KEY UPDATE
    `;
  if (!wRows.length) return 0;
  const wallet = toWalletRow(wRows[0]!);
  if (wallet.debt_atoms <= 0n || wallet.available_atoms <= 0n) return 0;

  const chargeRows = await tx`
      SELECT id, request_id, model, operation, debt_atoms::text, funded_atoms::text, lot_allocations
      FROM public.billing_charges
      WHERE wallet_id = ${walletId} AND debt_atoms > 0
      ORDER BY created_at, id
      LIMIT 32
      FOR UPDATE
    `;

  const lotRowsRaw = await tx`
      SELECT id, wallet_id, kind, priority, models, operations, expires_at, created_at,
        issued_atoms::text, available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text
      FROM public.wallet_lots
      WHERE wallet_id = ${walletId} AND available_atoms > 0
      ORDER BY id
      FOR NO KEY UPDATE
    `;
  const lots = sortLots(
    lotRowsRaw.map((r: Record<string, unknown>) => toLotRow(r)),
    wallet.policy,
  );
  const journals: JournalLine[] = [];
  expireLotsInMemory(wallet, lots, nowMs, journals);

  let collectedCharges = 0;
  for (const ch of chargeRows) {
    let debt = BigInt(String(ch.debt_atoms));
    if (debt <= 0n) continue;
    let paid = 0n;
    const allocs = [...((ch.lot_allocations ?? []) as ChargeLotAllocation[])];
    const allocByLot = new Map<string, ChargeLotAllocation>();
    for (const a of allocs) allocByLot.set(a.lot_id, { ...a });
    const lines: { account: string; atoms: bigint }[] = [];
    for (const lot of lots) {
      if (debt === 0n) break;
      if (!lotEligible(lot, String(ch.model), String(ch.operation), nowMs)) continue;
      const take = debt < lot.available_atoms ? debt : lot.available_atoms;
      debt -= take;
      paid += take;
      lot.available_atoms -= take;
      lot.spent_atoms += take;
      lot.dirty = true;
      const existing = allocByLot.get(lot.id);
      if (existing) {
        existing.amount_atoms = (BigInt(existing.amount_atoms) + take).toString();
      } else {
        allocByLot.set(lot.id, {
          lot_id: lot.id,
          amount_atoms: take.toString(),
          refunded_atoms: "0",
        });
      }
      lines.push({ account: `lot:${lot.id}:available`, atoms: -take });
    }
    if (paid === 0n) continue;
    collectedCharges++;
    wallet.available_atoms -= paid;
    wallet.spent_atoms += paid;
    wallet.debt_atoms -= paid;
    wallet.freeze_reason = computeFreezeReason(wallet);
    wallet.dirty = true;
    lines.push({ account: "system:receivable", atoms: paid });
    const jid = crypto.randomUUID();
    for (let ord = 0; ord < lines.length; ord++) {
      journals.push({
        journal_id: jid,
        ordinal: ord,
        side: "CUSTOMER",
        entity_id: wallet.id,
        request_id: String(ch.request_id),
        reference: `debt_collect:${ch.id}:${jid}`,
        kind: "DEBT_COLLECT",
        currency: wallet.currency,
        account: lines[ord]!.account,
        atoms: lines[ord]!.atoms,
      });
    }
    const newFunded = BigInt(String(ch.funded_atoms)) + paid;
    const newOutcome = debt === 0n ? "SETTLED" : "DEBT";
    await tx`
        UPDATE public.billing_charges
        SET funded_atoms = ${newFunded.toString()}::numeric,
            debt_atoms = ${debt.toString()}::numeric,
            lot_allocations = ${[...allocByLot.values()]}::jsonb,
            outcome = ${newOutcome}
        WHERE id = ${ch.id}
      `;
  }

  if (wallet.debt_atoms > 0n && wallet.available_atoms > 0n) {
    const [remRow] = await tx<{ total: string }[]>`
      SELECT coalesce(sum(debt_atoms), 0)::text AS total
      FROM public.billing_charges
      WHERE wallet_id = ${walletId} AND debt_atoms > 0
    `;
    const chargeDebtTotal = BigInt(remRow?.total ?? "0");
    let accountDebt = wallet.debt_atoms - chargeDebtTotal;
    if (accountDebt > 0n) {
      let paidAccount = 0n;
      const lines: { account: string; atoms: bigint }[] = [];
      for (const lot of lots) {
        if (accountDebt === 0n) break;
        if (lot.available_atoms <= 0n) continue;
        if (lot.expires_at_ms !== null && lot.expires_at_ms <= nowMs) continue;
        if (lot.models.length > 0 || lot.operations.length > 0) continue;
        const take = accountDebt < lot.available_atoms ? accountDebt : lot.available_atoms;
        accountDebt -= take;
        paidAccount += take;
        lot.available_atoms -= take;
        lot.spent_atoms += take;
        lot.dirty = true;
        lines.push({ account: `lot:${lot.id}:available`, atoms: -take });
      }
      if (paidAccount > 0n) {
        collectedCharges++;
        wallet.available_atoms -= paidAccount;
        wallet.spent_atoms += paidAccount;
        wallet.debt_atoms -= paidAccount;
        wallet.freeze_reason = computeFreezeReason(wallet);
        wallet.dirty = true;
        lines.push({ account: "system:receivable", atoms: paidAccount });
        const jid = crypto.randomUUID();
        for (let ord = 0; ord < lines.length; ord++) {
          journals.push({
            journal_id: jid,
            ordinal: ord,
            side: "CUSTOMER",
            entity_id: wallet.id,
            request_id: null,
            reference: `debt_collect:wallet:${wallet.id}:${jid}`,
            kind: "DEBT_COLLECT",
            currency: wallet.currency,
            account: lines[ord]!.account,
            atoms: lines[ord]!.atoms,
          });
        }
      }
    }
  }

  await flushDirtyWallets(tx, [wallet]);
  await flushDirtyLots(tx, lots);
  await insertJournalLines(tx, journals);
  return collectedCharges;
}
