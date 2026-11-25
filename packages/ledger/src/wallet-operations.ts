import type { SQL } from "bun";
import { and, desc, eq } from "drizzle-orm";
import { DomainError } from "@gateway/kernel";
import { walletLots, wallets, type LedgerDrizzle } from "./schema.ts";
import type {
  AdjustChargeInput,
  AdjustWalletInput,
  AdjustWalletResult,
  ChargeLotAllocation,
  CreditWalletInput,
  RefundChargeInput,
  SetWalletFreezeInput,
  WalletLotSnapshot,
  WalletPolicy,
  WalletSnapshot,
  WalletWithLotsSnapshot,
} from "./types.ts";
import type { LeaseManager } from "./lease-manager.ts";
import {
  parseAtoms,
  parseSignedAtoms,
  sha256Hex,
  computeFreezeReason,
  sortLots,
  lotEligible,
  toWalletRow,
  toLotRow,
  insertJournalLines,
  flushDirtyWallets,
  flushDirtyLots,
  expireLotsInMemory,
  type WalletRow,
  type LotRow,
  type JournalLine,
} from "./funds.ts";
import {
  flushDirtyKeySpends,
  rolloverKeySpendInMemory,
  toKeySpendRow,
  type KeySpendRow,
} from "./key-spend.ts";

async function lockChargeContext(
  tx: SQL,
  chargeId: string,
  nowMs: number,
): Promise<{ wallet: WalletRow; keySpend: KeySpendRow | null }> {
  const metaRows = await tx`
    SELECT wallet_id, api_key_id FROM public.billing_charges WHERE id = ${chargeId}
  `;
  if (!metaRows.length) throw new Error("Charge not found");
  const walletId = String(metaRows[0]!.wallet_id);
  const apiKeyId = metaRows[0]!.api_key_id ? String(metaRows[0]!.api_key_id) : null;
  let keySpend: KeySpendRow | null = null;
  if (apiKeyId) {
    const kRows = await tx`
      SELECT id, billing_account_id, spend_limit_atoms::text, spend_limit_window,
        spend_window_start, spent_atoms::text, reserved_atoms::text
      FROM public.api_keys
      WHERE id = ${apiKeyId} AND spend_limit_atoms IS NOT NULL
      FOR NO KEY UPDATE
    `;
    if (kRows.length > 0) {
      keySpend = toKeySpendRow(kRows[0]!);
      rolloverKeySpendInMemory(keySpend, nowMs);
    }
  }
  const [wallet] = await tx`
    SELECT id, billing_account_id, currency, policy,
      available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
      debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
      credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
    FROM public.wallets
    WHERE id = ${walletId}
    FOR NO KEY UPDATE
  `;
  if (!wallet) throw new Error("Charge not found");
  return { wallet: toWalletRow(wallet), keySpend };
}

async function collectDebtInTx(tx: SQL, walletId: string, nowMs: number): Promise<number> {
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

export function createWalletOperations(
  db: SQL,
  orm: LedgerDrizzle,
  invalidateWallet: LeaseManager["invalidateWallet"],
) {
  async function refundCharge(input: RefundChargeInput): Promise<{
    operation_id: string;
    replayed: boolean;
    refunded_atoms: string;
    waived_debt_atoms: string;
    expired_atoms: string;
  }> {
    const atoms = parseAtoms(input.amount_atoms);
    if (atoms === null || atoms <= 0n) throw new Error("Refund amount must be positive");
    const fingerprint = sha256Hex({
      charge_id: input.charge_id,
      amount_atoms: input.amount_atoms,
      reason: input.reason,
    });
    return db.begin(async (tx) => {
      const [pgNowRow] = await tx`SELECT floor(extract(epoch from now()) * 1000)::bigint AS ms`;
      const nowMs = pgNowRow ? Number(pgNowRow.ms) : Date.now();
      const { wallet, keySpend } = await lockChargeContext(tx, input.charge_id, nowMs);
      const chRows = await tx`
          SELECT id, request_id, billing_account_id, wallet_id, api_key_id, model, operation, currency,
            customer_atoms::text, funded_atoms::text, debt_atoms::text, refunded_atoms::text,
            debt_refunded_atoms::text, lot_allocations, created_at
          FROM public.billing_charges WHERE id = ${input.charge_id} FOR UPDATE
        `;
      if (!chRows.length) throw new Error("Charge not found");
      const ch = chRows[0]!;

      const existingOp = await tx`
          SELECT id, fingerprint, receipt FROM public.funding_operations
          WHERE side = 'CUSTOMER' AND entity_id = ${ch.id} AND kind = 'REFUND' AND external_ref = ${input.external_ref}
        `;
      if (existingOp.length > 0) {
        if (String(existingOp[0]!.fingerprint) !== fingerprint) {
          throw new Error("Refund external_ref conflict");
        }
        const rec = existingOp[0]!.receipt as {
          refunded_atoms: string;
          waived_debt_atoms: string;
          expired_atoms: string;
        };
        return {
          operation_id: String(existingOp[0]!.id),
          replayed: true,
          refunded_atoms: rec.refunded_atoms,
          waived_debt_atoms: rec.waived_debt_atoms,
          expired_atoms: rec.expired_atoms,
        };
      }

      const customerAtoms = BigInt(String(ch.customer_atoms));
      const alreadyRefunded = BigInt(String(ch.refunded_atoms));
      if (atoms > customerAtoms - alreadyRefunded) {
        throw new Error("Refund exceeds remaining refundable charge amount");
      }

      const chargeDebt = BigInt(String(ch.debt_atoms));
      const waivedDebt = atoms < chargeDebt ? atoms : chargeDebt;
      let rem = atoms - waivedDebt;
      const refundedFromFunded = rem;

      const allocs = [...((ch.lot_allocations ?? []) as ChargeLotAllocation[])].map((a) => ({
        ...a,
      }));
      const lotIds = allocs.map((a) => a.lot_id);
      const lotRowsRaw = lotIds.length
        ? await tx`
              SELECT id, wallet_id, kind, priority, models, operations, expires_at, created_at,
                issued_atoms::text, available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text
              FROM public.wallet_lots WHERE id = ANY(${lotIds}::uuid[]) ORDER BY id FOR NO KEY UPDATE
            `
        : [];
      const lotsById = new Map<string, LotRow>();
      for (const r of lotRowsRaw) {
        const l = toLotRow(r);
        lotsById.set(l.id, l);
      }

      const lines: { account: string; atoms: bigint }[] = [
        { account: "system:revenue", atoms: -atoms },
      ];
      if (waivedDebt > 0n) lines.push({ account: "system:receivable", atoms: waivedDebt });

      let availBack = 0n;
      let expBack = 0n;
      for (let idx = allocs.length - 1; idx >= 0 && rem > 0n; idx--) {
        const a = allocs[idx]!;
        const refundable = BigInt(a.amount_atoms) - BigInt(a.refunded_atoms);
        if (refundable <= 0n) continue;
        const take = rem < refundable ? rem : refundable;
        rem -= take;
        a.refunded_atoms = (BigInt(a.refunded_atoms) + take).toString();
        const lot = lotsById.get(a.lot_id)!;
        lot.spent_atoms -= take;
        if (lot.expires_at_ms !== null && lot.expires_at_ms <= nowMs) {
          lot.expired_atoms += take;
          expBack += take;
          lines.push({ account: `lot:${lot.id}:expired`, atoms: take });
        } else {
          lot.available_atoms += take;
          availBack += take;
          lines.push({ account: `lot:${lot.id}:available`, atoms: take });
        }
        lot.dirty = true;
      }
      if (rem !== 0n) throw new Error("Refund allocation invariant violation");

      wallet.available_atoms += availBack;
      wallet.expired_atoms += expBack;
      wallet.spent_atoms -= refundedFromFunded;
      wallet.debt_atoms -= waivedDebt;
      wallet.freeze_reason = computeFreezeReason(wallet);
      wallet.dirty = true;

      if (keySpend && Date.parse(String(ch.created_at)) >= keySpend.spend_window_start_ms) {
        keySpend.spent_atoms = keySpend.spent_atoms > atoms ? keySpend.spent_atoms - atoms : 0n;
        keySpend.dirty = true;
        await flushDirtyKeySpends(tx, [keySpend]);
      }

      await flushDirtyLots(tx, lotsById.values());
      await flushDirtyWallets(tx, [wallet]);

      const opId = crypto.randomUUID();
      await insertJournalLines(
        tx,
        lines.map((line, ord) => ({
          journal_id: opId,
          ordinal: ord,
          side: "CUSTOMER",
          entity_id: wallet.id,
          request_id: String(ch.request_id),
          reference: `refund:${input.external_ref}`,
          kind: "REFUND",
          currency: wallet.currency,
          account: line.account,
          atoms: line.atoms,
        })),
      );

      await tx`
          UPDATE public.billing_charges
          SET debt_atoms = ${(chargeDebt - waivedDebt).toString()}::numeric,
              debt_refunded_atoms = ${(BigInt(String(ch.debt_refunded_atoms)) + waivedDebt).toString()}::numeric,
              refunded_atoms = ${(alreadyRefunded + atoms).toString()}::numeric,
              lot_allocations = ${allocs}::jsonb
          WHERE id = ${ch.id}
        `;
      const receipt = {
        refunded_atoms: atoms.toString(),
        waived_debt_atoms: waivedDebt.toString(),
        expired_atoms: expBack.toString(),
      };
      await tx`
          INSERT INTO public.funding_operations (id, side, entity_id, kind, external_ref, fingerprint, amount_atoms, currency, reason, receipt)
          VALUES (${opId}, 'CUSTOMER', ${ch.id}, 'REFUND', ${input.external_ref}, ${fingerprint}, ${atoms.toString()}::numeric, ${wallet.currency}, ${input.reason}, ${receipt}::jsonb)
        `;
      const outboxPayload = {
        operation_id: opId,
        charge_id: String(ch.id),
        request_id: String(ch.request_id),
        billing_account_id: String(ch.billing_account_id),
        api_key_id: ch.api_key_id ? String(ch.api_key_id) : null,
        model: String(ch.model),
        operation: String(ch.operation),
        currency: wallet.currency,
        refunded_atoms: atoms.toString(),
        delta_atoms: (-atoms).toString(),
        reason: input.reason,
      };
      await tx`
          INSERT INTO public.outbox_events (event_id, kind, request_id, billing_account_id, payload)
          VALUES (${crypto.randomUUID()}::uuid, 'billing.refunded', ${ch.request_id}::uuid, ${ch.billing_account_id}::uuid, ${outboxPayload}::jsonb)
        `;

      if (availBack > 0n && wallet.debt_atoms > 0n) {
        await collectDebtInTx(tx, wallet.id, nowMs);
      }

      return {
        operation_id: opId,
        replayed: false,
        ...receipt,
      };
    });
  }

  return {
    refundCharge,
    async creditWallet(input: CreditWalletInput): Promise<{
      lot_id: string;
      wallet_id: string;
      replayed: boolean;
      available_atoms: string;
      debt_collected_charges: number;
    }> {
      const atoms = parseAtoms(input.amount_atoms);
      if (atoms === null || atoms <= 0n) {
        throw new Error("Credit amount must be a positive integer");
      }
      invalidateWallet(input.billing_account_id, input.currency);
      const fingerprint = sha256Hex({
        billing_account_id: input.billing_account_id,
        currency: input.currency,
        amount_atoms: input.amount_atoms,
        kind: input.kind,
        priority: input.priority ?? 100,
        expires_at: input.expires_at ?? null,
        models: input.models ?? [],
        operations: input.operations ?? [],
      });
      return db.begin(async (tx) => {
        const nowMs = Date.now();
        await tx`
          INSERT INTO public.wallets (id, billing_account_id, currency)
          VALUES (${crypto.randomUUID()}, ${input.billing_account_id}, ${input.currency})
          ON CONFLICT (billing_account_id, currency) DO NOTHING
        `;
        const wRows = await tx`
          SELECT id, billing_account_id, currency, policy,
            available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
            debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
            credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
          FROM public.wallets
          WHERE billing_account_id = ${input.billing_account_id} AND currency = ${input.currency}
          FOR NO KEY UPDATE
        `;
        const wallet = toWalletRow(wRows[0]!);
        const existingOp = await tx`
          SELECT fingerprint, receipt FROM public.funding_operations
          WHERE side = 'CUSTOMER' AND entity_id = ${wallet.id} AND kind = 'CREDIT' AND external_ref = ${input.external_ref}
        `;
        if (existingOp.length > 0) {
          if (String(existingOp[0]!.fingerprint) !== fingerprint) {
            throw new Error("Credit external_ref conflict with different payload");
          }
          const rec = existingOp[0]!.receipt as { lot_id: string };
          return {
            lot_id: rec.lot_id,
            wallet_id: wallet.id,
            replayed: true,
            available_atoms: wallet.available_atoms.toString(),
            debt_collected_charges: 0,
          };
        }

        const lotId = crypto.randomUUID();
        const opId = crypto.randomUUID();
        const priority = input.priority ?? 100;
        const models = input.models ?? [];
        const operations = input.operations ?? [];
        const expiresAt = input.expires_at ?? null;

        await tx`
          INSERT INTO public.wallet_lots (id, wallet_id, external_ref, kind, priority, models, operations, expires_at, issued_atoms, available_atoms)
          VALUES (${lotId}, ${wallet.id}, ${input.external_ref}, ${input.kind}, ${priority}, ${models}, ${operations}, ${expiresAt}::timestamptz, ${atoms.toString()}::numeric, ${atoms.toString()}::numeric)
        `;
        wallet.available_atoms += atoms;
        wallet.dirty = true;
        await flushDirtyWallets(tx, [wallet]);

        const jid = crypto.randomUUID();
        await insertJournalLines(tx, [
          {
            journal_id: jid,
            ordinal: 0,
            side: "CUSTOMER",
            entity_id: wallet.id,
            request_id: null,
            reference: `credit:${input.external_ref}`,
            kind: "CREDIT",
            currency: wallet.currency,
            account: `lot:${lotId}:available`,
            atoms,
          },
          {
            journal_id: jid,
            ordinal: 1,
            side: "CUSTOMER",
            entity_id: wallet.id,
            request_id: null,
            reference: `credit:${input.external_ref}`,
            kind: "CREDIT",
            currency: wallet.currency,
            account: "system:funding",
            atoms: -atoms,
          },
        ]);

        const collected = await collectDebtInTx(tx, wallet.id, nowMs);
        const refreshed =
          await tx`SELECT available_atoms::text FROM public.wallets WHERE id = ${wallet.id}`;
        const receipt = {
          lot_id: lotId,
          wallet_id: wallet.id,
          amount_atoms: atoms.toString(),
          kind: input.kind,
        };
        const payload = {
          kind: input.kind,
          billing_account_id: input.billing_account_id,
          priority,
          expires_at: expiresAt,
        };
        await tx`
          INSERT INTO public.funding_operations (id, side, entity_id, kind, external_ref, fingerprint, amount_atoms, currency, reason, payload, receipt)
          VALUES (${opId}, 'CUSTOMER', ${wallet.id}, 'CREDIT', ${input.external_ref}, ${fingerprint}, ${atoms.toString()}::numeric, ${wallet.currency}, ${input.reason ?? ""}, ${payload}::jsonb, ${receipt}::jsonb)
        `;
        return {
          lot_id: lotId,
          wallet_id: wallet.id,
          replayed: false,
          available_atoms: String(refreshed[0]!.available_atoms),
          debt_collected_charges: collected,
        };
      });
    },

    async adjustCharge(input: AdjustChargeInput): Promise<{
      operation_id: string;
      replayed: boolean;
      delta_atoms: string;
    }> {
      const delta = parseSignedAtoms(input.delta_atoms);
      if (delta === null || delta === 0n) throw new Error("Adjustment delta must be non-zero");
      if (delta < 0n) {
        const res = await refundCharge({
          charge_id: input.charge_id,
          external_ref: input.external_ref,
          amount_atoms: (-delta).toString(),
          reason: input.reason,
        });
        return {
          operation_id: res.operation_id,
          replayed: res.replayed,
          delta_atoms: delta.toString(),
        };
      }
      const fingerprint = sha256Hex({
        charge_id: input.charge_id,
        delta_atoms: input.delta_atoms,
        reason: input.reason,
      });
      return db.begin(async (tx) => {
        const [pgNowRow] = await tx`SELECT floor(extract(epoch from now()) * 1000)::bigint AS ms`;
        const nowMs = pgNowRow ? Number(pgNowRow.ms) : Date.now();
        const { wallet, keySpend } = await lockChargeContext(tx, input.charge_id, nowMs);
        const chRows = await tx`
          SELECT id, request_id, billing_account_id, wallet_id, api_key_id, model, operation, currency,
            customer_atoms::text, debt_atoms::text, created_at
          FROM public.billing_charges WHERE id = ${input.charge_id} FOR UPDATE
        `;
        if (!chRows.length) throw new Error("Charge not found");
        const ch = chRows[0]!;
        const existingOp = await tx`
          SELECT id, fingerprint FROM public.funding_operations
          WHERE side = 'CUSTOMER' AND entity_id = ${ch.id} AND kind = 'ADJUST' AND external_ref = ${input.external_ref}
        `;
        if (existingOp.length > 0) {
          if (String(existingOp[0]!.fingerprint) !== fingerprint) {
            throw new Error("Adjustment external_ref conflict");
          }
          return {
            operation_id: String(existingOp[0]!.id),
            replayed: true,
            delta_atoms: delta.toString(),
          };
        }
        if (keySpend && Date.parse(String(ch.created_at)) >= keySpend.spend_window_start_ms) {
          keySpend.spent_atoms += delta;
          keySpend.dirty = true;
          await flushDirtyKeySpends(tx, [keySpend]);
        }
        wallet.debt_atoms += delta;
        wallet.freeze_reason = computeFreezeReason(wallet);
        wallet.dirty = true;
        await flushDirtyWallets(tx, [wallet]);
        const opId = crypto.randomUUID();
        await insertJournalLines(tx, [
          {
            journal_id: opId,
            ordinal: 0,
            side: "CUSTOMER",
            entity_id: wallet.id,
            request_id: String(ch.request_id),
            reference: `adjust:${input.external_ref}`,
            kind: "ADJUST",
            currency: wallet.currency,
            account: "system:receivable",
            atoms: -delta,
          },
          {
            journal_id: opId,
            ordinal: 1,
            side: "CUSTOMER",
            entity_id: wallet.id,
            request_id: String(ch.request_id),
            reference: `adjust:${input.external_ref}`,
            kind: "ADJUST",
            currency: wallet.currency,
            account: "system:revenue",
            atoms: delta,
          },
        ]);
        await tx`
          UPDATE public.billing_charges
          SET customer_atoms = ${(BigInt(String(ch.customer_atoms)) + delta).toString()}::numeric,
              debt_atoms = ${(BigInt(String(ch.debt_atoms)) + delta).toString()}::numeric,
              outcome = 'DEBT'
          WHERE id = ${ch.id}
        `;
        await tx`
          INSERT INTO public.funding_operations (id, side, entity_id, kind, external_ref, fingerprint, amount_atoms, currency, reason, receipt)
          VALUES (${opId}, 'CUSTOMER', ${ch.id}, 'ADJUST', ${input.external_ref}, ${fingerprint}, ${delta.toString()}::numeric, ${wallet.currency}, ${input.reason}, ${{ delta_atoms: delta.toString() }}::jsonb)
        `;
        const adjustPayload = {
          operation_id: opId,
          charge_id: String(ch.id),
          request_id: String(ch.request_id),
          billing_account_id: String(ch.billing_account_id),
          api_key_id: ch.api_key_id ? String(ch.api_key_id) : null,
          model: String(ch.model),
          operation: String(ch.operation),
          currency: wallet.currency,
          delta_atoms: delta.toString(),
          reason: input.reason,
        };
        await tx`
          INSERT INTO public.outbox_events (event_id, kind, request_id, billing_account_id, payload)
          VALUES (${crypto.randomUUID()}::uuid, 'billing.adjusted', ${ch.request_id}::uuid, ${ch.billing_account_id}::uuid, ${adjustPayload}::jsonb)
        `;
        await collectDebtInTx(tx, wallet.id, nowMs);
        return { operation_id: opId, replayed: false, delta_atoms: delta.toString() };
      });
    },

    async configureWalletCredit(input: {
      billing_account_id: string;
      currency: string;
      expected_version: string;
      credit_limit_atoms: string;
      policy?: WalletPolicy;
    }): Promise<WalletSnapshot> {
      const limit = parseAtoms(input.credit_limit_atoms);
      if (limit === null) throw new Error("Invalid credit_limit_atoms");
      invalidateWallet(input.billing_account_id, input.currency);
      return db.begin(async (tx) => {
        const rows = await tx`
          SELECT id, billing_account_id, currency, policy,
            available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
            debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
            credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
          FROM public.wallets
          WHERE billing_account_id = ${input.billing_account_id} AND currency = ${input.currency}
          FOR NO KEY UPDATE
        `;
        if (!rows.length) throw new Error("Wallet not found");
        const w = toWalletRow(rows[0]!);
        if (w.credit_policy_version.toString() !== input.expected_version) {
          throw new Error("Credit policy version conflict");
        }
        w.credit_limit_atoms = limit;
        w.credit_policy_version += 1n;
        if (input.policy) w.policy = input.policy;
        w.freeze_reason = computeFreezeReason(w);
        await tx`
          UPDATE public.wallets
          SET credit_limit_atoms = ${w.credit_limit_atoms.toString()}::numeric,
              credit_policy_version = ${w.credit_policy_version.toString()}::numeric,
              policy = ${w.policy},
              freeze_reason = ${w.freeze_reason}
          WHERE id = ${w.id}
        `;
        return {
          id: w.id,
          billing_account_id: w.billing_account_id,
          currency: w.currency,
          policy: w.policy,
          available_atoms: w.available_atoms.toString(),
          reserved_atoms: w.reserved_atoms.toString(),
          spent_atoms: w.spent_atoms.toString(),
          expired_atoms: w.expired_atoms.toString(),
          debt_atoms: w.debt_atoms.toString(),
          credit_limit_atoms: w.credit_limit_atoms.toString(),
          credit_reserved_atoms: w.credit_reserved_atoms.toString(),
          credit_policy_version: w.credit_policy_version.toString(),
          manual_frozen: w.manual_frozen,
          evidence_disputed: w.evidence_disputed,
          freeze_reason: w.freeze_reason,
        };
      });
    },

    async expireAllLots(limit = 100): Promise<number> {
      const targetWallets = await db`
        SELECT DISTINCT wallet_id FROM public.wallet_lots
        WHERE available_atoms > 0 AND expires_at IS NOT NULL AND expires_at <= now()
        LIMIT ${limit}
      `;
      let totalWallets = 0;
      for (const row of targetWallets) {
        const wid = String(row.wallet_id);
        await db.begin(async (tx) => {
          const wRows = await tx`
            SELECT id, billing_account_id, currency, policy,
              available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
              debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
              credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
            FROM public.wallets WHERE id = ${wid} FOR NO KEY UPDATE
          `;
          if (!wRows.length) return;
          const wallet = toWalletRow(wRows[0]!);
          const lRows = await tx`
            SELECT id, wallet_id, kind, priority, models, operations, expires_at, created_at,
              issued_atoms::text, available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text
            FROM public.wallet_lots WHERE wallet_id = ${wid} AND available_atoms > 0 AND expires_at <= now()
            ORDER BY id FOR NO KEY UPDATE
          `;
          const lots = lRows.map((r: Record<string, unknown>) => toLotRow(r));
          const journals: JournalLine[] = [];
          const expired = expireLotsInMemory(wallet, lots, Date.now(), journals);
          if (expired > 0n) {
            totalWallets++;
            await flushDirtyWallets(tx, [wallet]);
            await flushDirtyLots(tx, lots);
            await insertJournalLines(tx, journals);
          }
        });
      }
      return totalWallets;
    },

    async collectAllDebts(limit = 50): Promise<number> {
      const rows = await db`
        SELECT id FROM public.wallets
        WHERE debt_atoms > 0 AND available_atoms > 0
        ORDER BY id LIMIT ${limit}
      `;
      let total = 0;
      for (const r of rows) {
        total += await db.begin((tx) => collectDebtInTx(tx, String(r.id), Date.now()));
      }
      return total;
    },

    async adjustWallet(input: AdjustWalletInput): Promise<AdjustWalletResult> {
      const delta = parseSignedAtoms(input.delta_atoms);
      if (delta === null || delta === 0n) {
        throw new DomainError("validation_error", "Wallet adjustment delta_atoms must be non-zero");
      }
      const lotKind = input.lot_kind ?? "CASH";
      const forceDebt = Boolean(input.force_debt);
      invalidateWallet(input.billing_account_id, input.currency);
      const fingerprint = sha256Hex({
        billing_account_id: input.billing_account_id,
        currency: input.currency,
        delta_atoms: input.delta_atoms,
        lot_kind: lotKind,
        force_debt: forceDebt,
        reason: input.reason,
      });

      return db.begin(async (tx) => {
        const [pgNowRow] = await tx`SELECT floor(extract(epoch from now()) * 1000)::bigint AS ms`;
        const nowMs = pgNowRow ? Number(pgNowRow.ms) : Date.now();
        await tx`
          INSERT INTO public.wallets (id, billing_account_id, currency)
          VALUES (${crypto.randomUUID()}, ${input.billing_account_id}::uuid, ${input.currency})
          ON CONFLICT (billing_account_id, currency) DO NOTHING
        `;
        const wRows = await tx`
          SELECT id, billing_account_id, currency, policy,
            available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
            debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
            credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
          FROM public.wallets
          WHERE billing_account_id = ${input.billing_account_id}::uuid AND currency = ${input.currency}
          FOR NO KEY UPDATE
        `;
        if (!wRows.length) throw new DomainError("wallet_not_found", "Wallet not found");
        const wallet = toWalletRow(wRows[0]!);

        const existingOp = await tx`
          SELECT id, fingerprint, receipt FROM public.funding_operations
          WHERE side = 'CUSTOMER' AND entity_id = ${wallet.id}::uuid
            AND kind = 'ADJUST' AND external_ref = ${input.external_ref}
        `;
        if (existingOp.length > 0) {
          if (String(existingOp[0]!.fingerprint) !== fingerprint) {
            throw new DomainError("adjustment_conflict", "Wallet adjustment external_ref conflict");
          }
          const rec = existingOp[0]!.receipt as {
            lot_id: string | null;
            delta_atoms: string;
            funded_atoms: string;
            debt_delta_atoms: string;
            available_atoms: string;
            debt_atoms: string;
            freeze_reason: string | null;
          };
          return {
            operation_id: String(existingOp[0]!.id),
            wallet_id: wallet.id,
            replayed: true,
            delta_atoms: rec.delta_atoms,
            lot_id: rec.lot_id,
            funded_atoms: rec.funded_atoms,
            debt_delta_atoms: rec.debt_delta_atoms,
            available_atoms: rec.available_atoms,
            debt_atoms: rec.debt_atoms,
            freeze_reason: rec.freeze_reason,
            debt_collected_charges: 0,
          };
        }

        const opId = crypto.randomUUID();
        if (delta > 0n) {
          const lotId = crypto.randomUUID();
          await tx`
            INSERT INTO public.wallet_lots (
              id, wallet_id, external_ref, kind, priority, models, operations, expires_at, issued_atoms, available_atoms
            )
            VALUES (
              ${lotId}::uuid, ${wallet.id}::uuid, ${`adjust:${input.external_ref}`},
              ${lotKind}, 100, '{}'::text[], '{}'::text[], NULL,
              ${delta.toString()}::numeric, ${delta.toString()}::numeric
            )
          `;
          wallet.available_atoms += delta;
          wallet.freeze_reason = computeFreezeReason(wallet);
          wallet.dirty = true;
          await flushDirtyWallets(tx, [wallet]);
          await insertJournalLines(tx, [
            {
              journal_id: opId,
              ordinal: 0,
              side: "CUSTOMER",
              entity_id: wallet.id,
              request_id: null,
              reference: `wallet_adjust:${input.external_ref}`,
              kind: "ADJUST",
              currency: wallet.currency,
              account: `lot:${lotId}:available`,
              atoms: delta,
            },
            {
              journal_id: opId,
              ordinal: 1,
              side: "CUSTOMER",
              entity_id: wallet.id,
              request_id: null,
              reference: `wallet_adjust:${input.external_ref}`,
              kind: "ADJUST",
              currency: wallet.currency,
              account: "system:adjustment",
              atoms: -delta,
            },
          ]);
          const collected = await collectDebtInTx(tx, wallet.id, nowMs);
          const [refreshed] = await tx<
            {
              available_atoms: string;
              debt_atoms: string;
              freeze_reason: string | null;
            }[]
          >`
            SELECT available_atoms::text, debt_atoms::text, freeze_reason
            FROM public.wallets WHERE id = ${wallet.id}::uuid
          `;
          const receipt = {
            wallet_id: wallet.id,
            lot_id: lotId,
            delta_atoms: delta.toString(),
            funded_atoms: delta.toString(),
            debt_delta_atoms: "0",
            available_atoms: String(refreshed!.available_atoms),
            debt_atoms: String(refreshed!.debt_atoms),
            freeze_reason: refreshed!.freeze_reason,
          };
          const payload = {
            actor: input.actor ?? null,
            billing_account_id: input.billing_account_id,
            lot_kind: lotKind,
            force_debt: false,
          };
          await tx`
            INSERT INTO public.funding_operations (
              id, side, entity_id, kind, external_ref, fingerprint,
              amount_atoms, currency, reason, payload, receipt
            )
            VALUES (
              ${opId}::uuid, 'CUSTOMER', ${wallet.id}::uuid, 'ADJUST',
              ${input.external_ref}, ${fingerprint}, ${delta.toString()}::numeric,
              ${wallet.currency}, ${input.reason}, ${payload}::jsonb, ${receipt}::jsonb
            )
          `;
          return {
            operation_id: opId,
            wallet_id: wallet.id,
            replayed: false,
            delta_atoms: delta.toString(),
            lot_id: lotId,
            funded_atoms: delta.toString(),
            debt_delta_atoms: "0",
            available_atoms: receipt.available_atoms,
            debt_atoms: receipt.debt_atoms,
            freeze_reason: receipt.freeze_reason,
            debt_collected_charges: collected,
          };
        }

        const amount = -delta;
        const lotRowsRaw = await tx`
          SELECT id, wallet_id, kind, priority, models, operations, expires_at, created_at,
            issued_atoms::text, available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text
          FROM public.wallet_lots
          WHERE wallet_id = ${wallet.id}::uuid AND available_atoms > 0
          ORDER BY id
          FOR NO KEY UPDATE
        `;
        const lots = sortLots(
          lotRowsRaw.map((r: Record<string, unknown>) => toLotRow(r)),
          wallet.policy,
        );
        const journals: JournalLine[] = [];
        expireLotsInMemory(wallet, lots, nowMs, journals);

        let rem = amount;
        let fundedPart = 0n;
        const lines: { account: string; atoms: bigint }[] = [];
        for (const lot of lots) {
          if (rem === 0n) break;
          if (lot.available_atoms <= 0n) continue;
          if (lot.expires_at_ms !== null && lot.expires_at_ms <= nowMs) continue;
          const take = rem < lot.available_atoms ? rem : lot.available_atoms;
          rem -= take;
          fundedPart += take;
          lot.available_atoms -= take;
          lot.spent_atoms += take;
          lot.dirty = true;
          lines.push({ account: `lot:${lot.id}:available`, atoms: -take });
        }

        const shortfall = rem;
        if (shortfall > 0n) {
          if (!forceDebt) {
            if (wallet.credit_limit_atoms === 0n) {
              throw new DomainError(
                "insufficient_balance",
                "Wallet has insufficient available balance and zero credit limit",
              );
            }
            if (
              wallet.debt_atoms + wallet.credit_reserved_atoms + shortfall >
              wallet.credit_limit_atoms
            ) {
              throw new DomainError(
                "credit_limit_exceeded",
                "Adjustment exceeds available wallet balance and credit limit",
              );
            }
          }
          wallet.debt_atoms += shortfall;
          lines.push({ account: "system:receivable", atoms: -shortfall });
        }

        lines.push({ account: "system:adjustment", atoms: amount });
        wallet.available_atoms -= fundedPart;
        wallet.spent_atoms += fundedPart;
        wallet.freeze_reason = computeFreezeReason(wallet);
        wallet.dirty = true;

        for (let ord = 0; ord < lines.length; ord++) {
          journals.push({
            journal_id: opId,
            ordinal: ord,
            side: "CUSTOMER",
            entity_id: wallet.id,
            request_id: null,
            reference: `wallet_adjust:${input.external_ref}`,
            kind: "ADJUST",
            currency: wallet.currency,
            account: lines[ord]!.account,
            atoms: lines[ord]!.atoms,
          });
        }

        await flushDirtyLots(tx, lots);
        await flushDirtyWallets(tx, [wallet]);
        await insertJournalLines(tx, journals);

        const receipt = {
          wallet_id: wallet.id,
          lot_id: null,
          delta_atoms: delta.toString(),
          funded_atoms: fundedPart.toString(),
          debt_delta_atoms: shortfall.toString(),
          available_atoms: wallet.available_atoms.toString(),
          debt_atoms: wallet.debt_atoms.toString(),
          freeze_reason: wallet.freeze_reason,
        };
        const payload = {
          actor: input.actor ?? null,
          billing_account_id: input.billing_account_id,
          lot_kind: lotKind,
          force_debt: forceDebt,
        };
        await tx`
          INSERT INTO public.funding_operations (
            id, side, entity_id, kind, external_ref, fingerprint,
            amount_atoms, currency, reason, payload, receipt
          )
          VALUES (
            ${opId}::uuid, 'CUSTOMER', ${wallet.id}::uuid, 'ADJUST',
            ${input.external_ref}, ${fingerprint}, ${delta.toString()}::numeric,
            ${wallet.currency}, ${input.reason}, ${payload}::jsonb, ${receipt}::jsonb
          )
        `;
        return {
          operation_id: opId,
          wallet_id: wallet.id,
          replayed: false,
          delta_atoms: delta.toString(),
          lot_id: null,
          funded_atoms: fundedPart.toString(),
          debt_delta_atoms: shortfall.toString(),
          available_atoms: receipt.available_atoms,
          debt_atoms: receipt.debt_atoms,
          freeze_reason: receipt.freeze_reason,
          debt_collected_charges: 0,
        };
      });
    },

    async setWalletFreeze(input: SetWalletFreezeInput): Promise<WalletSnapshot> {
      invalidateWallet(input.billing_account_id, input.currency);
      return db.begin(async (tx) => {
        const rows = await tx`
          SELECT id, billing_account_id, currency, policy,
            available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
            debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
            credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
          FROM public.wallets
          WHERE billing_account_id = ${input.billing_account_id}::uuid AND currency = ${input.currency}
          FOR NO KEY UPDATE
        `;
        if (!rows.length) throw new DomainError("wallet_not_found", "Wallet not found");
        const w = toWalletRow(rows[0]!);
        w.manual_frozen = input.frozen;
        w.freeze_reason = computeFreezeReason(w);
        await tx`
          UPDATE public.wallets
          SET manual_frozen = ${w.manual_frozen},
              freeze_reason = ${w.freeze_reason}
          WHERE id = ${w.id}::uuid
        `;
        return {
          id: w.id,
          billing_account_id: w.billing_account_id,
          currency: w.currency,
          policy: w.policy,
          available_atoms: w.available_atoms.toString(),
          reserved_atoms: w.reserved_atoms.toString(),
          spent_atoms: w.spent_atoms.toString(),
          expired_atoms: w.expired_atoms.toString(),
          debt_atoms: w.debt_atoms.toString(),
          credit_limit_atoms: w.credit_limit_atoms.toString(),
          credit_reserved_atoms: w.credit_reserved_atoms.toString(),
          credit_policy_version: w.credit_policy_version.toString(),
          manual_frozen: w.manual_frozen,
          evidence_disputed: w.evidence_disputed,
          freeze_reason: w.freeze_reason,
        };
      });
    },

    async getWallet(billingAccountId: string, currency: string): Promise<WalletSnapshot | null> {
      const rows = await orm
        .select()
        .from(wallets)
        .where(and(eq(wallets.billingAccountId, billingAccountId), eq(wallets.currency, currency)));
      const w = rows[0];
      if (!w) return null;
      return {
        id: w.id,
        billing_account_id: w.billingAccountId,
        currency: w.currency,
        policy: w.policy,
        available_atoms: w.availableAtoms,
        reserved_atoms: w.reservedAtoms,
        spent_atoms: w.spentAtoms,
        expired_atoms: w.expiredAtoms,
        debt_atoms: w.debtAtoms,
        credit_limit_atoms: w.creditLimitAtoms,
        credit_reserved_atoms: w.creditReservedAtoms,
        credit_policy_version: w.creditPolicyVersion,
        manual_frozen: w.manualFrozen,
        evidence_disputed: w.evidenceDisputed,
        freeze_reason: w.freezeReason,
      };
    },

    async getWalletWithLots(
      billingAccountId: string,
      currency: string,
    ): Promise<WalletWithLotsSnapshot | null> {
      const rows = await orm
        .select()
        .from(wallets)
        .where(and(eq(wallets.billingAccountId, billingAccountId), eq(wallets.currency, currency)));
      const w = rows[0];
      if (!w) return null;
      const lotRows = await orm
        .select()
        .from(walletLots)
        .where(eq(walletLots.walletId, w.id))
        .orderBy(desc(walletLots.createdAt), desc(walletLots.id))
        .limit(200);
      const lots: WalletLotSnapshot[] = lotRows.map((l) => ({
        id: l.id,
        wallet_id: l.walletId,
        external_ref: l.externalRef,
        kind: l.kind,
        priority: l.priority,
        models: l.models,
        operations: l.operations,
        expires_at: l.expiresAt,
        issued_atoms: l.issuedAtoms,
        available_atoms: l.availableAtoms,
        reserved_atoms: l.reservedAtoms,
        spent_atoms: l.spentAtoms,
        expired_atoms: l.expiredAtoms,
        created_at: l.createdAt,
      }));
      return {
        id: w.id,
        billing_account_id: w.billingAccountId,
        currency: w.currency,
        policy: w.policy,
        available_atoms: w.availableAtoms,
        reserved_atoms: w.reservedAtoms,
        spent_atoms: w.spentAtoms,
        expired_atoms: w.expiredAtoms,
        debt_atoms: w.debtAtoms,
        credit_limit_atoms: w.creditLimitAtoms,
        credit_reserved_atoms: w.creditReservedAtoms,
        credit_policy_version: w.creditPolicyVersion,
        manual_frozen: w.manualFrozen,
        evidence_disputed: w.evidenceDisputed,
        freeze_reason: w.freezeReason,
        lots,
      };
    },
  };
}
