import type { SQL } from "bun";
import { and, eq } from "drizzle-orm";
import { wallets, type LedgerDrizzle } from "./schema.ts";
import type {
  AdjustChargeInput,
  ChargeLotAllocation,
  CreditWalletInput,
  RefundChargeInput,
  WalletPolicy,
  WalletSnapshot,
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

async function lockChargeWallet(tx: SQL, chargeId: string): Promise<WalletRow> {
  const [wallet] = await tx`
    SELECT id, project_id, currency, policy,
      available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
      debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
      credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
    FROM public.wallets
    WHERE id = (SELECT wallet_id FROM public.billing_charges WHERE id = ${chargeId})
    FOR NO KEY UPDATE
  `;
  if (!wallet) throw new Error("Charge not found");
  return toWalletRow(wallet);
}

async function collectDebtInTx(tx: SQL, walletId: string, nowMs: number): Promise<number> {
  const wRows = await tx`
      SELECT id, project_id, currency, policy,
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
  if (!chargeRows.length) return 0;

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
      const nowMs = Date.now();
      const wallet = await lockChargeWallet(tx, input.charge_id);
      const chRows = await tx`
          SELECT id, request_id, wallet_id, currency, customer_atoms::text, funded_atoms::text,
            debt_atoms::text, refunded_atoms::text, debt_refunded_atoms::text, lot_allocations
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
      invalidateWallet(input.project_id, input.currency);
      const fingerprint = sha256Hex({
        project_id: input.project_id,
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
          INSERT INTO public.wallets (id, project_id, currency)
          VALUES (${crypto.randomUUID()}, ${input.project_id}, ${input.currency})
          ON CONFLICT (project_id, currency) DO NOTHING
        `;
        const wRows = await tx`
          SELECT id, project_id, currency, policy,
            available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
            debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
            credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
          FROM public.wallets
          WHERE project_id = ${input.project_id} AND currency = ${input.currency}
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
        const receipt = { lot_id: lotId, wallet_id: wallet.id, amount_atoms: atoms.toString() };
        await tx`
          INSERT INTO public.funding_operations (id, side, entity_id, kind, external_ref, fingerprint, amount_atoms, currency, reason, receipt)
          VALUES (${opId}, 'CUSTOMER', ${wallet.id}, 'CREDIT', ${input.external_ref}, ${fingerprint}, ${atoms.toString()}::numeric, ${wallet.currency}, ${input.reason ?? ""}, ${receipt}::jsonb)
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
        const wallet = await lockChargeWallet(tx, input.charge_id);
        const chRows = await tx`
          SELECT id, request_id, wallet_id, currency, customer_atoms::text, debt_atoms::text
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
        await collectDebtInTx(tx, wallet.id, Date.now());
        return { operation_id: opId, replayed: false, delta_atoms: delta.toString() };
      });
    },

    async configureWalletCredit(input: {
      project_id: string;
      currency: string;
      expected_version: string;
      credit_limit_atoms: string;
      policy?: WalletPolicy;
    }): Promise<WalletSnapshot> {
      const limit = parseAtoms(input.credit_limit_atoms);
      if (limit === null) throw new Error("Invalid credit_limit_atoms");
      invalidateWallet(input.project_id, input.currency);
      return db.begin(async (tx) => {
        const rows = await tx`
          SELECT id, project_id, currency, policy,
            available_atoms::text, reserved_atoms::text, spent_atoms::text, expired_atoms::text,
            debt_atoms::text, credit_limit_atoms::text, credit_reserved_atoms::text,
            credit_policy_version::text, manual_frozen, evidence_disputed, freeze_reason
          FROM public.wallets
          WHERE project_id = ${input.project_id} AND currency = ${input.currency}
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
          project_id: w.project_id,
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
            SELECT id, project_id, currency, policy,
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

    async getWallet(projectId: string, currency: string): Promise<WalletSnapshot | null> {
      const rows = await orm
        .select()
        .from(wallets)
        .where(and(eq(wallets.projectId, projectId), eq(wallets.currency, currency)));
      const w = rows[0];
      if (!w) return null;
      return {
        id: w.id,
        project_id: w.projectId,
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
  };
}
