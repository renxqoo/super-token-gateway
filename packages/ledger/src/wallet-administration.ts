import type {
  AdjustWalletInput,
  AdjustWalletResult,
  SetWalletFreezeInput,
  WalletSnapshot,
  WalletWithLotsSnapshot,
  WalletLotSnapshot,
} from "./types.ts";
import {
  parseSignedAtoms,
  sha256Hex,
  toWalletRow,
  computeFreezeReason,
  flushDirtyWallets,
  insertJournalLines,
  sortLots,
  toLotRow,
  type JournalLine,
  expireLotsInMemory,
  flushDirtyLots,
} from "./funds.ts";
import { DomainError } from "@gateway/kernel";
import { collectDebtInTx } from "./wallet-debt.ts";
import {
  flushActiveWalletLeaseSpendTx,
  lockActiveWalletLeaseSpendTx,
  type PendingLeaseSpend,
} from "./leases.ts";
import { createDrizzle, wallets, walletLots, type LedgerDrizzle } from "./schema.ts";
import { and, eq, desc, sql } from "drizzle-orm";
import type { SQL } from "bun";

export function createWalletAdministration({
  invalidateWallet,
  db,
  orm,
}: {
  invalidateWallet: (billingAccountId: string, currency: string) => void;
  db: SQL;
  orm: LedgerDrizzle;
}) {
  return {
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
        let pendingLeaseSpend: PendingLeaseSpend[] = [];
        if (delta < 0n) {
          const wIdRows = await tx<{ id: string }[]>`
            SELECT id::text AS id
            FROM public.wallets
            WHERE billing_account_id = ${input.billing_account_id}::uuid AND currency = ${input.currency}
          `;
          if (wIdRows.length > 0) {
            pendingLeaseSpend = await lockActiveWalletLeaseSpendTx(tx, wIdRows[0]!.id);
          }
        }
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

        if (pendingLeaseSpend.length > 0) {
          await flushActiveWalletLeaseSpendTx(tx, wallet, pendingLeaseSpend);
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
        const [leaseSpend] = await tx<{ spent: string }[]>`
          SELECT coalesce(sum(spent_atoms), 0)::text AS spent
          FROM public.wallet_leases
          WHERE wallet_id = ${w.id}::uuid AND state = 'ACTIVE'
        `;
        const deferredSpent = BigInt(leaseSpend?.spent ?? "0");
        return {
          id: w.id,
          billing_account_id: w.billing_account_id,
          currency: w.currency,
          policy: w.policy,
          available_atoms: w.available_atoms.toString(),
          reserved_atoms: (w.reserved_atoms - deferredSpent).toString(),
          spent_atoms: (w.spent_atoms + deferredSpent).toString(),
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
        .select({
          id: wallets.id,
          billingAccountId: wallets.billingAccountId,
          currency: wallets.currency,
          policy: wallets.policy,
          availableAtoms: wallets.availableAtoms,
          reservedAtoms: wallets.reservedAtoms,
          spentAtoms: wallets.spentAtoms,
          expiredAtoms: wallets.expiredAtoms,
          debtAtoms: wallets.debtAtoms,
          creditLimitAtoms: wallets.creditLimitAtoms,
          creditReservedAtoms: wallets.creditReservedAtoms,
          creditPolicyVersion: wallets.creditPolicyVersion,
          manualFrozen: wallets.manualFrozen,
          evidenceDisputed: wallets.evidenceDisputed,
          freezeReason: wallets.freezeReason,
          deferredSpent: sql<string>`coalesce((
            SELECT sum(wl.spent_atoms)
            FROM public.wallet_leases wl
            WHERE wl.wallet_id = "wallets"."id" AND wl.state = 'ACTIVE'
          ), 0)::text`,
        })
        .from(wallets)
        .where(and(eq(wallets.billingAccountId, billingAccountId), eq(wallets.currency, currency)));
      const w = rows[0];
      if (!w) return null;
      const deferredSpent = BigInt(w.deferredSpent);
      return {
        id: w.id,
        billing_account_id: w.billingAccountId,
        currency: w.currency,
        policy: w.policy,
        available_atoms: w.availableAtoms,
        reserved_atoms: (BigInt(w.reservedAtoms) - deferredSpent).toString(),
        spent_atoms: (BigInt(w.spentAtoms) + deferredSpent).toString(),
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
      return db.begin("isolation level repeatable read read only", async (tx) => {
        const txOrm = createDrizzle(tx);
        const rows = await txOrm
          .select()
          .from(wallets)
          .where(
            and(eq(wallets.billingAccountId, billingAccountId), eq(wallets.currency, currency)),
          );
        const w = rows[0];
        if (!w) return null;
        const lotRows = await txOrm
          .select()
          .from(walletLots)
          .where(eq(walletLots.walletId, w.id))
          .orderBy(desc(walletLots.createdAt), desc(walletLots.id))
          .limit(200);
        const leaseSpends = await tx<{ spent_lot_id: string | null; spent: string }[]>`
          SELECT spent_lot_id::text AS spent_lot_id, coalesce(sum(spent_atoms), 0)::text AS spent
          FROM public.wallet_leases
          WHERE wallet_id = ${w.id}::uuid AND state = 'ACTIVE' AND spent_atoms > 0
          GROUP BY spent_lot_id
        `;
        const spentByLot = new Map<string, bigint>();
        let totalDeferredSpent = 0n;
        for (const row of leaseSpends) {
          const spent = BigInt(row.spent);
          totalDeferredSpent += spent;
          if (row.spent_lot_id) {
            spentByLot.set(row.spent_lot_id, (spentByLot.get(row.spent_lot_id) ?? 0n) + spent);
          }
        }
        const lots: WalletLotSnapshot[] = lotRows.map((l) => {
          const lotSpent = spentByLot.get(l.id) ?? 0n;
          return {
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
            reserved_atoms: (BigInt(l.reservedAtoms) - lotSpent).toString(),
            spent_atoms: (BigInt(l.spentAtoms) + lotSpent).toString(),
            expired_atoms: l.expiredAtoms,
            created_at: l.createdAt,
          };
        });
        return {
          id: w.id,
          billing_account_id: w.billingAccountId,
          currency: w.currency,
          policy: w.policy,
          available_atoms: w.availableAtoms,
          reserved_atoms: (BigInt(w.reservedAtoms) - totalDeferredSpent).toString(),
          spent_atoms: (BigInt(w.spentAtoms) + totalDeferredSpent).toString(),
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
      });
    },
  };
}
