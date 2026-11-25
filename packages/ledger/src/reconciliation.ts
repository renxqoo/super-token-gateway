import type { SQL } from "bun";
import type { Pricing } from "@gateway/kernel";
import type {
  CommitTerminalInput,
  CommitTerminalResult,
  ResolveRequestInput,
  ResolveSupplierAttemptInput,
  ResolveSupplierAttemptResult,
  SupplierHoldEntry,
} from "./types.ts";
import { parseAtoms, insertJournalLines } from "./funds.ts";
import { notifyHoldRelease, releaseHoldInTx } from "./hold-transitions.ts";
import { applyDirectSettlements, markAccountsReconcileRequired } from "./settlement-direct.ts";
import type { LeaseManager } from "./lease-manager.ts";

function gcdBigInt(a: bigint, b: bigint): bigint {
  let x = a < 0n ? -a : a;
  let y = b < 0n ? -b : b;
  while (y !== 0n) {
    const t = x % y;
    x = y;
    y = t;
  }
  return x === 0n ? 1n : x;
}

export function scalePricingRates(
  rates: Pricing["rates"],
  numStr: string | null,
  denStr: string | null,
): Pricing["rates"] {
  if (!numStr || !denStr) return rates;
  const num = BigInt(numStr);
  const den = BigInt(denStr);
  if (num <= 0n || den <= 0n) return rates;
  const out: Record<string, { numerator: string; denominator: string }> = {};
  for (const [metric, rate] of Object.entries(rates)) {
    if (!rate) continue;
    const rn = BigInt(rate.numerator) * num;
    const rd = BigInt(rate.denominator) * den;
    const g = gcdBigInt(rn, rd);
    out[metric] = { numerator: (rn / g).toString(), denominator: (rd / g).toString() };
  }
  return out as Pricing["rates"];
}

async function recordResolution(tx: SQL, input: ResolveRequestInput): Promise<void> {
  const billingPriceId = input.action === "SETTLE" ? (input.billing_price_id ?? null) : null;
  const usage = input.action === "SETTLE" ? (input.usage ?? {}) : {};
  await tx`
    INSERT INTO public.billing_resolutions (id, request_id, action, billing_price_id, usage, reason, actor)
    VALUES (${crypto.randomUUID()}, ${input.request_id}, ${input.action}, ${billingPriceId},
      ${usage}::jsonb, ${input.reason}, ${input.actor})
  `;
}

export async function resolveSupplierAttemptInTx(
  tx: SQL,
  input: ResolveSupplierAttemptInput,
  allowHistoricalHeld = false,
): Promise<ResolveSupplierAttemptResult> {
  const cost = input.action === "SETTLE" ? parseAtoms(input.supplier_cost_atoms) : 0n;
  if (cost === null) return { ok: false, code: "invalid_supplier_cost" };
  const state = input.action === "SETTLE" ? "SETTLED" : "RELEASED";
  const [hold] = await tx`
      SELECT supplier_holds, supplier_lease_id, state FROM public.billing_holds
      WHERE request_id = ${input.request_id} FOR NO KEY UPDATE
    `;
  const attempts = (hold?.supplier_holds ?? []) as SupplierHoldEntry[];
  const attempt = attempts.find((entry) => entry.attempt_id === input.attempt_id);
  if (!attempt) return { ok: false, code: "supplier_attempt_not_found" };
  if (attempt.resolution) {
    if (
      attempt.resolution.action === input.action &&
      attempt.resolution.evidence_ref === input.evidence_ref &&
      attempt.resolution.supplier_cost_atoms === cost.toString()
    )
      return { ok: true, replayed: true, state };
    return { ok: false, code: "supplier_attempt_conflict" };
  }
  if (
    (attempt.state !== "UNKNOWN" &&
      !(allowHistoricalHeld && hold!.state === "UNKNOWN" && attempt.state === "HELD")) ||
    hold!.state === "HELD" ||
    hold!.supplier_lease_id
  ) {
    return { ok: false, code: "supplier_attempt_conflict" };
  }
  await tx`
      SELECT id FROM public.upstream_accounts
      WHERE id = ${attempt.account_id}
      FOR NO KEY UPDATE
    `;
  const [cell] = await tx`
      UPDATE public.supplier_cells
      SET reserved_atoms = reserved_atoms - ${attempt.held_atoms}::numeric,
        balance_atoms = balance_atoms - ${cost.toString()}::numeric,
        spent_atoms = spent_atoms + ${cost.toString()}::numeric,
        revision = revision + 1
      WHERE account_id = ${attempt.account_id} AND currency = ${attempt.currency}
        AND cell = ${attempt.cell} AND reserved_atoms >= ${attempt.held_atoms}::numeric
      RETURNING balance_atoms::text
    `;
  if (!cell) return { ok: false, code: "supplier_attempt_conflict" };
  if (cost > 0n) {
    const journal = {
      journal_id: crypto.randomUUID(),
      side: "SUPPLIER",
      entity_id: attempt.account_id,
      request_id: input.request_id,
      reference: `supplier_attempt_resolution:${input.attempt_id}`,
      kind: "SUPPLIER_COST",
      currency: attempt.currency,
    } as const;
    await insertJournalLines(tx, [
      { ...journal, ordinal: 0, account: "upstream:available", atoms: -cost },
      { ...journal, ordinal: 1, account: "upstream:expense", atoms: cost },
    ]);
  }
  if (cost > BigInt(attempt.held_atoms) || BigInt(String(cell.balance_atoms)) < 0n) {
    await tx`
        UPDATE public.upstream_accounts
        SET state = 'RECONCILE_REQUIRED', revision = revision + 1, updated_at = now()
        WHERE id = ${attempt.account_id} AND state = 'ACTIVE'
      `;
    await tx`UPDATE public.config_epoch SET version = version + 1 WHERE id = 1`;
  }
  attempt.state = state;
  attempt.resolution = {
    action: input.action,
    supplier_cost_atoms: cost.toString(),
    evidence_ref: input.evidence_ref,
    reason: input.reason,
    actor: input.actor,
  };
  await tx`
      UPDATE public.billing_holds SET supplier_holds = ${attempts}::jsonb
      WHERE request_id = ${input.request_id}
    `;
  return { ok: true, replayed: false, state };
}

export function createReconciliation(
  db: SQL,
  notifyDetachedLocal: LeaseManager["notifyDetachedLocal"],
) {
  return {
    async resolveSupplierAttempt(
      input: ResolveSupplierAttemptInput,
    ): Promise<ResolveSupplierAttemptResult> {
      return db.begin(async (tx) => {
        await tx`SELECT request_id FROM public.billing_holds
          WHERE request_id=${input.request_id} FOR NO KEY UPDATE`;
        const [pending] = await tx`SELECT 1 FROM public.billing_reconciliation_receipts
          WHERE request_id=${input.request_id} AND attempt_id=${input.attempt_id}
            AND state<>'DISMISSED'
            AND (state<>'APPLIED' OR (verdict='FINAL' AND ${input.action}='RELEASE'))`;
        if (pending) return { ok: false, code: "supplier_attempt_conflict" };
        return resolveSupplierAttemptInTx(tx, input);
      });
    },

    async resolveRequest(
      input: ResolveRequestInput,
    ): Promise<{ replayed: boolean; state: string }> {
      const settled = await db.begin(async (tx) => {
        const [hold] = await tx`
          SELECT state, supplier_holds, discount_numerator::text, discount_denominator::text
          FROM public.billing_holds
          WHERE request_id = ${input.request_id} FOR NO KEY UPDATE
        `;
        if (!hold) throw new Error("Billing hold not found");
        const [existing] = await tx`
          SELECT action FROM public.billing_resolutions WHERE request_id = ${input.request_id}
        `;
        if (existing) {
          return { kind: "replay" as const, state: String(existing.action) };
        }
        if (hold.state !== "HELD" && hold.state !== "UNKNOWN") {
          throw new Error(`Hold already finalized in state ${hold.state}`);
        }
        const [queued] = await tx`SELECT 1 FROM public.billing_reconciliation_receipts
          WHERE request_id=${input.request_id} AND state<>'DISMISSED' LIMIT 1`;
        if (queued) throw new Error("Request already has a reconciliation receipt");

        if (input.action === "RELEASE") {
          const release = await releaseHoldInTx(tx, input.request_id);
          if (!release.released) throw new Error("Failed to release billing hold");
          await recordResolution(tx, input);
          return { kind: "release" as const, state: "RELEASED", release };
        }

        if (!input.billing_price_id) throw new Error("billing_price_id required for SETTLE");
        if (input.supplier_cost_atoms === undefined)
          throw new Error("supplier_cost_atoms required for SETTLE");
        const [pr] = await tx`
        SELECT id, currency, basis, rates FROM public.price_versions
        WHERE id = ${input.billing_price_id} AND side = 'CUSTOMER'
      `;
        if (!pr) throw new Error("Customer price version not found");
        if (pr.basis === "PROVIDER_USAGE" && !input.usage) {
          throw new Error("Verified usage required for provider-usage settlement");
        }
        const allSup = (hold.supplier_holds ?? []) as SupplierHoldEntry[];
        const sh = [...allSup].reverse().find((s) => s.state === "HELD" || s.state === "UNKNOWN");
        if (!sh) throw new Error("No unresolved supplier attempt");
        if (
          input.supplier_cost_version_id &&
          sh.cost_version_id &&
          input.supplier_cost_version_id !== sh.cost_version_id
        ) {
          throw new Error("Supplier price version conflicts with the recorded attempt");
        }
        const costVersionId = sh.cost_version_id ?? input.supplier_cost_version_id;
        if (!costVersionId)
          throw new Error("supplier_cost_version_id required for a historical hold");
        const [supplierPrice] = await tx`
        SELECT currency, basis, rates FROM public.price_versions
        WHERE id = ${costVersionId} AND side = 'SUPPLIER' AND currency = ${sh.currency}
      `;
        if (!supplierPrice)
          throw new Error("Supplier price version not found for attempt currency");
        const effectiveRates = scalePricingRates(
          pr.rates as Pricing["rates"],
          hold.discount_numerator ? String(hold.discount_numerator) : null,
          hold.discount_denominator ? String(hold.discount_denominator) : null,
        );
        const item: CommitTerminalInput = {
          request_id: input.request_id,
          selected_attempt_id: sh.attempt_id,
          channel_id: sh.channel_id,
          billing_price_id: String(pr.id),
          cost_version_id: costVersionId,
          usage: input.usage ?? { requests: "1" },
          billing_pricing: { currency: String(pr.currency), rates: effectiveRates },
          billing_basis: pr.basis as CommitTerminalInput["billing_basis"],
          ...(input.usage?.characters === undefined
            ? {}
            : { input_characters: input.usage.characters }),
          supplier_pricing: {
            currency: sh.currency,
            rates: supplierPrice.rates as Pricing["rates"],
          },
          supplier_currency: sh.currency,
          supplier_cost_atoms_override: input.supplier_cost_atoms,
        };
        const results: CommitTerminalResult[] = [];
        const { detached, reconcileAccounts } = await applyDirectSettlements(
          tx,
          [{ index: 0, item }],
          results,
        );
        const commit = results[0]!;
        if (!commit.ok) throw new Error(commit.message);
        await recordResolution(tx, input);
        return { kind: "settle" as const, state: commit.outcome, detached, reconcileAccounts };
      });
      if (settled.kind === "release") {
        notifyHoldRelease(notifyDetachedLocal, input.request_id, settled.release);
      } else if (settled.kind === "settle") {
        if (settled.reconcileAccounts.length > 0) {
          await markAccountsReconcileRequired(db, settled.reconcileAccounts);
        }
        for (const d of settled.detached) {
          notifyDetachedLocal(d.requestId, d.wlId, d.heldCust, d.slId, d.supAtoms);
        }
        notifyDetachedLocal(input.request_id, null, 0n, null, 0n);
      }
      return { replayed: settled.kind === "replay", state: settled.state };
    },
  };
}
