import type { SQL } from "bun";
import { formatRational, parseRational, type Pricing, type PricingSpec } from "@gateway/kernel";
import type {
  CommitTerminalInput,
  CommitTerminalResult,
  PriceBasis,
  ResolveRequestInput,
  ResolveSupplierAttemptInput,
  ResolveSupplierAttemptResult,
  SupplierHoldEntry,
} from "./types.ts";
import { hasPricingSpec, rateCardFromPriceVersion } from "./catalog-pricing.ts";
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
          SELECT state, supplier_holds, pricing_override_id, discount_numerator::text, discount_denominator::text, created_at
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
        SELECT id, currency, basis, rates, ceilings, pricing_spec, source_digest FROM public.price_versions
        WHERE id = ${input.billing_price_id} AND side = 'CUSTOMER'
      `;
        if (!pr) throw new Error("Customer price version not found");
        const allSup = (hold.supplier_holds ?? []) as SupplierHoldEntry[];
        const sh = [...allSup].reverse().find((s) => s.state === "HELD" || s.state === "UNKNOWN");
        if (!sh) throw new Error("No unresolved supplier attempt");
        if (
          sh.customer_pricing_snapshot &&
          String(pr.id) !== sh.customer_pricing_snapshot.rate_card_id &&
          sh.customer_price_version_id !== String(pr.id)
        ) {
          throw new Error("Customer price version conflicts with the recorded attempt snapshot");
        }
        const effectiveBasis = sh.customer_pricing_snapshot
          ? sh.customer_pricing_snapshot.basis
          : (pr.basis as CommitTerminalInput["billing_basis"]);
        if (effectiveBasis === "PROVIDER_USAGE" && !input.usage) {
          throw new Error("Verified usage required for provider-usage settlement");
        }
        if (effectiveBasis === "INPUT_CHARACTERS" && input.usage?.characters === undefined) {
          throw new Error("Verified character usage required for input-character settlement");
        }
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
        const allowedSupplierCurrencies = sh.supplier_pricing_snapshot
          ? [...new Set([sh.currency, sh.supplier_pricing_snapshot.base_currency])]
          : [sh.currency];
        const [supplierPrice] = await tx`
        SELECT currency, basis, rates FROM public.price_versions
        WHERE id = ${costVersionId} AND side = 'SUPPLIER' AND currency = ANY(${allowedSupplierCurrencies}::text[])
      `;
        if (!supplierPrice)
          throw new Error("Supplier price version not found for attempt currency");
        const adjustment = sh.customer_price_adjustment;
        const effectiveOverrideId =
          adjustment !== undefined
            ? adjustment.override_id
            : hold.pricing_override_id
              ? String(hold.pricing_override_id)
              : null;
        const effectiveDiscNum =
          adjustment !== undefined
            ? adjustment.discount_numerator
            : hold.discount_numerator
              ? String(hold.discount_numerator)
              : null;
        const effectiveDiscDen =
          adjustment !== undefined
            ? adjustment.discount_denominator
            : hold.discount_denominator
              ? String(hold.discount_denominator)
              : null;
        const effectiveRates = scalePricingRates(
          pr.rates as Pricing["rates"],
          effectiveDiscNum,
          effectiveDiscDen,
        );
        let customerSnapshot = sh.customer_pricing_snapshot;
        const supplierSnapshot = sh.supplier_pricing_snapshot;
        const prSpec = (pr.pricing_spec ?? null) as PricingSpec | null;
        if (!customerSnapshot && prSpec?.matrix && prSpec.matrix.length > 0) {
          throw new Error(
            "Historical customer_pricing_snapshot is required to settle a matrix price version",
          );
        }
        if (!customerSnapshot && hasPricingSpec(prSpec)) {
          const card = rateCardFromPriceVersion({
            id: String(pr.id),
            side: "CUSTOMER",
            currency: String(pr.currency),
            basis: pr.basis as PriceBasis,
            rates: pr.rates as Pricing["rates"],
            ceilings: (pr.ceilings ?? {}) as Record<string, string>,
            pricing_spec: prSpec,
            source_digest: String(pr.source_digest),
          });
          const hasDisc =
            effectiveDiscNum !== null &&
            effectiveDiscDen !== null &&
            BigInt(effectiveDiscNum) > 0n &&
            BigInt(effectiveDiscDen) > 0n;
          const combined = hasDisc
            ? formatRational(
                parseRational({ numerator: effectiveDiscNum!, denominator: effectiveDiscDen! }),
              )
            : { numerator: "1", denominator: "1" };
          const ratedAt =
            hold.created_at instanceof Date
              ? hold.created_at.toISOString()
              : new Date(String(hold.created_at)).toISOString();
          customerSnapshot = {
            version: 1,
            rated_at: ratedAt,
            side: "CUSTOMER",
            currency: card.currency,
            base_currency: card.currency,
            rate_card_id: card.id,
            basis: card.basis,
            effective_card: card,
            matched_matrix_index: null,
            applied_modifiers: [],
            fx_version_id: null,
            fx_rate: null,
            combined_multiplier: combined,
          };
        }
        const item: CommitTerminalInput = {
          request_id: input.request_id,
          selected_attempt_id: sh.attempt_id,
          channel_id: sh.channel_id,
          billing_price_id: customerSnapshot ? customerSnapshot.rate_card_id : String(pr.id),
          cost_version_id: costVersionId,
          pricing_override_id: effectiveOverrideId,
          discount_numerator: effectiveDiscNum,
          discount_denominator: effectiveDiscDen,
          usage: input.usage ?? { requests: "1" },
          billing_pricing: {
            currency: customerSnapshot ? customerSnapshot.currency : String(pr.currency),
            rates: effectiveRates,
          },
          billing_basis: customerSnapshot
            ? customerSnapshot.basis
            : (pr.basis as CommitTerminalInput["billing_basis"]),
          ...(input.usage?.characters === undefined
            ? {}
            : { input_characters: input.usage.characters }),
          supplier_pricing: {
            currency: sh.currency,
            rates: supplierPrice.rates as Pricing["rates"],
          },
          supplier_currency: sh.currency,
          supplier_cost_atoms_override: input.supplier_cost_atoms,
          ...(customerSnapshot ? { customer_pricing_snapshot: customerSnapshot } : {}),
          ...(supplierSnapshot ? { supplier_pricing_snapshot: supplierSnapshot } : {}),
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
