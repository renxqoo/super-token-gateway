import type { SQL } from "bun";
import { canonical, type Pricing, type Usage } from "@gateway/kernel";
import type {
  CommitTerminalInput,
  CommitTerminalResult,
  PriceBasis,
  ReconciliationPolicy,
  ReconciliationReceipt,
  SupplierHoldEntry,
} from "./types.ts";
import { notifyHoldRelease, releaseHoldInTx } from "./hold-transitions.ts";
import { applyDirectSettlements, markAccountsReconcileRequired } from "./settlement-direct.ts";
import { priceSettlement } from "./settlement-pricing.ts";
import { resolveSupplierAttemptInTx, scalePricingRates } from "./reconciliation.ts";
import type { LeaseManager } from "./lease-manager.ts";
import { postgresState, withPgConcurrencyRetry } from "./funds.ts";

type ReceiptRow = {
  attempt_id: string;
  request_id: string;
  account_id: string;
  channel_id: string;
  currency: string;
  provider_request_id: string;
  evidence_ref: string;
  actor: string;
  verdict: ReconciliationReceipt["verdict"];
  supplier_cost_atoms: string | null;
  customer_usage: Usage | null;
  state: "PENDING" | "APPLIED" | "REVIEW" | "DISMISSED";
};

type UnknownHold = {
  request_id: string;
  state: string;
  supplier_holds: SupplierHoldEntry[];
  supplier_lease_id: string | null;
  wallet_lease_id: string | null;
  held_atoms: string;
};

function identifiesAttempt(
  attempt: SupplierHoldEntry | undefined,
  receipt: ReconciliationReceipt | ReceiptRow,
) {
  return (
    attempt?.account_id === receipt.account_id &&
    attempt.channel_id === receipt.channel_id &&
    attempt.currency === receipt.currency &&
    (!attempt.provider_request_id || attempt.provider_request_id === receipt.provider_request_id)
  );
}

async function settleAuthorizedCustomer(
  tx: SQL,
  hold: UnknownHold,
  attempt: SupplierHoldEntry,
  evidence: ReceiptRow,
  reconcileAccounts: string[],
): Promise<string> {
  if (
    hold.state !== "UNKNOWN" ||
    hold.supplier_lease_id ||
    hold.wallet_lease_id ||
    !attempt.customer_price_version_id ||
    !attempt.customer_price_adjustment ||
    !attempt.cost_version_id ||
    !evidence.customer_usage
  ) {
    return "customer_authorization_unavailable";
  }
  let previouslySettledSupplierJournal: string | null | undefined;
  if (attempt.state === "SETTLED") {
    if (
      attempt.resolution?.action !== "SETTLE" ||
      attempt.resolution.evidence_ref !== evidence.evidence_ref ||
      attempt.resolution.supplier_cost_atoms !== evidence.supplier_cost_atoms
    )
      return "supplier_settlement_mismatch";
    previouslySettledSupplierJournal = null;
    if (BigInt(evidence.supplier_cost_atoms!) > 0n) {
      const [entry] =
        await tx`SELECT journal_id FROM public.ledger_entries WHERE request_id=${hold.request_id}
        AND side='SUPPLIER' AND ordinal=0 AND reference=${"supplier_attempt_resolution:" + attempt.attempt_id}`;
      if (!entry) return "supplier_journal_unavailable";
      previouslySettledSupplierJournal = String(entry.journal_id);
    }
  } else if (attempt.state !== "HELD" && attempt.state !== "UNKNOWN") {
    return "customer_authorization_unavailable";
  }
  const prices = await tx<
    { id: string; side: string; currency: string; basis: PriceBasis; rates: Pricing["rates"] }[]
  >`
    SELECT id, side, currency, basis, rates FROM public.price_versions
    WHERE id = ANY(${[attempt.customer_price_version_id, attempt.cost_version_id]}::uuid[])
  `;
  const customer = prices.find(
    (price) => price.id === attempt.customer_price_version_id && price.side === "CUSTOMER",
  );
  const supplier = prices.find(
    (price) =>
      price.id === attempt.cost_version_id &&
      price.side === "SUPPLIER" &&
      price.currency === attempt.currency,
  );
  if (!customer || !supplier) return "historical_price_unavailable";
  const usage = evidence.customer_usage;
  const adjustment = attempt.customer_price_adjustment;
  if (
    (customer.basis === "PROVIDER_USAGE" &&
      Object.keys(customer.rates).some((metric) => usage[metric as keyof Usage] === undefined)) ||
    (customer.basis === "INPUT_CHARACTERS" && usage.characters === undefined)
  )
    return "incomplete_customer_usage";
  const item: CommitTerminalInput = {
    request_id: hold.request_id,
    selected_attempt_id: attempt.attempt_id,
    channel_id: attempt.channel_id,
    billing_price_id: customer.id,
    pricing_override_id: adjustment.override_id,
    discount_numerator: adjustment.discount_numerator,
    discount_denominator: adjustment.discount_denominator,
    cost_version_id: supplier.id,
    usage,
    billing_pricing: {
      currency: customer.currency,
      rates: scalePricingRates(
        customer.rates,
        adjustment.discount_numerator,
        adjustment.discount_denominator,
      ),
    },
    billing_basis: customer.basis,
    ...(usage.characters === undefined ? {} : { input_characters: usage.characters }),
    supplier_pricing: { currency: supplier.currency, rates: supplier.rates },
    supplier_currency: supplier.currency,
    supplier_cost_atoms_override: evidence.supplier_cost_atoms!,
  };
  try {
    if (priceSettlement(item).customerAtoms > BigInt(hold.held_atoms))
      return "customer_authorization_exceeded";
  } catch {
    return "invalid_customer_usage";
  }
  if (
    hold.supplier_holds.some(
      (entry) => entry.attempt_id !== attempt.attempt_id && entry.state === "HELD",
    )
  ) {
    return "ambiguous_held_sibling";
  }
  const results: CommitTerminalResult[] = [];
  const settled = await applyDirectSettlements(
    tx,
    [
      {
        index: 0,
        item,
        ...(previouslySettledSupplierJournal === undefined
          ? {}
          : { previouslySettledSupplierJournal }),
      },
    ],
    results,
  );
  if (!results[0]?.ok) return "settlement_rejected";
  reconcileAccounts.push(...settled.reconcileAccounts);
  return "settled";
}

async function applyReceipt(
  tx: SQL,
  hold: UnknownHold,
  evidence: ReceiptRow,
  reconcileAccounts: string[],
): Promise<string> {
  const attempt = hold.supplier_holds.find((entry) => entry.attempt_id === evidence.attempt_id);
  if (!attempt || !identifiesAttempt(attempt, evidence)) return "identity_conflict";
  if (evidence.verdict === "FINAL" && evidence.customer_usage && hold.state === "UNKNOWN") {
    return settleAuthorizedCustomer(tx, hold, attempt, evidence, reconcileAccounts);
  }
  const common = {
    request_id: hold.request_id,
    attempt_id: attempt.attempt_id,
    evidence_ref: evidence.evidence_ref,
    actor: evidence.actor,
    reason: "verified supplier receipt",
  };
  const outcome = await resolveSupplierAttemptInTx(
    tx,
    evidence.verdict === "FINAL"
      ? { ...common, action: "SETTLE", supplier_cost_atoms: evidence.supplier_cost_atoms! }
      : { ...common, action: "RELEASE" },
    true,
  );
  if (!outcome.ok) return outcome.code;
  if (
    evidence.verdict === "NOT_EXECUTED" &&
    hold.state === "UNKNOWN" &&
    hold.supplier_holds.every(
      (entry) => entry.attempt_id === attempt.attempt_id || entry.state === "RELEASED",
    )
  ) {
    if (!(await releaseHoldInTx(tx, hold.request_id)).released)
      throw Error("Reconciliation release failed");
    return "released";
  }
  return evidence.verdict === "FINAL" ? "supplier_settled" : "supplier_released";
}

function sameSupplierReceipt(row: ReceiptRow, input: ReconciliationReceipt) {
  return (
    row.request_id === input.request_id &&
    row.attempt_id === input.attempt_id &&
    row.account_id === input.account_id &&
    row.channel_id === input.channel_id &&
    row.currency === input.currency &&
    row.provider_request_id === input.provider_request_id &&
    row.evidence_ref === input.evidence_ref &&
    row.verdict === input.verdict &&
    row.supplier_cost_atoms === (input.verdict === "FINAL" ? input.supplier_cost_atoms : null)
  );
}

export function createEvidenceReconciliation(
  db: SQL,
  notifyDetachedLocal: LeaseManager["notifyDetachedLocal"],
) {
  async function processReceipt() {
    const reconcileAccounts: string[] = [];
    const processed = await withPgConcurrencyRetry(() =>
      db.begin(async (tx) => {
        reconcileAccounts.length = 0;
        const [hold] = await tx<UnknownHold[]>`
        SELECT h.request_id, h.state, h.supplier_holds, h.supplier_lease_id, h.wallet_lease_id,
          h.held_atoms::text
        FROM public.billing_reconciliation_receipts r JOIN public.billing_holds h USING (request_id)
        WHERE r.state='PENDING' AND h.state<>'HELD'
        ORDER BY r.created_at, r.attempt_id LIMIT 1 FOR NO KEY UPDATE OF h SKIP LOCKED
      `;
        if (!hold) return false;
        const [evidence] = await tx<ReceiptRow[]>`
        SELECT *, supplier_cost_atoms::text FROM public.billing_reconciliation_receipts
        WHERE request_id=${hold.request_id} AND state='PENDING'
        ORDER BY created_at, attempt_id LIMIT 1 FOR NO KEY UPDATE
      `;
        if (!evidence) return false;
        const result = await tx
          .savepoint((sp) => applyReceipt(sp, hold, evidence, reconcileAccounts))
          .catch((error: unknown) => {
            const sqlState = postgresState(error);
            if (sqlState !== "22003" && sqlState !== "23514" && sqlState !== "23503") throw error;
            reconcileAccounts.length = 0;
            return "accounting_constraint";
          });
        const applied = ["settled", "released", "supplier_settled", "supplier_released"].includes(
          result,
        );
        await tx`UPDATE public.billing_reconciliation_receipts
        SET state=${applied ? "APPLIED" : "REVIEW"}, result=${result}, reviewed_at=now()
        WHERE attempt_id=${evidence.attempt_id}`;
        return true;
      }),
    );
    await withPgConcurrencyRetry(() => markAccountsReconcileRequired(db, reconcileAccounts));
    return processed;
  }

  async function applyWaiver() {
    const outcome = await db.begin(async (tx) => {
      const [hold] = await tx`
        SELECT h.request_id, h.billing_account_id, h.held_atoms::text, h.supplier_holds->0 AS attempt
        FROM public.billing_holds h
        JOIN public.billing_reconciliation_policies p ON p.billing_account_id=h.billing_account_id
          AND p.channel_id=(h.supplier_holds->0->>'channel_id')::uuid
          AND p.supplier_currency=h.supplier_holds->0->>'currency'
        JOIN public.billing_reconciliation_budgets b ON b.currency=p.supplier_currency
        WHERE h.state='UNKNOWN' AND h.wallet_lease_id IS NULL AND h.supplier_lease_id IS NULL
          AND jsonb_array_length(h.supplier_holds)=1 AND h.supplier_holds->0->>'state' IN ('HELD','UNKNOWN')
          AND p.enabled AND (p.include_historical OR h.created_at>=p.eligible_since)
          AND COALESCE(h.reconciliation_started_at, h.expires_at) <= now() - p.age_seconds * interval '1 second'
          AND h.held_atoms > 0 AND h.held_atoms <= p.max_customer_atoms
          AND (h.supplier_holds->0->>'held_atoms')::numeric <= LEAST(p.limit_atoms-p.used_atoms, b.limit_atoms-b.used_atoms)
          AND NOT EXISTS (SELECT 1 FROM public.billing_reconciliation_receipts r WHERE r.request_id=h.request_id)
        ORDER BY h.created_at, h.request_id LIMIT 1 FOR NO KEY UPDATE OF h SKIP LOCKED
      `;
      if (!hold) return null;
      const attempt = hold.attempt as SupplierHoldEntry;
      const [known] =
        await tx`SELECT 1 FROM public.billing_reconciliation_receipts WHERE request_id=${hold.request_id} LIMIT 1`;
      if (known) return null;
      const [budget] = await tx`SELECT (limit_atoms-used_atoms)::text AS available
        FROM public.billing_reconciliation_budgets WHERE currency=${attempt.currency} FOR NO KEY UPDATE`;
      if (!budget || BigInt(budget.available) < BigInt(attempt.held_atoms)) return null;
      const [policy] = await tx`UPDATE public.billing_reconciliation_policies p
        SET used_atoms=used_atoms+${attempt.held_atoms}::numeric
        FROM public.billing_holds h WHERE h.request_id=${hold.request_id}
          AND p.billing_account_id=h.billing_account_id AND p.channel_id=${attempt.channel_id} AND p.supplier_currency=${attempt.currency}
          AND p.enabled AND (p.include_historical OR h.created_at>=p.eligible_since)
          AND COALESCE(h.reconciliation_started_at, h.expires_at) <= now() - p.age_seconds * interval '1 second'
          AND h.held_atoms <= p.max_customer_atoms AND p.limit_atoms-p.used_atoms >= ${attempt.held_atoms}::numeric
        RETURNING p.billing_account_id`;
      if (!policy) return null;
      await tx`UPDATE public.billing_reconciliation_budgets SET used_atoms=used_atoms+${attempt.held_atoms}::numeric
        WHERE currency=${attempt.currency}`;
      const release = await releaseHoldInTx(tx, hold.request_id, "CUSTOMER");
      if (!release.released) throw Error("Reconciliation waiver release failed");
      await tx`INSERT INTO public.billing_reconciliation_waivers
        (request_id, attempt_id, billing_account_id, channel_id, supplier_currency, supplier_risk_atoms, customer_released_atoms)
        VALUES (${hold.request_id}, ${attempt.attempt_id}, ${hold.billing_account_id}, ${attempt.channel_id}, ${attempt.currency},
          ${attempt.held_atoms}::numeric, ${hold.held_atoms}::numeric)`;
      return { requestId: String(hold.request_id), release };
    });
    if (!outcome) return false;
    notifyHoldRelease(notifyDetachedLocal, outcome.requestId, outcome.release);
    return true;
  }

  return {
    async claimReconciliationPoll(
      scope: { account_id: string; channel_id: string; currency: string },
      intervalMs: number,
      maxIntervalMs = intervalMs,
    ) {
      return db.begin(async (tx) => {
        const [candidate] = await tx<
          {
            request_id: string;
            attempt_id: string;
            provider_request_id: string;
          }[]
        >`
          SELECT h.request_id, (a->>'attempt_id')::uuid AS attempt_id, a->>'provider_request_id' AS provider_request_id
          FROM public.billing_holds h
          CROSS JOIN LATERAL jsonb_array_elements(h.supplier_holds) a
          LEFT JOIN public.billing_reconciliation_polls p ON p.attempt_id=(a->>'attempt_id')::uuid
          WHERE (h.state='UNKNOWN' OR h.state IN ('SETTLED','RELEASED')
            AND h.supplier_holds @> '[{"state":"UNKNOWN"}]' AND a->>'state'='UNKNOWN')
            AND a->>'state' IN ('HELD','UNKNOWN') AND h.supplier_lease_id IS NULL
            AND a->>'account_id'=${scope.account_id} AND a->>'channel_id'=${scope.channel_id}
            AND a->>'currency'=${scope.currency} AND a->>'provider_request_id' IS NOT NULL
            AND (p.next_at IS NULL OR p.next_at<=now())
            AND NOT EXISTS (SELECT 1 FROM public.billing_reconciliation_receipts r
              WHERE r.attempt_id=(a->>'attempt_id')::uuid)
          ORDER BY p.next_at NULLS FIRST, h.created_at, h.request_id
          LIMIT 1 FOR NO KEY UPDATE OF h SKIP LOCKED
        `;
        if (!candidate) return null;
        const [scheduled] =
          await tx`INSERT INTO public.billing_reconciliation_polls (attempt_id, request_id, next_at)
          VALUES (${candidate.attempt_id}, ${candidate.request_id}, now()+${intervalMs} * interval '1 millisecond')
          ON CONFLICT (attempt_id) DO UPDATE SET
            next_at=now() + LEAST(${maxIntervalMs}::bigint,
              ${intervalMs}::bigint * (1::bigint << LEAST(public.billing_reconciliation_polls.poll_count, 30))) * interval '1 millisecond',
            poll_count=LEAST(public.billing_reconciliation_polls.poll_count + 1, 31)
          WHERE public.billing_reconciliation_polls.next_at<=now() RETURNING attempt_id`;
        return scheduled ? candidate : null;
      });
    },

    async submitReconciliationReceipt(input: ReconciliationReceipt) {
      return db.begin(async (tx) => {
        const [hold] = await tx`SELECT supplier_holds, state FROM public.billing_holds
          WHERE request_id=${input.request_id} FOR NO KEY UPDATE`;
        const attempt = (hold?.supplier_holds as SupplierHoldEntry[] | undefined)?.find(
          (entry) => entry.attempt_id === input.attempt_id,
        );
        if (!attempt || !identifiesAttempt(attempt, input))
          return { status: "identity_conflict" as const };
        if (!attempt.provider_request_id && !input.match_reason)
          return { status: "match_evidence_required" as const };
        const [created] = await tx`INSERT INTO public.billing_reconciliation_receipts
          (attempt_id, request_id, account_id, channel_id, currency, provider_request_id, evidence_ref, match_reason,
            actor, verdict, supplier_cost_atoms, customer_usage, customer_actor)
          VALUES (${input.attempt_id}, ${input.request_id}, ${input.account_id}, ${input.channel_id}, ${input.currency},
            ${input.provider_request_id}, ${input.evidence_ref}, ${input.match_reason ?? null}, ${input.actor}, ${input.verdict},
            ${input.verdict === "FINAL" ? input.supplier_cost_atoms : null}::numeric,
            ${input.verdict === "FINAL" ? (input.customer_usage ?? null) : null}::jsonb,
            ${input.verdict === "FINAL" && input.customer_usage !== undefined ? input.actor : null})
          ON CONFLICT DO NOTHING RETURNING attempt_id`;
        if (created) return { status: "accepted" as const };
        const existing = await tx<
          ReceiptRow[]
        >`SELECT *, supplier_cost_atoms::text FROM public.billing_reconciliation_receipts
          WHERE attempt_id=${input.attempt_id} OR (account_id=${input.account_id}
            AND (provider_request_id=${input.provider_request_id} OR evidence_ref=${input.evidence_ref}))`;
        if (existing.length === 1 && sameSupplierReceipt(existing[0]!, input)) {
          const original = existing[0]!;
          const usage = input.verdict === "FINAL" ? input.customer_usage : undefined;
          if (usage === undefined || canonical(original.customer_usage) === canonical(usage))
            return { status: "replayed" as const, state: original.state };
          if (
            original.customer_usage === null &&
            original.state !== "REVIEW" &&
            hold?.state === "UNKNOWN"
          ) {
            await tx`UPDATE public.billing_reconciliation_receipts SET customer_usage=${usage}::jsonb,
              customer_actor=${input.actor}, state='PENDING', result=NULL, reviewed_at=NULL WHERE attempt_id=${input.attempt_id}`;
            return { status: "accepted" as const };
          }
        }
        if (existing.length)
          await tx`UPDATE public.billing_reconciliation_receipts SET state='REVIEW',
          result='conflicting_receipt', reviewed_at=now()
          WHERE attempt_id=ANY(${existing.map((row) => row.attempt_id)}::uuid[]) AND state='PENDING'`;
        return { status: "receipt_conflict" as const };
      });
    },

    async processReconciliation(limit: number) {
      let receipts = 0;
      let waivers = 0;
      while (receipts < limit && (await processReceipt())) receipts++;
      while (waivers < limit && (await applyWaiver())) waivers++;
      return { receipts, waivers };
    },

    async reviewReconciliationReceipt(input: {
      attempt_id: string;
      action: "RESUME" | "DISMISS";
      review_evidence_ref: string;
      reason: string;
      actor: string;
      customer_usage?: Usage | null | undefined;
    }) {
      return db.begin(async (tx) => {
        const [hold] = await tx`SELECT h.request_id FROM public.billing_reconciliation_receipts r
          JOIN public.billing_holds h USING (request_id) WHERE r.attempt_id=${input.attempt_id}
          FOR NO KEY UPDATE OF h`;
        if (!hold) return null;
        const [result] = await tx`UPDATE public.billing_reconciliation_receipts
          SET state=${input.action === "RESUME" ? "PENDING" : "DISMISSED"},
            review_history=review_history || jsonb_build_array(jsonb_build_object(
              'actor', ${input.actor}::text, 'action', ${input.action}::text,
              'reference', ${input.review_evidence_ref}::text, 'reason', ${input.reason}::text,
              'previous_result', result, 'previous_customer_usage', customer_usage,
              'previous_customer_actor', customer_actor, 'at', now())),
            customer_usage=CASE WHEN ${input.customer_usage !== undefined} THEN ${input.customer_usage ?? null}::jsonb ELSE customer_usage END,
            customer_actor=CASE WHEN ${input.customer_usage === null} THEN NULL WHEN ${input.customer_usage !== undefined} THEN ${input.actor} ELSE customer_actor END,
            reviewed_at=now(), result=NULL
          WHERE attempt_id=${input.attempt_id} AND state='REVIEW'
            AND (${input.customer_usage === undefined || input.customer_usage === null} OR verdict='FINAL')
          RETURNING attempt_id, state`;
        return result ?? null;
      });
    },

    async putReconciliationBudget(currency: string, limitAtoms: string) {
      const [result] =
        await db`INSERT INTO public.billing_reconciliation_budgets (currency, limit_atoms)
        VALUES (${currency}, ${limitAtoms}::numeric)
        ON CONFLICT (currency) DO UPDATE SET limit_atoms=EXCLUDED.limit_atoms
        WHERE public.billing_reconciliation_budgets.used_atoms<=EXCLUDED.limit_atoms
        RETURNING currency, limit_atoms::text, used_atoms::text`;
      return result ?? null;
    },

    async putReconciliationPolicy(input: ReconciliationPolicy) {
      const [result] = await db`INSERT INTO public.billing_reconciliation_policies
        (billing_account_id, channel_id, supplier_currency, enabled, include_historical, age_seconds, max_customer_atoms, limit_atoms)
        SELECT ${input.billing_account_id}::uuid, ${input.channel_id}::uuid, b.currency, ${input.enabled},
          ${input.include_historical}, ${input.age_seconds}, ${input.max_customer_atoms}::numeric, ${input.limit_atoms}::numeric
        FROM public.billing_reconciliation_budgets b WHERE b.currency=${input.supplier_currency}
        ON CONFLICT (billing_account_id, channel_id, supplier_currency) DO UPDATE SET
          eligible_since=CASE WHEN EXCLUDED.enabled AND NOT public.billing_reconciliation_policies.enabled
            THEN now() ELSE public.billing_reconciliation_policies.eligible_since END,
          enabled=EXCLUDED.enabled, include_historical=EXCLUDED.include_historical, age_seconds=EXCLUDED.age_seconds,
          max_customer_atoms=EXCLUDED.max_customer_atoms, limit_atoms=EXCLUDED.limit_atoms
        WHERE public.billing_reconciliation_policies.used_atoms<=EXCLUDED.limit_atoms
        RETURNING billing_account_id, channel_id, supplier_currency, enabled, include_historical,
          eligible_since, age_seconds, max_customer_atoms::text, limit_atoms::text, used_atoms::text`;
      return result ?? null;
    },

    listReconciliationCases(limit: number, days: number) {
      return db<
        {
          request_id: string;
          billing_account_id: string;
          model: string;
          operation: string;
          state: string;
          created_at: Date;
          held_atoms: string;
          supplier_holds: SupplierHoldEntry[];
          waived_at: Date | null;
          receipts: {
            attempt_id: string;
            state: string;
            result: string | null;
            evidence_ref: string;
          }[];
        }[]
      >`SELECT h.request_id, h.billing_account_id, h.model, h.operation, h.state, h.created_at,
        h.held_atoms::text, h.supplier_holds, w.created_at AS waived_at,
        COALESCE((SELECT jsonb_agg(jsonb_build_object('attempt_id', r.attempt_id, 'state', r.state,
          'result', r.result, 'evidence_ref', r.evidence_ref)) FROM public.billing_reconciliation_receipts r WHERE r.request_id=h.request_id), '[]') AS receipts
        FROM public.billing_holds h LEFT JOIN public.billing_reconciliation_waivers w USING (request_id)
        WHERE (h.state='UNKNOWN' OR (h.state IN ('SETTLED','RELEASED') AND h.supplier_holds @> '[{"state":"UNKNOWN"}]')
          OR EXISTS (SELECT 1 FROM public.billing_reconciliation_receipts r WHERE r.request_id=h.request_id AND r.state='REVIEW'))
          AND (h.created_at <= now()-${days} * interval '1 day')
        ORDER BY h.created_at, h.request_id LIMIT ${limit}`;
    },
  };
}
