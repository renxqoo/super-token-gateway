import type { SQL } from "bun";
import {
  GatewayError,
  calculateCost,
  quoteReservation,
  creditSchema,
  tariffSchema,
  refundSchema,
  adjustmentSchema,
  resolutionSchema,
  canonical,
  sha256,
} from "@gateway/kernel";
import type {
  BillingQuote,
  CreditInput,
  TariffInput,
  RefundInput,
  AdjustmentInput,
  ResolutionInput,
  Operation,
  Pricing,
  Usage,
} from "@gateway/kernel";

export function fundingError(error: unknown): never {
  const failure = error as { code?: string; errno?: string; message?: string };
  const code = failure.errno ?? failure.code;
  const statuses: Record<string, number> = {
    P0401: 401,
    P0402: 402,
    P0403: 403,
    P0404: 404,
    P0409: 409,
    P0422: 422,
  };
  if (code && statuses[code])
    throw new GatewayError(
      statuses[code],
      code === "P0402"
        ? "insufficient_funds"
        : code === "P0409"
          ? "billing_conflict"
          : code === "P0404"
            ? "not_found"
            : "billing_rejected",
      code === "P0402"
        ? "Wallet has insufficient eligible funds or is frozen"
        : code === "P0409"
          ? "Idempotent monetary command conflicts with its original payload"
          : "Billing command failed validation",
    );
  throw error;
}
export class FundsStore {
  private prices = new Map<string, { until: number; rows: PriceRow[] }>();
  private readonly priceLoads = new Map<string, Promise<PriceRow[]>>();
  constructor(readonly db: SQL) {}
  async credit(projectId: string, value: CreditInput): Promise<Record<string, unknown>> {
    const input = creditSchema.parse(value);
    const models = [...new Set(input.models)].sort();
    const operations = [...new Set(input.operations)].sort();
    try {
      const rows = await this
        .db`SELECT credit_wallet(${projectId}::uuid,${input.currency},${input.external_ref},${input.amount_atoms}::numeric,${input.kind},${input.priority},${input.expires_at ? new Date(input.expires_at).toISOString() : null}::timestamptz,${models}::text[],${operations}::text[]) AS result`;
      return rows[0]?.result;
    } catch (error) {
      fundingError(error);
    }
  }
  async wallets(projectId: string): Promise<unknown[]> {
    return this
      .db`SELECT w.id,w.project_id,w.currency,w.policy,w.available_atoms::text,w.reserved_atoms::text,w.spent_atoms::text,w.expired_atoms::text,w.debt_atoms::text,CASE WHEN dispute.active THEN 'EVIDENCE_CONFLICT' ELSE w.freeze_reason END AS freeze_reason,w.manual_frozen,w.evidence_disputed OR dispute.active AS evidence_disputed FROM wallets w CROSS JOIN LATERAL(SELECT EXISTS(SELECT 1 FROM wallet_disputes d WHERE d.wallet_id=w.id AND NOT EXISTS(SELECT 1 FROM billing_resolutions r WHERE r.request_id=d.request_id)) AS active) dispute WHERE w.project_id=${projectId} ORDER BY currency`;
  }
  async lots(projectId: string, currency: string): Promise<unknown[]> {
    return this
      .db`SELECT l.id,l.kind,l.priority,l.expires_at,l.models,l.operations,l.issued_atoms::text,l.available_atoms::text,l.reserved_atoms::text,l.spent_atoms::text,l.expired_atoms::text FROM wallet_lots l JOIN wallets w ON w.id=l.wallet_id WHERE w.project_id=${projectId} AND w.currency=${currency} ORDER BY l.created_at DESC,l.id LIMIT 1000`;
  }
  async journal(projectId: string, currency: string, before?: string): Promise<unknown[]> {
    return this
      .db`SELECT j.id,j.kind,j.currency,j.reference,j.lines,j.created_at FROM money_journal j JOIN wallets w ON w.id=j.wallet_id WHERE w.project_id=${projectId} AND w.currency=${currency} AND (${before ?? null}::uuid IS NULL OR (j.created_at,j.id)<(SELECT created_at,id FROM money_journal WHERE id=${before ?? null}::uuid AND wallet_id=w.id)) ORDER BY j.created_at DESC,j.id DESC LIMIT 100`;
  }
  async configure(
    projectId: string,
    currency: string,
    patch: { policy?: string | undefined; frozen?: boolean | undefined },
  ): Promise<void> {
    const rows = await this
      .db`UPDATE wallets SET policy=coalesce(${patch.policy ?? null},policy),manual_frozen=coalesce(${patch.frozen ?? null},manual_frozen),freeze_reason=CASE WHEN evidence_disputed THEN 'EVIDENCE_CONFLICT' WHEN coalesce(${patch.frozen ?? null},manual_frozen) THEN 'MANUAL' WHEN debt_atoms>0 THEN 'DEBT' ELSE NULL END WHERE project_id=${projectId} AND currency=${currency} RETURNING id`;
    if (!rows.length) throw new GatewayError(404, "not_found", "Wallet not found");
  }
  async setTariff(value: TariffInput): Promise<{ id: string; pricing_id: string }> {
    const input = tariffSchema.parse(value);
    const pricing = crypto.randomUUID();
    const tariff = crypto.randomUUID();
    const result = await this.db.begin(async (tx) => {
      await tx`INSERT INTO billing_prices(id,currency,rates,ceilings,basis) VALUES(${pricing},${input.pricing.currency},${input.pricing.rates},${input.ceilings},${input.basis})`;
      const rows =
        await tx`INSERT INTO billing_tariffs(id,model,operation,provider_id,currency,pricing_id) VALUES(${tariff},${input.model},${input.operation},${input.provider_id ?? null},${input.pricing.currency},${pricing}) ON CONFLICT(model,operation,provider_id,currency) DO UPDATE SET pricing_id=excluded.pricing_id,enabled=true RETURNING id`;
      return { id: String(rows[0]?.id), pricing_id: pricing };
    });
    this.prices.clear();
    this.priceLoads.clear();
    return result;
  }
  async tariffs(model?: string): Promise<unknown[]> {
    return this
      .db`SELECT t.*,p.rates,p.ceilings,p.basis FROM billing_tariffs t JOIN billing_prices p ON p.id=t.pricing_id WHERE (${model ?? null}::text IS NULL OR model=${model ?? null}) ORDER BY model,operation,currency,provider_id LIMIT 2000`;
  }
  async quote(
    model: string,
    operation: Operation,
    currency: string,
    providers: string[],
    body: Record<string, unknown>,
    outputBound: number,
    batchImages?: number,
  ): Promise<BillingQuote> {
    const cacheKey = `${model}:${operation}:${currency}:${[...providers].sort().join(",")}`;
    const cached = this.prices.get(cacheKey);
    let rows = cached && cached.until >= Date.now() ? cached.rows : undefined;
    if (!rows) {
      let pending = this.priceLoads.get(cacheKey);
      if (!pending) {
        let loading!: Promise<PriceRow[]>;
        loading = (async () => {
          const loaded = await this.db<
            PriceRow[]
          >`SELECT DISTINCT ON(provider.id) provider.id AS provider_id,p.id,p.currency,p.rates,p.ceilings,p.basis FROM unnest(${providers}::uuid[]) provider(id) JOIN billing_tariffs t ON t.model=${model} AND t.operation=${operation} AND t.currency=${currency} AND t.enabled AND (t.provider_id IS NULL OR t.provider_id=provider.id) JOIN billing_prices p ON p.id=t.pricing_id ORDER BY provider.id,(t.provider_id IS NOT NULL) DESC`;
          if (this.priceLoads.get(cacheKey) === loading) {
            if (this.prices.size > 10000) this.prices.clear();
            this.prices.set(cacheKey, { until: Date.now() + 1000, rows: loaded });
          }
          return loaded;
        })().finally(() => {
          if (this.priceLoads.get(cacheKey) === loading) this.priceLoads.delete(cacheKey);
        });
        this.priceLoads.set(cacheKey, loading);
        pending = loading;
      }
      rows = await pending;
    }
    return quoteReservation(rows, currency, operation, body, outputBound, batchImages);
  }

  async refund(chargeId: string, value: RefundInput): Promise<Record<string, unknown>> {
    const input = refundSchema.parse(value);
    try {
      const rows = await this
        .db`SELECT refund_billing_charge(${chargeId}::uuid,${input.amount_atoms}::numeric,${input.external_ref},${input.reason}) AS result`;
      return rows[0]?.result;
    } catch (error) {
      fundingError(error);
    }
  }
  async adjust(chargeId: string, value: AdjustmentInput): Promise<Record<string, unknown>> {
    const input = adjustmentSchema.parse(value);
    try {
      const rows = await this
        .db`SELECT adjust_billing_charge(${chargeId}::uuid,${input.target_atoms}::numeric,${input.external_ref},${input.reason}) AS result`;
      return rows[0]?.result;
    } catch (error) {
      fundingError(error);
    }
  }
  async receipt(requestId: string, projectId: string): Promise<Record<string, unknown>> {
    const rows = await this
      .db`SELECT q.id,q.billing_currency,q.billing_outcome,h.state AS hold_state,h.amount_atoms::text AS reserved_atoms,c.id AS charge_id,c.amount_atoms::text AS charged_atoms,c.funded_atoms::text,c.debt_atoms::text,c.refunded_atoms::text,c.pricing_id,c.evidence_id FROM gateway_requests q LEFT JOIN billing_holds h ON h.request_id=q.id LEFT JOIN billing_charges c ON c.request_id=q.id WHERE q.id=${requestId} AND q.project_id=${projectId}`;
    if (!rows[0]) throw new GatewayError(404, "not_found", "Billing request not found");
    return rows[0];
  }
  async reconcileKnownHolds(limit = 32): Promise<number> {
    const token = crypto.randomUUID();
    const rows = await this.db<{ request_id: string }[]>`WITH candidates AS (
      SELECT h.id,h.next_reconcile_at FROM billing_holds h
      WHERE h.state IN ('HELD','RECONCILING') AND h.next_reconcile_at<=now() AND (h.reconcile_until IS NULL OR h.reconcile_until<now())
      AND (EXISTS(SELECT 1 FROM attempts a WHERE a.request_id=h.request_id AND a.evidence_id IS NOT NULL) OR EXISTS(SELECT 1 FROM billing_resolutions r WHERE r.request_id=h.request_id))
      UNION ALL
      SELECT h.id,h.next_reconcile_at FROM billing_charges c JOIN billing_holds h ON h.id=c.hold_id
      WHERE c.debt_atoms>0 AND h.state NOT IN ('HELD','RECONCILING') AND h.next_reconcile_at<=now() AND (h.reconcile_until IS NULL OR h.reconcile_until<now())
    ),due AS (
      SELECT h.id FROM candidates c JOIN billing_holds h ON h.id=c.id WHERE h.reconcile_until IS NULL OR h.reconcile_until<now()
      ORDER BY c.next_reconcile_at,h.id LIMIT ${limit} FOR UPDATE OF h SKIP LOCKED
    ) UPDATE billing_holds h SET reconcile_token=${token},reconcile_until=now()+interval '30 seconds' FROM due WHERE h.id=due.id RETURNING h.request_id`;
    if (!rows.length) return 0;
    const result = await this
      .db`SELECT settle_requests(${rows.map((r) => r.request_id)}::uuid[],${token}::uuid) AS result`;
    return Object.values(result[0]?.result ?? {}).filter(
      (value) => !["STALE_LEASE", "ERROR"].includes((value as { state: string }).state),
    ).length;
  }
  async resolve(requestId: string, value: ResolutionInput): Promise<Record<string, unknown>> {
    const input = resolutionSchema.parse(value);
    const fingerprint = sha256(canonical(input));
    return this.db.begin(async (tx) => {
      const rows =
        await tx`SELECT w.id,w.currency,q.billing_quote FROM wallets w JOIN billing_holds h ON h.wallet_id=w.id JOIN gateway_requests q ON q.id=h.request_id WHERE q.id=${requestId} FOR NO KEY UPDATE OF w`;
      if (!rows[0]) throw new GatewayError(404, "not_found", "Preauthorization not found");
      const old =
        await tx`SELECT id,fingerprint FROM billing_resolutions WHERE request_id=${requestId} AND external_ref=${input.external_ref}`;
      if (old[0]) {
        if (old[0].fingerprint !== fingerprint)
          throw new GatewayError(409, "billing_conflict", "Resolution identity conflicts");
        return { id: old[0].id, replayed: true };
      }
      let price: PriceRow | undefined;
      if (input.action === "SETTLE") {
        const captured = Object.values((rows[0].billing_quote as BillingQuote).prices);
        if (!input.pricing_id || !captured.includes(input.pricing_id))
          throw new GatewayError(
            422,
            "invalid_resolution",
            "Resolution must use an originally captured pricing version",
          );
        const prices = await tx<
          PriceRow[]
        >`SELECT id,currency,rates,ceilings,basis FROM billing_prices WHERE id=${input.pricing_id}`;
        price = prices[0];
        if (!price || price.currency !== rows[0].currency)
          throw new GatewayError(422, "currency_mismatch", "Resolution currency mismatch");
        if (Object.keys(price.rates).some((key) => !Object.hasOwn(input.usage, key)))
          throw new GatewayError(
            422,
            "incomplete_evidence",
            "Resolution requires all priced numeric quantities",
          );
      }
      const id = crypto.randomUUID();
      await tx`INSERT INTO billing_resolutions(id,request_id,external_ref,fingerprint,action,usage,reason,pricing_id) VALUES(${id},${requestId},${input.external_ref},${fingerprint},${input.action},${input.usage},${input.reason},${input.pricing_id ?? null})`;
      const charges = await tx`SELECT id FROM billing_charges WHERE request_id=${requestId}`;
      if (charges[0]) {
        const target = price
          ? calculateCost(input.usage, { currency: price.currency, rates: price.rates })
          : "0";
        await tx`SELECT adjust_billing_charge(${charges[0].id}::uuid,${target}::numeric,${`resolution:${id}`},'PROVIDER_RECONCILIATION')`;
      }
      await tx`UPDATE wallets w SET evidence_disputed=EXISTS(SELECT 1 FROM billing_holds h JOIN attempts a ON a.request_id=h.request_id WHERE h.wallet_id=w.id AND a.metering_conflict AND NOT EXISTS(SELECT 1 FROM billing_resolutions r WHERE r.request_id=h.request_id)) WHERE w.id=${rows[0].id}`;
      await tx`UPDATE wallets SET freeze_reason=CASE WHEN evidence_disputed THEN 'EVIDENCE_CONFLICT' WHEN manual_frozen THEN 'MANUAL' WHEN debt_atoms>0 THEN 'DEBT' ELSE NULL END WHERE id=${rows[0].id}`;
      await tx`UPDATE billing_holds SET next_reconcile_at=now(),reconcile_until=NULL WHERE request_id=${requestId}`;
      return { id, replayed: false };
    });
  }
}
interface PriceRow {
  provider_id: string;
  id: string;
  currency: string;
  rates: Pricing["rates"];
  ceilings: Usage;
  basis: "PROVIDER_USAGE" | "INPUT_CHARACTERS" | "REQUEST_COMPLETION";
}
