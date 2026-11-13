import type { SQL } from "bun";
import { GatewayError } from "@gateway/contracts";
import { quoteReservation, DomainError } from "@gateway/kernel";
import type { BillingQuote, Operation, Pricing, Usage } from "@gateway/kernel";

export class FundsStore {
  private prices = new Map<string, { until: number; rows: PriceRow[] }>();
  private readonly priceLoads = new Map<string, Promise<PriceRow[]>>();
  constructor(private readonly db: SQL) {}
  async quote(
    model: string,
    operation: Operation,
    currency: string,
    providers: string[],
    measurement: { inputCharacters?: string | undefined; images?: unknown },
    outputBound: number,
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
    try {
      return quoteReservation(rows, currency, measurement, outputBound);
    } catch (error) {
      if (error instanceof DomainError)
        throw new GatewayError(
          ["tariff_unavailable", "invalid_tariff"].includes(error.code) ? 503 : 422,
          error.code,
          error.message,
        );
      throw error;
    }
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
