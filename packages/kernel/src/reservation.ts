import { GatewayError } from "./errors.ts";
import { calculateCost } from "./cost.ts";
import type { BillingQuote } from "./billing.ts";
import type { Pricing, Usage } from "./contracts.ts";
export interface ReservationPrice {
  provider_id: string;
  id: string;
  currency: string;
  rates: Pricing["rates"];
  ceilings: Usage;
  basis: "PROVIDER_USAGE" | "INPUT_CHARACTERS" | "REQUEST_COMPLETION";
}
export function quoteReservation(
  rows: ReservationPrice[],
  currency: string,
  body: Record<string, unknown>,
  outputBound: number,
): BillingQuote {
  if (!rows.length)
    throw new GatewayError(
      503,
      "tariff_unavailable",
      "No enabled tariff exists for this model, operation and currency",
    );
  const inputCharacters = typeof body.input === "string" ? String([...body.input].length) : "0";
  const prices: Record<string, string> = {};
  let amount = 0n;
  for (const row of rows) {
    const usage: Usage = { ...row.ceilings };
    if (row.rates.requests) {
      if (BigInt(usage.requests ?? "0") < 1n)
        throw new GatewayError(
          503,
          "invalid_tariff",
          "Request tariff requires a positive reservation ceiling",
        );
      usage.requests = "1";
    }
    if (row.rates.output_tokens && outputBound > 0) {
      if (BigInt(outputBound) > BigInt(usage.output_tokens ?? "0")) continue;
      usage.output_tokens = String(outputBound);
    }
    if (row.rates.characters) {
      if (BigInt(inputCharacters) > BigInt(usage.characters ?? "0")) continue;
      usage.characters = inputCharacters;
    }
    if (row.rates.images) {
      const n = body.n ?? 1;
      if (
        !Number.isSafeInteger(n) ||
        Number(n) < 1 ||
        BigInt(Number(n)) > BigInt(usage.images ?? "0")
      )
        continue;
      usage.images = String(n);
    }
    const cost = BigInt(calculateCost(usage, { currency: row.currency, rates: row.rates }));
    if (cost > amount) amount = cost;
    prices[row.provider_id] = row.id;
  }
  if (!Object.keys(prices).length)
    throw new GatewayError(
      422,
      "billing_ceiling",
      "Request exceeds the configured billing reservation ceilings",
    );
  return { currency, amount_atoms: amount.toString(), input_characters: inputCharacters, prices };
}
