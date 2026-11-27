import { DomainError } from "./errors.ts";
import { calculateCost } from "./cost.ts";
import type { BillingQuote } from "./values.ts";
import type { Pricing, Usage } from "./values.ts";
export interface ReservationPrice {
  provider_id: string;
  id: string;
  currency: string;
  rates: Pricing["rates"];
  ceilings: Usage;
  basis: "PROVIDER_USAGE" | "INPUT_CHARACTERS" | "REQUEST_COMPLETION";
}

export type ReservationMeasurement = {
  inputCharacters?: string | undefined;
  estimatedInputTokens?: number | undefined;
  images?: unknown;
};

const textInputMetrics = [
  "input_tokens",
  "cached_input_tokens",
  "cache_write_tokens",
  "cache_write_5m_tokens",
  "cache_write_1h_tokens",
] as const;

type TextInputMetric = (typeof textInputMetrics)[number];

function quoteTextInput(
  row: ReservationPrice,
  estimatedInputTokens: number | undefined,
  usage: Usage,
): void {
  if (
    row.basis !== "PROVIDER_USAGE" ||
    estimatedInputTokens === undefined ||
    !Number.isSafeInteger(estimatedInputTokens) ||
    estimatedInputTokens <= 0 ||
    !row.rates.input_tokens
  )
    return;
  const est = BigInt(estimatedInputTokens);
  if (est > BigInt(usage.input_tokens ?? "0")) return;
  let maxMetric: TextInputMetric = "input_tokens";
  let maxRate = row.rates.input_tokens;
  for (let i = 1; i < textInputMetrics.length; i++) {
    const metric = textInputMetrics[i]!;
    const rate = row.rates[metric];
    if (!rate) continue;
    if (
      BigInt(rate.numerator) * BigInt(maxRate.denominator) >
      BigInt(maxRate.numerator) * BigInt(rate.denominator)
    ) {
      maxMetric = metric;
      maxRate = rate;
    }
  }
  if (est > BigInt(usage[maxMetric] ?? "0")) return;
  for (const metric of textInputMetrics) {
    delete usage[metric];
  }
  usage[maxMetric] = String(estimatedInputTokens);
}

export function quoteReservation(
  rows: ReservationPrice[],
  currency: string,
  measurement: ReservationMeasurement,
  outputBound: number,
): BillingQuote {
  if (!rows.length)
    throw new DomainError(
      "tariff_unavailable",
      "No enabled tariff exists for this model, operation and currency",
    );
  const needsCharacters = rows.some(
    (row) => row.basis === "INPUT_CHARACTERS" && row.rates.characters,
  );
  const inputCharacters = needsCharacters ? measurement.inputCharacters : "0";
  const prices: Record<string, string> = {};
  let amount = 0n;
  for (const row of rows) {
    const usage: Usage = { ...row.ceilings };
    quoteTextInput(row, measurement.estimatedInputTokens, usage);
    if (row.rates.requests) {
      if (BigInt(usage.requests ?? "0") < 1n)
        throw new DomainError(
          "invalid_tariff",
          "Request tariff requires a positive reservation ceiling",
        );
      usage.requests = "1";
    }
    if (row.rates.output_tokens && outputBound > 0) {
      if (BigInt(outputBound) > BigInt(usage.output_tokens ?? "0")) continue;
      usage.output_tokens = String(outputBound);
    }
    if (row.basis === "INPUT_CHARACTERS" && row.rates.characters) {
      if (
        inputCharacters === undefined ||
        BigInt(inputCharacters) > BigInt(usage.characters ?? "0")
      )
        continue;
      usage.characters = String(inputCharacters);
    }
    const images = measurement.images;
    if (row.rates.images && images !== undefined) {
      const n = images;
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
  if (!Object.keys(prices).length) {
    if (inputCharacters === undefined)
      throw new DomainError("invalid_request", "Input character count cannot be determined");
    throw new DomainError(
      "billing_ceiling",
      "Request exceeds the configured billing reservation ceilings",
    );
  }
  return {
    currency,
    amount_atoms: amount.toString(),
    input_characters: String(inputCharacters ?? 0),
    prices,
  };
}
