import { dimensions } from "./values.ts";
import type { Pricing, Usage } from "./values.ts";

const integer = /^(0|[1-9][0-9]{0,37})$/;
export function validateUsage(usage: Usage): void {
  for (const [key, value] of Object.entries(usage)) {
    if (![...dimensions, "reasoning_tokens"].includes(key))
      throw new TypeError("Unknown usage dimension");
    const decimal = key === "audio_seconds" || key === "video_seconds";
    if (
      typeof value !== "string" ||
      !(decimal ? /^(0|[1-9][0-9]{0,37})(\.[0-9]{1,18})?$/ : integer).test(value)
    )
      throw new TypeError("Usage counters must be exact nonnegative integers or decimal durations");
  }
  if (
    usage.cache_write_tokens !== undefined &&
    usage.cache_write_5m_tokens !== undefined &&
    usage.cache_write_1h_tokens !== undefined &&
    BigInt(usage.cache_write_tokens) !==
      BigInt(usage.cache_write_5m_tokens) + BigInt(usage.cache_write_1h_tokens)
  )
    throw new TypeError("Provider cache write partitions disagree");
}
export function validatePricing(pricing: Pricing): void {
  if (!/^[A-Z]{3,8}$/.test(pricing.currency)) throw new TypeError("Invalid currency");
  for (const [key, rate] of Object.entries(pricing.rates)) {
    if (
      !(dimensions as readonly string[]).includes(key) ||
      !rate ||
      !integer.test(rate.numerator) ||
      !/^[1-9][0-9]{0,37}$/.test(rate.denominator)
    )
      throw new TypeError("Invalid exact rational rate");
  }
  if (
    pricing.rates.cache_write_tokens &&
    (pricing.rates.cache_write_5m_tokens || pricing.rates.cache_write_1h_tokens)
  )
    throw new TypeError("Choose aggregate or TTL-specific cache write rates");
  for (const prefix of ["audio", "video"] as const)
    if (pricing.rates[`${prefix}_ms`] && pricing.rates[`${prefix}_seconds`])
      throw new TypeError("Choose one duration unit per media type");
}
