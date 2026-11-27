import {
  cacheWritePartitionMatches,
  calculateCompiledCostAtoms,
  compilePricing,
  DomainError,
  hasPricedTokenUsage,
  type CompiledPricing,
  type Pricing,
  type Usage,
} from "@gateway/kernel";
import { parseAtoms } from "./funds.ts";
import type { CommitTerminalInput } from "./types.ts";

const compiledPricingCache = new WeakMap<Pricing, CompiledPricing>();

function getCompiledPricing(pricing: Pricing): CompiledPricing {
  let compiled = compiledPricingCache.get(pricing);
  if (!compiled) {
    compiled = compilePricing(pricing);
    compiledPricingCache.set(pricing, compiled);
  }
  return compiled;
}

function usageForSettlement(
  item: CommitTerminalInput,
  basis: CommitTerminalInput["supplier_basis"],
  pricing: Pricing,
): Usage {
  if (basis === "REQUEST_COMPLETION") return { requests: "1" };
  if (basis === "INPUT_CHARACTERS")
    return { characters: item.input_characters ?? "0", requests: "1" };
  if (!hasPricedTokenUsage(item.usage, pricing.rates)) {
    throw new DomainError(
      "invalid_upstream_usage",
      "Upstream omitted token usage required by pricing",
    );
  }
  return item.usage;
}

function calculateSettlementCost(usage: Usage, pricing: Pricing): bigint {
  const total = usage.cache_write_tokens;
  const write5m = usage.cache_write_5m_tokens;
  const write1h = usage.cache_write_1h_tokens;
  const cacheWritesReported = total !== undefined || write5m !== undefined || write1h !== undefined;
  const noCacheWrites = total === "0" && (write5m ?? "0") === "0" && (write1h ?? "0") === "0";
  const totalRate = pricing.rates.cache_write_tokens;
  const rate5m = pricing.rates.cache_write_5m_tokens;
  const rate1h = pricing.rates.cache_write_1h_tokens;
  const needsTotal = totalRate !== undefined && totalRate.numerator !== "0" && total === undefined;
  if (
    cacheWritesReported &&
    !noCacheWrites &&
    ((needsTotal && (write5m === undefined || write1h === undefined)) ||
      (rate5m && rate5m.numerator !== "0" && write5m === undefined) ||
      (rate1h && rate1h.numerator !== "0" && write1h === undefined))
  ) {
    throw new DomainError(
      "invalid_upstream_usage",
      "Upstream omitted cache write usage required by pricing",
    );
  }
  const pricedUsage =
    needsTotal && write5m !== undefined && write1h !== undefined
      ? { ...usage, cache_write_tokens: (BigInt(write5m) + BigInt(write1h)).toString() }
      : usage;
  return calculateCompiledCostAtoms(pricedUsage, getCompiledPricing(pricing));
}

export function priceSettlement(item: CommitTerminalInput): {
  customerAtoms: bigint;
  supplierAtoms: bigint;
} {
  const { cache_write_tokens, cache_write_5m_tokens, cache_write_1h_tokens } = item.usage;
  if (
    !cacheWritePartitionMatches(cache_write_tokens, cache_write_5m_tokens, cache_write_1h_tokens)
  ) {
    throw new DomainError("usage_alias_conflict", "Provider cache write partitions disagree");
  }
  const customerAtoms = calculateSettlementCost(
    usageForSettlement(item, item.billing_basis, item.billing_pricing),
    item.billing_pricing,
  );
  if (item.supplier_cost_atoms_override === undefined) {
    return {
      customerAtoms,
      supplierAtoms: calculateSettlementCost(
        usageForSettlement(item, item.supplier_basis, item.supplier_pricing),
        item.supplier_pricing,
      ),
    };
  }

  const supplierAtoms = parseAtoms(item.supplier_cost_atoms_override);
  if (supplierAtoms === null) throw new Error("Invalid supplier_cost_atoms_override");
  return { customerAtoms, supplierAtoms };
}
