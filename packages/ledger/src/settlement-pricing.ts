import {
  calculateCompiledCostAtoms,
  compilePricing,
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
): Usage {
  if (basis === "REQUEST_COMPLETION") return { requests: "1" };
  if (basis === "INPUT_CHARACTERS")
    return { characters: item.input_characters ?? "0", requests: "1" };
  return item.usage;
}

export function priceSettlement(item: CommitTerminalInput): {
  customerAtoms: bigint;
  supplierAtoms: bigint;
} {
  const customerAtoms = calculateCompiledCostAtoms(
    usageForSettlement(item, item.billing_basis),
    getCompiledPricing(item.billing_pricing),
  );
  if (item.supplier_cost_atoms_override === undefined) {
    return {
      customerAtoms,
      supplierAtoms: calculateCompiledCostAtoms(
        usageForSettlement(item, item.supplier_basis),
        getCompiledPricing(item.supplier_pricing),
      ),
    };
  }

  const supplierAtoms = parseAtoms(item.supplier_cost_atoms_override);
  if (supplierAtoms === null) throw new Error("Invalid supplier_cost_atoms_override");
  return { customerAtoms, supplierAtoms };
}
