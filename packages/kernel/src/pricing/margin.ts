import { ceilRational, multiplyRational, parseRational } from "./rational.ts";
import type { MarginEvaluation, MarginPolicy, Rational } from "./types.ts";

export function evaluateMargin(input: {
  customerCurrency: string;
  customerAtoms: bigint;
  supplierCurrency: string;
  supplierAtoms: bigint;
  supplierToCustomerFx?: Rational | null;
  policy?: MarginPolicy;
}): MarginEvaluation {
  const { customerCurrency, customerAtoms, supplierCurrency, supplierAtoms, policy } = input;
  if (customerAtoms < 0n || supplierAtoms < 0n) {
    throw new RangeError("Margin evaluation requires nonnegative customer and supplier atoms");
  }
  let normalizedSupplierAtoms: bigint | null = null;
  if (customerCurrency === supplierCurrency) {
    normalizedSupplierAtoms = supplierAtoms;
  } else if (input.supplierToCustomerFx) {
    const fx = parseRational(input.supplierToCustomerFx);
    if (fx.numerator > 0n) {
      normalizedSupplierAtoms = ceilRational(
        multiplyRational({ numerator: supplierAtoms, denominator: 1n }, fx),
      );
    }
  }

  if (normalizedSupplierAtoms === null) {
    const requiresMarginCheck =
      policy?.allow_negative_margin === false ||
      policy?.min_margin_bps !== undefined ||
      policy?.max_loss_atoms !== undefined;
    return {
      comparable: false,
      customer_currency: customerCurrency,
      supplier_currency: supplierCurrency,
      customer_atoms: customerAtoms,
      supplier_atoms: supplierAtoms,
      normalized_supplier_atoms: null,
      gross_margin_atoms: null,
      margin_bps: null,
      is_negative_margin: null,
      allowed: !requiresMarginCheck,
      reason: requiresMarginCheck ? "fx_rate_unavailable_for_margin_guard" : null,
    };
  }

  const grossMargin = customerAtoms - normalizedSupplierAtoms;
  const isNegative = grossMargin < 0n;
  const marginBps: bigint | null =
    customerAtoms > 0n
      ? grossMargin >= 0n
        ? (grossMargin * 10_000n) / customerAtoms
        : -((-grossMargin * 10_000n + customerAtoms - 1n) / customerAtoms)
      : normalizedSupplierAtoms === 0n
        ? 0n
        : null;

  let allowed = true;
  let reason: string | null = null;
  if (policy) {
    if (policy.allow_negative_margin === false && isNegative) {
      allowed = false;
      reason = "negative_margin_blocked";
    } else if (
      policy.max_loss_atoms !== undefined &&
      isNegative &&
      -grossMargin > policy.max_loss_atoms
    ) {
      allowed = false;
      reason = "loss_exceeds_max_loss_atoms";
    } else if (
      policy.min_margin_bps !== undefined &&
      (marginBps === null || marginBps < policy.min_margin_bps)
    ) {
      allowed = false;
      reason = "margin_below_min_bps";
    }
  }

  return {
    comparable: true,
    customer_currency: customerCurrency,
    supplier_currency: supplierCurrency,
    customer_atoms: customerAtoms,
    supplier_atoms: supplierAtoms,
    normalized_supplier_atoms: normalizedSupplierAtoms,
    gross_margin_atoms: grossMargin,
    margin_bps: marginBps,
    is_negative_margin: isNegative,
    allowed,
    reason,
  };
}
