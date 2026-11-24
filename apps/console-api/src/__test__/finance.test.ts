import { expect, test } from "bun:test";
import { enrichAdminChargeDecimals } from "../routes/shared.ts";

test("charge details leave gross margin unavailable for revenue and costs in different currencies", () => {
  expect(
    enrichAdminChargeDecimals({
      currency: "USD",
      supplier_currency: "EUR",
      customer_atoms: "10000000000000000000",
      supplier_cost_atoms: "7000000000000000000",
    }),
  ).toMatchObject({
    currency: "USD",
    supplier_currency: "EUR",
    net_revenue_decimal: "10.000000000000000000",
    supplier_cost_decimal: "7.000000000000000000",
    gross_margin_atoms: null,
    gross_margin_decimal: null,
  });
});
