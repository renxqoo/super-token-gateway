import { expect, test } from "bun:test";
import { routeSchema, routePatchSchema } from "../public.ts";
const route = {
  provider_id: "00000000-0000-4000-8000-000000000001",
  upstream_model: "synthetic",
  operations: ["chat"],
  pricing: { currency: "USD", rates: { requests: { numerator: "0", denominator: "1" } } },
};
test("rated zero-cost routes cannot omit their explicit reservation ceiling", () => {
  expect(routeSchema.safeParse(route).success).toBe(false);
  expect(routeSchema.safeParse({ ...route, ceilings: {} }).success).toBe(false);
  expect(routeSchema.parse({ ...route, ceilings: { requests: "0" } }).ceilings).toEqual({
    requests: "0",
  });
});
test("supplier price replacement cannot reuse an absent or mismatched previous ceiling", () => {
  expect(routePatchSchema.safeParse({ pricing: route.pricing }).success).toBe(false);
  expect(routePatchSchema.safeParse({ ceilings: { requests: "1" } }).success).toBe(false);
  expect(
    routePatchSchema.safeParse({ pricing: route.pricing, ceilings: { input_tokens: "100" } })
      .success,
  ).toBe(false);
  expect(
    routePatchSchema.parse({ pricing: route.pricing, ceilings: { requests: "1" } }).ceilings,
  ).toEqual({ requests: "1" });
  expect(routePatchSchema.parse({ enabled: false })).toEqual({ enabled: false });
});
test("empty supplier rates and unknown reservation dimensions are rejected at the route boundary", () => {
  expect(
    routeSchema.safeParse({
      ...route,
      pricing: { currency: "USD", rates: {} },
      ceilings: { requests: "1" },
    }).success,
  ).toBe(false);
  expect(
    routeSchema.safeParse({ ...route, ceilings: { requests: "1", unknown: "1" } }).success,
  ).toBe(false);
});
