import { expect, test } from "bun:test";
import {
  opaqueResourceIdSchema,
  usageSchema,
  tariffSchema,
  upstreamCostConfigurationSchema,
} from "../index.ts";

test("resource identities reject URLs, signed queries and traversal", () => {
  for (const id of [
    "https://media.example/video",
    "file?signature=opaque",
    "files/../secret",
    "data:text/plain,body",
  ])
    expect(opaqueResourceIdSchema.safeParse(id).success).toBe(false);
  expect(opaqueResourceIdSchema.parse("files/stable_123")).toBe("files/stable_123");
});
test("music evidence carries integer milliseconds and tariffs require matching ceilings", () => {
  expect(usageSchema.parse({ music_ms: "125" })).toEqual({ music_ms: "125" });
  expect(() =>
    tariffSchema.parse({
      model: "music",
      operation: "music",
      pricing: { currency: "USD", rates: { music_ms: { numerator: "8", denominator: "1000" } } },
      ceilings: {},
    }),
  ).toThrow("explicit reservation ceiling");
});

test("upstream music prices cannot omit their cost ceiling", () => {
  expect(() =>
    upstreamCostConfigurationSchema.parse({
      pricing: { currency: "USD", rates: { music_ms: { numerator: "8", denominator: "1000" } } },
      ceilings: {},
    }),
  ).toThrow("explicit reservation ceiling");
  expect(
    upstreamCostConfigurationSchema.parse({
      pricing: { currency: "USD", rates: { music_ms: { numerator: "8", denominator: "1000" } } },
      ceilings: { music_ms: "125" },
    }).ceilings,
  ).toEqual({ music_ms: "125" });
});
