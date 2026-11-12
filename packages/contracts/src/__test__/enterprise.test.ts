import { expect, test } from "bun:test";
import {
  opaqueResourceIdSchema,
  resourceCommitSchema,
  usageEvidenceSchema,
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
test("resource commits reject model content, arbitrary metadata and music pseudo resources", () => {
  const data = {
    public_id: "file_123",
    remote_id: "files/stable_123",
    kind: "file",
    project_id: crypto.randomUUID(),
    channel_id: crypto.randomUUID(),
    binding_id: crypto.randomUUID(),
    state: "ACTIVE",
  };
  expect(resourceCommitSchema.safeParse(data).success).toBe(true);
  for (const added of [
    { metadata: { download_url: "https://media.example/?signed=token" } },
    { content: "model output" },
    { kind: "music" },
  ])
    expect(resourceCommitSchema.safeParse({ ...data, ...added }).success).toBe(false);
});
test("music evidence carries integer milliseconds and tariffs require matching ceilings", () => {
  expect(
    usageEvidenceSchema.parse({
      attemptId: crypto.randomUUID(),
      state: "SUCCEEDED",
      finality: "FINAL",
      usage: { music_ms: "125" },
    }).usage,
  ).toEqual({ music_ms: "125" });
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
