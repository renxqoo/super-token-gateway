import { expect, test } from "bun:test";
import {
  calculateCompiledCostAtoms,
  calculateCost,
  canonical,
  compilePricing,
  type Pricing,
  type Usage,
} from "@gateway/kernel";
import { MicroBatchQueue } from "../micro-batch.ts";
import { outboxShard, sha256Hex } from "../funds.ts";

test("adaptive MicroBatchQueue flushes immediately when idle and batches concurrent arrivals while busy", async () => {
  const batchSizes: number[] = [];
  let releaseFirst!: () => void;
  const firstBusy = new Promise<void>((r) => {
    releaseFirst = r;
  });
  let calls = 0;
  const queue = new MicroBatchQueue<number, number>(
    async (items) => {
      batchSizes.push(items.length);
      if (++calls === 1) await firstBusy;
      return items.map((x) => x * 2);
    },
    64,
    250,
    1,
  );

  const t0 = performance.now();
  const p1 = queue.enqueue(1);
  await Bun.sleep(5);
  expect(batchSizes).toEqual([1]);
  expect(performance.now() - t0).toBeLessThan(100);

  const queued = [queue.enqueue(2), queue.enqueue(3), queue.enqueue(4)];
  releaseFirst();
  expect(await p1).toBe(2);
  expect(await Promise.all(queued)).toEqual([4, 6, 8]);
  expect(batchSizes).toEqual([1, 3]);
  await queue.drain();
});

test("compilePricing and calculateCompiledCostAtoms match calculateCost across safe, large, fractional, and duration usage", () => {
  const pricingInt: Pricing = {
    currency: "USD",
    rates: {
      input_tokens: { numerator: "10", denominator: "1" },
      cached_input_tokens: { numerator: "5", denominator: "1" },
      output_tokens: { numerator: "20", denominator: "1" },
    },
  };
  const compiledInt = compilePricing(pricingInt);
  const usages: Usage[] = [
    { input_tokens: "7", cached_input_tokens: "3", output_tokens: "5" },
    { input_tokens: "0", output_tokens: "0" },
    { input_tokens: "123456789012345678901", output_tokens: "999999999999999" },
  ];
  for (const u of usages) {
    expect(calculateCompiledCostAtoms(u, compiledInt).toString()).toBe(
      calculateCost(u, pricingInt),
    );
  }

  const pricingFrac: Pricing = {
    currency: "USD",
    rates: {
      input_tokens: { numerator: "1", denominator: "3" },
      output_tokens: { numerator: "2", denominator: "3" },
      audio_seconds: { numerator: "7", denominator: "2" },
    },
  };
  const compiledFrac = compilePricing(pricingFrac);
  const uFrac: Usage = { input_tokens: "1", output_tokens: "1", audio_seconds: "1.25" };
  expect(calculateCompiledCostAtoms(uFrac, compiledFrac).toString()).toBe(
    calculateCost(uFrac, pricingFrac),
  );

  expect(() => calculateCompiledCostAtoms({ input_tokens: "-5" } as Usage, compiledInt)).toThrow();
  expect(() =>
    calculateCompiledCostAtoms({ unknown_dim: "5" } as unknown as Usage, compiledInt),
  ).toThrow();
  const hugePricing: Pricing = {
    currency: "USD",
    rates: { input_tokens: { numerator: "1000000000000000000000000000000", denominator: "1" } },
  };
  const compiledHuge = compilePricing(hugePricing);
  expect(() => calculateCompiledCostAtoms({ input_tokens: "10000000000" }, compiledHuge)).toThrow(
    RangeError,
  );
});

test("sha256Hex fast path matches canonical SHA-256 and outboxShard stays in 0..15", () => {
  const samples: unknown[] = [
    { output_tokens: "5", input_tokens: "7", cached_input_tokens: "3" },
    { audio_seconds: "12.5", requests: "1" },
    {},
    { nested: { a: 1 }, list: [1, 2] },
  ];
  for (const sample of samples) {
    const expected = new Bun.CryptoHasher("sha256").update(canonical(sample)).digest("hex");
    expect(sha256Hex(sample)).toBe(expected);
  }
  expect(() => sha256Hex({ input_tokens: "5", output_tokens: undefined })).toThrow(
    "Invalid JSON value",
  );
  for (let i = 0; i < 64; i++) {
    const shard = outboxShard(crypto.randomUUID());
    expect(shard).toBeGreaterThanOrEqual(0);
    expect(shard).toBeLessThan(16);
  }
  expect(outboxShard(null)).toBe(0);
});
