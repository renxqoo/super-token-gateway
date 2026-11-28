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
import {
  CELL_COUNT,
  outboxShard,
  SETTLEMENT_LANES,
  sha256Hex,
  SUPPLIER_CELLS_PER_LANE,
  walletAdmissionError,
  walletLane,
  type WalletRow,
} from "../funds.ts";
import { priceSettlement } from "../settlement-pricing.ts";
import type { CommitTerminalInput } from "../types.ts";

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

test("symptom: separate arrivals in a sustained burst each open a durable batch", async () => {
  const batches: number[][] = [];
  const queue = new MicroBatchQueue<number, number>(
    async (items) => {
      batches.push(items);
      return items;
    },
    64,
    250,
    1,
    "collect-bursts",
  );
  expect(await queue.enqueue(1)).toBe(1);
  await Promise.resolve();
  const second = queue.enqueue(2);
  await Bun.sleep(5);
  const third = queue.enqueue(3);
  expect(await Promise.all([second, third])).toEqual([2, 3]);
  expect(batches).toEqual([[1], [2, 3]]);
  await queue.drain();
});

test("symptom: an idle request waits behind the prior batch's timer", async () => {
  const batches: number[][] = [];
  const first = Promise.withResolvers<void>();
  const queue = new MicroBatchQueue<number, number>(
    async (items) => {
      batches.push(items);
      if (items.includes(1)) await first.promise;
      return items;
    },
    64,
    250,
    2,
  );
  const a = queue.enqueue(1);
  await Bun.sleep(0);
  const b = queue.enqueue(2);
  first.resolve();
  await Promise.all([a, b]);
  await Promise.resolve();
  const c = queue.enqueue(3);
  await Bun.sleep(5);
  const flushed = batches.slice();
  await queue.drain();
  expect(await c).toBe(3);
  expect(flushed).toEqual([[1], [2], [3]]);
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

test("symptom: settlement charges only the input when supplier-priced output evidence is absent", () => {
  const item = {
    request_id: "request",
    selected_attempt_id: "attempt",
    channel_id: "channel",
    billing_price_id: "customer-price",
    cost_version_id: "supplier-price",
    billing_pricing: {
      currency: "USD",
      rates: { input_tokens: { numerator: "2", denominator: "1" } },
    },
    supplier_pricing: {
      currency: "USD",
      rates: { output_tokens: { numerator: "4", denominator: "1" } },
    },
    billing_basis: "PROVIDER_USAGE",
    supplier_basis: "PROVIDER_USAGE",
    supplier_currency: "USD",
    usage: { input_tokens: "3" },
  } satisfies CommitTerminalInput;
  expect(() => priceSettlement(item)).toThrow("Upstream omitted token usage required by pricing");
  expect(priceSettlement({ ...item, usage: { input_tokens: "3", output_tokens: "0" } })).toEqual({
    customerAtoms: 6n,
    supplierAtoms: 0n,
  });
  expect(priceSettlement({ ...item, supplier_cost_atoms_override: "9" })).toEqual({
    customerAtoms: 6n,
    supplierAtoms: 9n,
  });
});

test("priceSettlement synthesizes aggregate cache write totals, rejects partial or conflicting TTLs, and allows explicit zero cache writes", () => {
  const baseItem = {
    request_id: "req-cache",
    selected_attempt_id: "att-cache",
    channel_id: "ch-cache",
    billing_price_id: "cp-cache",
    cost_version_id: "sp-cache",
    billing_pricing: {
      currency: "USD",
      rates: {
        input_tokens: { numerator: "1", denominator: "1" },
        output_tokens: { numerator: "1", denominator: "1" },
        cache_write_tokens: { numerator: "2", denominator: "1" },
      },
    },
    supplier_pricing: {
      currency: "USD",
      rates: {
        input_tokens: { numerator: "1", denominator: "1" },
        output_tokens: { numerator: "1", denominator: "1" },
      },
    },
    billing_basis: "PROVIDER_USAGE" as const,
    supplier_basis: "PROVIDER_USAGE" as const,
    supplier_currency: "USD",
    usage: {
      input_tokens: "10",
      output_tokens: "5",
      cache_write_5m_tokens: "100",
      cache_write_1h_tokens: "200",
    },
  } satisfies CommitTerminalInput;

  expect(priceSettlement(baseItem)).toEqual({ customerAtoms: 615n, supplierAtoms: 15n });

  expect(() =>
    priceSettlement({
      ...baseItem,
      usage: { input_tokens: "10", output_tokens: "5", cache_write_5m_tokens: "100" },
    }),
  ).toThrow("Upstream omitted cache write usage required by pricing");

  const ttlPricing = {
    currency: "USD",
    rates: {
      input_tokens: { numerator: "1", denominator: "1" },
      output_tokens: { numerator: "1", denominator: "1" },
      cache_write_5m_tokens: { numerator: "2", denominator: "1" },
      cache_write_1h_tokens: { numerator: "3", denominator: "1" },
    },
  };
  expect(() =>
    priceSettlement({
      ...baseItem,
      billing_pricing: ttlPricing,
      usage: { input_tokens: "10", output_tokens: "5", cache_write_tokens: "100" },
    }),
  ).toThrow("Upstream omitted cache write usage required by pricing");

  expect(
    priceSettlement({
      ...baseItem,
      billing_pricing: ttlPricing,
      usage: { input_tokens: "10", output_tokens: "5", cache_write_tokens: "0" },
    }),
  ).toEqual({ customerAtoms: 15n, supplierAtoms: 15n });

  for (const conflicting of [
    {
      input_tokens: "10",
      output_tokens: "5",
      cache_write_tokens: "100",
      cache_write_5m_tokens: "150",
    },
    {
      input_tokens: "10",
      output_tokens: "5",
      cache_write_tokens: "100",
      cache_write_5m_tokens: "60",
      cache_write_1h_tokens: "60",
    },
  ]) {
    expect(() => priceSettlement({ ...baseItem, usage: conflicting })).toThrow(
      "Provider cache write partitions disagree",
    );
  }

  expect(
    priceSettlement({
      ...baseItem,
      billing_basis: "INPUT_CHARACTERS",
      billing_pricing: {
        currency: "USD",
        rates: { characters: { numerator: "3", denominator: "1" } },
      },
      supplier_basis: "REQUEST_COMPLETION",
      supplier_pricing: {
        currency: "USD",
        rates: { requests: { numerator: "7", denominator: "1" } },
      },
      input_characters: "12",
      usage: { requests: "1" },
    }),
  ).toEqual({ customerAtoms: 36n, supplierAtoms: 7n });

  expect(() => priceSettlement({ ...baseItem, supplier_cost_atoms_override: "-1" })).toThrow(
    "Invalid supplier_cost_atoms_override",
  );
});

test("2-lane sharding partitions 16 cells evenly and walletAdmissionError accounts for deferred lease spend", () => {
  expect(SETTLEMENT_LANES).toBe(2);
  expect(CELL_COUNT).toBe(16);
  expect(SUPPLIER_CELLS_PER_LANE).toBe(8);
  const seenLanes = new Set<number>();
  for (let nibble = 0; nibble < 16; nibble++) {
    const id = `00000000-0000-4000-8000-00000000000${nibble.toString(16)}`;
    const lane = walletLane(id);
    expect(lane).toBeGreaterThanOrEqual(0);
    expect(lane).toBeLessThan(2);
    seenLanes.add(lane);
  }
  expect(seenLanes.size).toBe(2);

  const wallet: WalletRow = {
    id: crypto.randomUUID(),
    billing_account_id: crypto.randomUUID(),
    currency: "PTS",
    policy: "GIFT_FIRST",
    available_atoms: 100n,
    reserved_atoms: 400n,
    spent_atoms: 0n,
    expired_atoms: 0n,
    debt_atoms: 350n,
    credit_limit_atoms: 500n,
    credit_reserved_atoms: 0n,
    credit_policy_version: 0n,
    manual_frozen: false,
    evidence_disputed: false,
    freeze_reason: null,
    dirty: false,
  };
  expect(walletAdmissionError(wallet, 0n)).toBeNull();
  expect(walletAdmissionError(wallet, 150n)).toBeNull();
  expect(walletAdmissionError(wallet, 151n)).toEqual({
    code: "insufficient_balance",
    message: "Wallet has a negative settled balance",
  });
});
