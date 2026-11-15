import { expect, test } from "bun:test";
import { orderRouteCandidates } from "../route-policy.ts";

type Candidate = { id: string; provider_id: string; priority: number; weight: number };

function route(id: string, priority: number, weight = 1): Candidate {
  return { id, provider_id: `provider-${id}`, priority, weight };
}

function limits(
  health: Record<string, { open?: boolean; latency?: number }> = {},
  offsets: number[] = [],
) {
  const turns: { scope: string; size: number }[] = [];
  return {
    turns,
    limits: {
      health: async (provider: string) => ({
        open: health[provider]?.open ?? false,
        latency: health[provider]?.latency ?? 100,
      }),
      roundRobin: async (scope: string, size: number) => {
        turns.push({ scope, size });
        return offsets.shift() ?? 0;
      },
    },
  };
}

test("a higher priority tier is always offered before a faster or heavier lower tier", async () => {
  const { limits: fake } = limits({ "provider-low": { latency: 1 } });
  for (const strategy of ["WEIGHTED", "ROUND_ROBIN", "LEAST_LATENCY"])
    expect(
      (
        await orderRouteCandidates(
          [route("low", 1, 1000), route("high", 5)],
          strategy,
          "model",
          fake,
        )
      ).map((c) => c.id),
    ).toEqual(["high", "low"]);
});

test("channels with an open circuit are left out and no healthy channel is no_route", async () => {
  const { limits: fake } = limits({ "provider-a": { open: true } });
  expect(
    (await orderRouteCandidates([route("a", 5), route("b", 1)], "WEIGHTED", "m", fake)).map(
      (c) => c.id,
    ),
  ).toEqual(["b"]);
  const closed = limits({ "provider-a": { open: true }, "provider-b": { open: true } });
  await expect(
    orderRouteCandidates([route("a", 5), route("b", 1)], "WEIGHTED", "m", closed.limits),
  ).rejects.toMatchObject({ status: 503, code: "no_route" });
});

test("round robin rotates one equal-priority tier from the shared offset keyed by model and priority", async () => {
  const shared = limits({}, [1, 2]);
  const tier = [route("c", 3), route("a", 3), route("b", 3), route("z", 1)];
  expect(
    (await orderRouteCandidates(tier, "ROUND_ROBIN", "m", shared.limits)).map((c) => c.id),
  ).toEqual(["b", "c", "a", "z"]);
  expect(
    (await orderRouteCandidates(tier, "ROUND_ROBIN", "m", shared.limits)).map((c) => c.id),
  ).toEqual(["c", "a", "b", "z"]);
  expect(shared.turns).toEqual([
    { scope: "m:3", size: 3 },
    { scope: "m:3", size: 3 },
  ]);
});

test("least latency orders a tier by observed latency and breaks ties by weight", async () => {
  const { limits: fake } = limits({
    "provider-slow": { latency: 300 },
    "provider-fast": { latency: 20 },
    "provider-tie-light": { latency: 50 },
    "provider-tie-heavy": { latency: 50 },
  });
  expect(
    (
      await orderRouteCandidates(
        [route("slow", 2), route("tie-light", 2, 1), route("fast", 2), route("tie-heavy", 2, 9)],
        "LEAST_LATENCY",
        "m",
        fake,
      )
    ).map((c) => c.id),
  ).toEqual(["fast", "tie-heavy", "tie-light", "slow"]);
});

test("weighted routing prefers channels in proportion to their weight", async () => {
  const { limits: fake } = limits();
  const first = new Map<string, number>();
  for (let i = 0; i < 4000; i++) {
    const [head] = await orderRouteCandidates(
      [route("light", 1, 1), route("heavy", 1, 9)],
      "WEIGHTED",
      "m",
      fake,
    );
    first.set(head!.id, (first.get(head!.id) ?? 0) + 1);
  }
  const share = (first.get("heavy") ?? 0) / 4000;
  expect(share).toBeGreaterThan(0.86);
  expect(share).toBeLessThan(0.94);
});
