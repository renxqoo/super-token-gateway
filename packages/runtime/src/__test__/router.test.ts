import { expect, test } from "bun:test";
import { LocalRateLimiter, SmartRouter, type RoutableCandidate } from "../router.ts";

function makeCandidate(
  id: string,
  channelId: string,
  priority: number,
  weight = 1,
): RoutableCandidate {
  return {
    id,
    channel_id: channelId,
    account_id: `acct-${channelId}`,
    priority,
    weight,
    operations: ["chat"],
    enabled: true,
    channel_enabled: true,
    account_active: true,
  };
}

test("SmartRouter enforces priority tiers, LEAST_LATENCY, ROUND_ROBIN, and WEIGHTED ordering", () => {
  let clock = 1000;
  const router = new SmartRouter({ now: () => clock, random: () => 0.9 });
  const c1 = makeCandidate("r1", "ch1", 10, 1);
  const c2 = makeCandidate("r2", "ch2", 10, 9);
  const c3 = makeCandidate("r3", "ch3", 5, 1);

  router.recordSuccess("ch1", 300);
  router.recordSuccess("ch2", 40);
  const least = router.selectCandidates("m1", [c1, c2, c3], "LEAST_LATENCY", "chat");
  expect(least.map((x) => x.route.channel_id)).toEqual(["ch2", "ch1", "ch3"]);

  const rr1 = router.selectCandidates("m1", [c1, c2, c3], "ROUND_ROBIN", "chat");
  const rr2 = router.selectCandidates("m1", [c1, c2, c3], "ROUND_ROBIN", "chat");
  expect(rr1[0]!.route.channel_id).not.toBe(rr2[0]!.route.channel_id);
  expect(rr1[2]!.route.channel_id).toBe("ch3");

  const weighted = router.selectCandidates("m1", [c1, c2, c3], "WEIGHTED", "chat");
  expect(weighted[0]!.route.channel_id).toBe("ch2");
  expect(weighted[2]!.route.channel_id).toBe("ch3");
});

test("SmartRouter trips OPEN after failure threshold, isolates 429 cooldown and auth_error, and recovers via single HALF_OPEN probe", () => {
  let clock = 1000;
  const router = new SmartRouter({
    now: () => clock,
    failureThreshold: 3,
    baseOpenMs: 10_000,
  });
  const c1 = makeCandidate("r1", "ch1", 10, 1);
  const c2 = makeCandidate("r2", "ch2", 5, 1);

  router.recordFailure("ch1", "retryable");
  router.recordFailure("ch1", "retryable");
  expect(router.isAvailable("ch1")).toEqual({ available: true, isProbe: false });
  router.recordFailure("ch1", "retryable");
  expect(router.isAvailable("ch1")).toEqual({ available: false, isProbe: false });

  const duringOpen = router.selectCandidates("m1", [c1, c2], "WEIGHTED", "chat");
  expect(duringOpen.map((x) => x.route.channel_id)).toEqual(["ch2"]);

  clock += 10_001;
  expect(router.isAvailable("ch1")).toEqual({ available: true, isProbe: true });
  expect(router.isAvailable("ch1")).toEqual({ available: false, isProbe: false });

  router.recordSuccess("ch1", 50);
  expect(router.isAvailable("ch1")).toEqual({ available: true, isProbe: false });

  router.recordFailure("ch1", "rate_limit", 5000);
  expect(router.isAvailable("ch1")).toEqual({ available: false, isProbe: false });
  clock += 5001;
  expect(router.isAvailable("ch1")).toEqual({ available: true, isProbe: false });

  router.recordFailure("ch1", "auth_error");
  expect(router.isAvailable("ch1")).toEqual({ available: false, isProbe: false });
});

test("LocalRateLimiter enforces RPM, TPM, and active concurrency accurately", () => {
  let clock = 0;
  const limiter = new LocalRateLimiter(() => clock);
  const p1 = limiter.acquire("proj:1", { rpm: 2, tpm: 100, concurrency: 1 }, 60);
  expect(p1.ok).toBe(true);
  expect(limiter.acquire("proj:1", { rpm: 2, tpm: 100, concurrency: 1 }, 10)).toEqual({
    ok: false,
    reason: "concurrency",
  });
  if (p1.ok) p1.release();

  expect(limiter.acquire("proj:1", { rpm: 2, tpm: 100, concurrency: 1 }, 50)).toEqual({
    ok: false,
    reason: "tpm",
  });
  const p2 = limiter.acquire("proj:1", { rpm: 2, tpm: 100, concurrency: 1 }, 40);
  expect(p2.ok).toBe(true);
  if (p2.ok) p2.release();

  expect(limiter.acquire("proj:1", { rpm: 2, tpm: 100, concurrency: 1 }, 0)).toEqual({
    ok: false,
    reason: "rpm",
  });
  clock += 60_001;
  const p3 = limiter.acquire("proj:1", { rpm: 2, tpm: 100, concurrency: 1 }, 10);
  expect(p3.ok).toBe(true);
  if (p3.ok) p3.release();
});
