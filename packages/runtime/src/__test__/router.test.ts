import { expect, test } from "bun:test";
import {
  LocalRateLimiter,
  SmartRouter,
  type CircuitTransition,
  type RoutableCandidate,
} from "../router.ts";

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

test("symptom: late failures from one burst keep extending an open circuit", () => {
  const durations: number[] = [];
  const router = new SmartRouter({
    now: () => 100,
    onStateChange: (event) => durations.push(event.open_duration_ms),
  });
  for (let i = 0; i < 6; i++) router.recordFailure("burst", "retryable");
  expect(durations).toEqual([15_000]);
});

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
  expect(router.isAvailable("ch1")).toEqual({ available: true });
  router.recordFailure("ch1", "retryable");
  expect(router.isAvailable("ch1")).toEqual({ available: false });

  const duringOpen = router.selectCandidates("m1", [c1, c2], "WEIGHTED", "chat");
  expect(duringOpen.map((x) => x.route.channel_id)).toEqual(["ch2"]);

  clock += 10_001;
  const probe = router.isAvailable("ch1").probe;
  expect(probe).toBeDefined();
  expect(router.isAvailable("ch1")).toEqual({ available: false });

  router.recordSuccess("ch1", 50, router.beginAttempt("ch1", probe));
  expect(router.isAvailable("ch1")).toEqual({ available: true });

  router.recordFailure("ch1", "rate_limit", 5000);
  expect(router.isAvailable("ch1")).toEqual({ available: false });
  clock += 5001;
  expect(router.isAvailable("ch1")).toEqual({ available: true });

  router.recordFailure("ch1", "auth_error");
  expect(router.isAvailable("ch1")).toEqual({ available: false });
});

test("shadow scoring preserves live draws and the half-open probe while active factors stay within priority and protocol", () => {
  let clock = 1000;
  let draws = 0;
  const router = new SmartRouter({
    now: () => clock,
    failureThreshold: 1,
    baseOpenMs: 5000,
    random: () => {
      draws++;
      return 0.7;
    },
  });
  const candidates = [
    { ...makeCandidate("native-a", "half-open", 10), same_protocol: true },
    { ...makeCandidate("native-b", "native-closed", 10), same_protocol: true },
    makeCandidate("converted", "cross", 10, 1000),
    { ...makeCandidate("backup", "low", 1, 1000), same_protocol: true },
  ];
  router.recordFailure("half-open", "retryable");
  clock += 5001;
  let comparisons = 0;
  const adaptive = {
    factors: (group: readonly RoutableCandidate[]) => {
      expect(group.map(({ id }) => id)).toEqual(["native-a", "native-b"]);
      return new Map([
        ["native-a", 1],
        ["native-b", 0.25],
      ]);
    },
    compare: () => {
      comparisons++;
    },
  };
  const preview = router.selectCandidates("chat", candidates, "WEIGHTED", "chat", {
    ...adaptive,
    mode: "observe",
  });
  expect(draws).toBe(2);
  expect(comparisons).toBe(1);
  expect(preview.map(({ route }) => route.id)).toEqual([
    "native-b",
    "native-a",
    "converted",
    "backup",
  ]);
  const token = preview[1]?.probe;
  expect(token).toBeDefined();
  expect(router.peekAvailable("half-open", token)).toBeTrue();
  expect(router.peekAvailable("half-open")).toBeFalse();
  router.releaseProbe("half-open", token!);

  const active = router.selectCandidates("chat", candidates, "WEIGHTED", "chat", {
    ...adaptive,
    mode: "active",
  });
  expect(active.map(({ route }) => route.id)).toEqual([
    "native-a",
    "native-b",
    "converted",
    "backup",
  ]);
  expect(draws).toBe(4);
  expect(comparisons).toBe(1);
});

test("symptom: expired probe results can release a successor and close the newer circuit", () => {
  let clock = 1000;
  const events: CircuitTransition[] = [];
  const router = new SmartRouter({
    now: () => clock,
    failureThreshold: 2,
    baseOpenMs: 5_000,
    probeTimeoutMs: 10_000,
    onStateChange: (event) => events.push(event),
  });

  const prior = router.beginAttempt("ch1");
  router.recordFailure("ch1", "retryable");
  router.recordFailure("ch1", "retryable");
  router.recordSuccess("ch1", 50, prior);
  expect(router.isAvailable("ch1")).toEqual({ available: false });

  clock += 5_001;
  const first = router.isAvailable("ch1").probe!;
  const firstAttempt = router.beginAttempt("ch1", first);
  expect(router.isAvailable("ch1")).toEqual({ available: false });

  clock += 10_001;
  const successor = router.isAvailable("ch1").probe;
  expect(successor).toBeDefined();
  expect(successor).not.toBe(first);
  router.releaseProbe("ch1", first);
  router.recordSuccess("ch1", 50, firstAttempt);
  expect(router.isAvailable("ch1")).toEqual({ available: false });
  expect(router.peekAvailable("ch1", successor)).toBe(true);

  router.applyRemoteEvent({ ...events[0]!, state: "CLOSED", open_duration_ms: 0 });
  expect(router.snapshot().find((c) => c.channel_id === "ch1")).toMatchObject({
    state: "CLOSED",
    backoff_level: 0,
    probe_in_flight: false,
    consecutive_failures: 0,
  });
});

test("symptom: local connection failures can make the same route unavailable on another node", () => {
  let clock = 10;
  const peer = new SmartRouter({ now: () => clock });
  const local = new SmartRouter({
    now: () => clock,
    onStateChange: (event) => peer.applyRemoteEvent(event),
  });
  for (let i = 0; i < 3; i++) local.recordDialFailure("channel", local.beginAttempt("channel"));
  expect(local.peekAvailable("channel")).toBe(false);
  expect(peer.peekAvailable("channel")).toBe(true);
  clock += 251;
  expect(local.peekAvailable("channel")).toBe(true);
});

test("symptom: reordered circuit events make peers disagree or reopen a completed probe", () => {
  const at = Date.now();
  const event: CircuitTransition = {
    channel_id: "channel",
    state: "OPEN",
    circuit_id: "a",
    issued_at_ms: at,
    open_duration_ms: 15_000,
    cooldown_duration_ms: 0,
  };
  const parallel = { ...event, circuit_id: "b" };
  const done: CircuitTransition = { ...parallel, state: "CLOSED", open_duration_ms: 0 };
  const first = new SmartRouter();
  const second = new SmartRouter();
  [event, parallel, done].forEach((item) => first.applyRemoteEvent(item));
  [parallel, event, done].forEach((item) => second.applyRemoteEvent(item));
  expect(first.peekAvailable("channel")).toBe(true);
  expect(second.peekAvailable("channel")).toBe(true);

  const late = new SmartRouter();
  [done, parallel].forEach((item) => late.applyRemoteEvent(item));
  expect(late.peekAvailable("channel")).toBe(true);
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

test("SmartRouter prefers same-protocol routes within the same priority tier while preserving cross-protocol fallback", () => {
  const router = new SmartRouter({ random: () => 0.9 });
  const crossHighPriority: RoutableCandidate = {
    ...makeCandidate("r-hi", "ch-hi", 20, 1),
    same_protocol: false,
  };
  const crossSameTier: RoutableCandidate = {
    ...makeCandidate("r-cross", "ch-cross", 10, 100),
    same_protocol: false,
  };
  const nativeSameTier: RoutableCandidate = {
    ...makeCandidate("r-native", "ch-native", 10, 1),
    same_protocol: true,
  };

  const selected = router.selectCandidates(
    "m-proto",
    [crossSameTier, nativeSameTier],
    "WEIGHTED",
    "chat",
  );
  expect(selected.map((x) => x.route.channel_id)).toEqual(["ch-native", "ch-cross"]);

  const withHigherPriority = router.selectCandidates(
    "m-proto",
    [crossSameTier, nativeSameTier, crossHighPriority],
    "WEIGHTED",
    "chat",
  );
  expect(withHigherPriority.map((x) => x.route.channel_id)).toEqual([
    "ch-hi",
    "ch-native",
    "ch-cross",
  ]);
});

test("SmartRouter peekAvailable reflects live OPEN, cooldown, and HALF_OPEN ownership without mutating probe state", () => {
  let clock = 1000;
  const router = new SmartRouter({
    now: () => clock,
    failureThreshold: 2,
    baseOpenMs: 5_000,
    probeTimeoutMs: 10_000,
  });

  expect(router.peekAvailable("ch-new")).toBe(true);
  router.recordFailure("ch-1", "retryable");
  expect(router.peekAvailable("ch-1")).toBe(true);
  router.recordFailure("ch-1", "retryable");
  expect(router.peekAvailable("ch-1")).toBe(false);

  clock += 5_001;
  expect(router.peekAvailable("ch-1")).toBe(true);
  expect(router.snapshot().find((s) => s.channel_id === "ch-1")?.state).toBe("OPEN");
  const probe = router.isAvailable("ch-1").probe;
  expect(probe).toBeDefined();
  expect(router.peekAvailable("ch-1")).toBe(false);
  expect(router.peekAvailable("ch-1", probe)).toBe(true);

  router.recordFailure("ch-1", "rate_limit", 4_000, undefined, router.beginAttempt("ch-1", probe));
  expect(router.peekAvailable("ch-1")).toBe(false);
  expect(router.peekAvailable("ch-1", probe)).toBe(false);
  clock += 4_001;
  expect(router.peekAvailable("ch-1")).toBe(true);
  expect(router.peekAvailable("ch-1", probe)).toBe(false);
});
