import { expect, test } from "bun:test";
import { RedisClient } from "bun";
import { CoreRpcError } from "@gateway/core-client";
import { Limits } from "@gateway/runtime";
import type { ExecutionReply } from "@gateway/contracts";
import { prepareFinancialAdmission, withFinancial } from "../support/financial-execution.ts";
import { eventually, tcpProxy } from "../support/gateway-process.ts";

type Client = Parameters<typeof prepareFinancialAdmission>[1];

function outcome(pending: Promise<unknown>) {
  return pending.then(
    () => "OK",
    (error: unknown) =>
      error instanceof CoreRpcError ? `${error.status}:${error.detail.code}` : String(error),
  );
}

function observe(
  client: Client,
  attempt: string,
  channel: string,
  result: "SUCCESS" | "FAILURE" | "NOT_SENT",
  latency = 20,
) {
  return client.execution(
    "execution_quota_v1",
    {
      attempt_id: attempt,
      permit_epoch: "1",
      action: "OBSERVE",
      observation: { channel_id: channel, outcome: result, latency_ms: latency },
    },
    { deadlineAt: new Date(Date.now() + 5000).toISOString() },
  );
}

async function fund(
  f: { request(path: string, body?: unknown): Promise<{ status: number }> },
  project: string,
) {
  expect(
    (
      await f.request(`/projects/${project}/credits`, {
        currency: "USD",
        external_ref: `health-${crypto.randomUUID()}`,
        amount_atoms: "100000",
        kind: "GIFT",
      })
    ).status,
  ).toBe(201);
}

test("three failed attempts open the channel circuit, stale closed-mode successes cannot close it, and only one half-open probe is admitted until it reports", () =>
  withFinancial(async (f) => {
    const setup = await f.bootstrap(),
      client = f.client(),
      stranger = f.client(),
      channel = setup.provider.id,
      redis = new RedisClient(process.env.REDIS_URL!),
      healthKey = `${f.namespace}:health:${channel}`;
    try {
      await client.startup();
      await stranger.startup();
      await fund(f, setup.project.id);
      const admitted: string[] = [];
      for (let i = 0; i < 4; i++) {
        const prepared = await prepareFinancialAdmission(setup, client);
        await prepared.admit();
        admitted.push(prepared.attemptId);
      }
      const context = () => outcome(prepareFinancialAdmission(setup, client));
      for (const attempt of admitted.slice(0, 3)) {
        expect(await context()).toBe("OK");
        expect(await observe(client, attempt, channel, "FAILURE")).toEqual({ state: "OBSERVED" });
      }
      expect(await context()).toBe("503:no_route");
      expect(Number(await redis.send("HGET", [healthKey, "fails"]))).toBe(3);
      await observe(client, admitted[3]!, channel, "SUCCESS");
      await observe(client, admitted[0]!, channel, "SUCCESS");
      expect(await context()).toBe("503:no_route");
      expect(Number(await redis.send("HGET", [healthKey, "fails"]))).toBe(3);
      await redis.send("HSET", [healthKey, "open_until", String(Date.now() - 1)]);
      const probe = await prepareFinancialAdmission(setup, client),
        loser = await prepareFinancialAdmission(setup, client),
        released = await prepareFinancialAdmission(setup, client);
      expect(await outcome(probe.admit())).toBe("OK");
      expect(await context()).toBe("503:no_route");
      expect(await outcome(loser.admit())).toBe("503:no_route");
      expect(await observe(stranger, probe.attemptId, channel, "SUCCESS")).toEqual({
        state: "OBSERVED",
      });
      expect(await context()).toBe("503:no_route");
      await observe(client, probe.attemptId, channel, "NOT_SENT");
      expect(await context()).toBe("OK");
      expect(await outcome(released.admit())).toBe("OK");
      expect(await context()).toBe("503:no_route");
      await observe(client, released.attemptId, channel, "SUCCESS");
      expect(await context()).toBe("OK");
      expect(await redis.send("HMGET", [healthKey, "fails", "open_until"])).toEqual(["0", "0"]);
    } finally {
      redis.close();
    }
  }));

test("LEAST_LATENCY orders channels by the latency the Gateway observed for sent attempts", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    await fund(f, setup.project.id);
    const second = await f.addChannel(setup, {
      baseUrl: "https://second.example.test",
      apiKey: "synthetic-upstream-secret-2",
      maxInputTokens: 1000,
      ceilings: { input_tokens: "100", output_tokens: "50" },
    });
    await db`UPDATE models SET routing_strategy='LEAST_LATENCY' WHERE name=${setup.model}`;
    const channels = [setup.provider.id, String(second.id)];
    const firstChannel = async () => {
      const prepared = await prepareFinancialAdmission(setup, client);
      return String(prepared.data.dispatch_projection.candidate_ref.channel_id);
    };
    const sent = async (channel: string, latency: number) => {
      const prepared = await prepareFinancialAdmission(setup, client, { channelId: channel });
      await prepared.admit();
      await observe(client, prepared.attemptId, channel, "SUCCESS", latency);
    };
    await sent(channels[0]!, 5);
    await sent(channels[1]!, 900);
    expect(await firstChannel()).toBe(channels[0]!);
    for (let i = 0; i < 6; i++) await sent(channels[0]!, 2000);
    await sent(channels[1]!, 10);
    expect(await firstChannel()).toBe(channels[1]!);
  }));

test("losing the admission Redis fails context and admission closed without persisting the command, and both recover without a Core restart", async () => {
  const proxy = await tcpProxy(new URL(process.env.REDIS_URL!));
  try {
    await withFinancial(
      async (f, db) => {
        const setup = await f.bootstrap(),
          client = f.client();
        await client.startup();
        const prepared = await prepareFinancialAdmission(setup, client);
        proxy.cut();
        expect(await outcome(prepareFinancialAdmission(setup, client))).toBe(
          "503:identity_store_unavailable",
        );
        const lost = await prepared.admit().then(
          () => undefined,
          (error: unknown) => error,
        );
        expect(lost).toBeInstanceOf(CoreRpcError);
        expect((lost as CoreRpcError).detail).toMatchObject({
          code: "identity_store_unavailable",
          retryability: "SAME_COMMAND",
        });
        expect(
          (
            await db`SELECT count(*)::integer n FROM execution_commands WHERE command_id=${prepared.command}`
          )[0].n,
        ).toBe(0);
        expect((await db`SELECT count(*)::integer n FROM billing_holds`)[0].n).toBe(0);
        proxy.resume();
        await eventually(
          async () => (await outcome(prepareFinancialAdmission(setup, client))) === "OK",
          "context after Redis recovery",
        );
        expect(await outcome(prepared.admit())).toBe("OK");
        expect((await db`SELECT count(*)::integer n FROM billing_holds`)[0].n).toBe(1);
      },
      { redisUrl: proxy.url },
    );
  } finally {
    await proxy.close();
  }
});

for (const [change, expected] of [
  ["UPDATE api_keys SET enabled=false", "403:customer_forbidden"],
  ["UPDATE projects SET enabled=false", "403:customer_forbidden"],
  ["UPDATE channels SET enabled=false", "409:candidate_stale"],
  ["UPDATE routes SET enabled=false", "409:candidate_stale"],
  ["UPDATE models SET enabled=false", "409:candidate_stale"],
  ["UPDATE upstream_accounts SET state='DISABLED',revision=revision+1", "409:candidate_stale"],
] as const)
  test(`begin re-reads authority and denies send after admission when ${change}`, () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap(),
        client = f.client();
      await client.startup();
      const prepared = await prepareFinancialAdmission(setup, client);
      const reply = (await prepared.admit()) as ExecutionReply;
      await db.unsafe(change);
      expect(
        await outcome(
          client.execution("execution_begin_v1", prepared.beginData(reply.receipt), {
            deadlineAt: prepared.deadline,
          }),
        ),
      ).toBe(expected);
      expect(
        await db`SELECT state FROM execution_permits WHERE attempt_id=${prepared.attemptId}`,
      ).toEqual([{ state: "RESERVED" }]);
    }));

async function withHealth(
  run: (health: {
    limits: Limits;
    grant: (attempt: string, leaseMs?: number) => Promise<boolean>;
    settle: (attempt: string, failed?: boolean) => Promise<boolean>;
    open: () => Promise<boolean>;
    trip: () => Promise<void>;
    cool: () => Promise<void>;
    key: string;
  }) => Promise<void>,
) {
  const limits = new Limits(process.env.REDIS_URL!, `health-${crypto.randomUUID()}`),
    channel = crypto.randomUUID(),
    owner = "core-session",
    key = `${limits.namespace}:health:${channel}`;
  const grant = (attempt: string, leaseMs = 60000) =>
    limits.grantHealth(channel, attempt, owner, leaseMs);
  const settle = (attempt: string, failed?: boolean) =>
    limits.settleHealth(
      channel,
      attempt,
      owner,
      failed === undefined ? undefined : { failed, latency: 20 },
    );
  try {
    await run({
      limits,
      grant,
      settle,
      open: async () => (await limits.health(channel)).open,
      trip: async () => {
        for (let i = 0; i < 3; i++) {
          const attempt = crypto.randomUUID();
          expect(await grant(attempt)).toBe(true);
          expect(await settle(attempt, true)).toBe(true);
        }
        expect((await limits.health(channel)).open).toBe(true);
      },
      cool: async () => {
        await limits.command("HSET", [key, "open_until", String(Date.now() - 1)]);
      },
      key,
    });
  } finally {
    const keys = (await limits.command("KEYS", [`${limits.namespace}:*`])) as string[];
    if (keys.length) await limits.command("DEL", keys);
    limits.close();
  }
}

test("a half-open probe released without an observation cannot report later over its replacement probe, so only the replacement's result moves the circuit", () =>
  withHealth(async ({ limits, grant, settle, open, trip, cool, key }) => {
    await trip();
    await cool();
    expect(await grant("released")).toBe(true);
    expect(await grant("excess")).toBe(false);
    expect(await settle("released")).toBe(true);
    expect(await grant("replacement")).toBe(true);
    expect(await grant("excess-2")).toBe(false);
    expect(await settle("released", false)).toBe(false);
    expect(await open()).toBe(true);
    expect(await grant("excess-3")).toBe(false);
    expect(await settle("replacement", true)).toBe(true);
    for (let i = 0; i < 3; i++) expect(await settle("released", false)).toBe(false);
    expect(await open()).toBe(true);
    expect(await limits.command("HGET", [key, "fails"])).toBe("4");
  }));

test("a half-open probe whose lease expired cannot report a success that closes the circuit or removes its replacement probe's exclusivity", () =>
  withHealth(async ({ limits, grant, settle, open, trip, cool, key }) => {
    await trip();
    await cool();
    expect(await grant("expired", 1000)).toBe(true);
    await Bun.sleep(1100);
    expect(await settle("expired", false)).toBe(false);
    expect(await limits.command("HGET", [key, "fails"])).toBe("3");
    expect(await grant("current")).toBe(true);
    expect(await grant("excess")).toBe(false);
    expect(await settle("expired", false)).toBe(false);
    expect(await grant("excess-2")).toBe(false);
    expect(await settle("current", true)).toBe(true);
    expect(await open()).toBe(true);
  }));

test("a half-open probe with a lease longer than the health retention keeps the health record and its exclusivity for the whole lease", () =>
  withHealth(async ({ limits, grant, settle, trip, cool, key }) => {
    await trip();
    await cool();
    expect(await grant("long", 700000)).toBe(true);
    expect(Number(await limits.command("PTTL", [key]))).toBeGreaterThanOrEqual(700000);
    expect(await grant("excess")).toBe(false);
    expect(await settle("long")).toBe(true);
  }));

test("a closed-mode success admitted before the breaker opened cannot close the circuit while the recovery probe is pending, nor erase failures counted after recovery", () =>
  withHealth(async ({ limits, grant, settle, open, trip, cool, key }) => {
    expect(await grant("old-during-probe")).toBe(true);
    expect(await grant("old-after-recovery")).toBe(true);
    await trip();
    await cool();
    expect(await grant("probe")).toBe(true);
    expect(await settle("old-during-probe", false)).toBe(true);
    expect(await open()).toBe(true);
    expect(await grant("excess")).toBe(false);
    expect(await settle("probe", false)).toBe(true);
    expect(await open()).toBe(false);
    const current = ["current-0", "current-1", "current-2"];
    for (const attempt of current) expect(await grant(attempt)).toBe(true);
    expect(await settle(current[0]!, true)).toBe(true);
    expect(await settle(current[1]!, true)).toBe(true);
    expect(await settle("old-after-recovery", false)).toBe(true);
    expect(await settle(current[2]!, true)).toBe(true);
    expect(await open()).toBe(true);
    expect(await limits.command("HGET", [key, "fails"])).toBe("3");
  }));
