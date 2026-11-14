import { expect, test } from "bun:test";
import { RedisClient } from "bun";
import { CoreRpcError } from "@gateway/core-client";
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
