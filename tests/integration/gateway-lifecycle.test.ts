import { expect, test } from "bun:test";
import { execFileSync } from "node:child_process";
import { RedisClient, type SQL } from "bun";
import { createCommitAckProxy } from "../support/commit-ack-proxy.ts";
import {
  chatFrames,
  chatReplies,
  eventually,
  requestOutcome,
  settledCharge,
  slowStream,
  stall,
  tcpProxy,
  withGatewayStack,
} from "../support/gateway-process.ts";

const timeout = 90000;

test(
  "a stream cut before its usage and [DONE] reports a gateway stream error and settles no success charge",
  () =>
    withGatewayStack(
      { reply: () => chatReplies.sse(chatFrames().slice(0, 1), false) },
      async ({ db, received, send }) => {
        const response = await send({ stream: true });
        expect(response.status).toBe(200);
        const text = await response.text();
        expect(text).toContain('"content":"o"');
        expect(text).toContain("gateway_stream_error");
        expect(text).not.toContain("[DONE]");
        const requestId = response.headers.get("x-request-id")!;
        expect(await requestOutcome(db, requestId)).toEqual({
          state: "FAILED",
          billing_outcome: "FAILED",
          charged: "0",
        });
        expect(
          await db`SELECT state,error_code FROM attempts WHERE request_id=${requestId}`,
        ).toEqual([{ state: "PARTIAL", error_code: "stream_interrupted" }]);
        expect(received).toHaveLength(1);
      },
    ),
  timeout,
);

test(
  "empty JSON choices, missing or empty messages, unfinished choices and terminal-only streams never charge as success",
  () => {
    const replies = [
      () => Response.json({ id: "x", model: "synthetic", choices: [] }),
      () => Response.json({ id: "x", model: "synthetic", choices: [{ finish_reason: "stop" }] }),
      () =>
        Response.json({
          id: "x",
          model: "synthetic",
          choices: [{ message: { role: "assistant", content: "" }, finish_reason: "stop" }],
        }),
      () =>
        Response.json({
          id: "x",
          model: "synthetic",
          choices: [{ message: { role: "assistant", content: "ok" }, finish_reason: null }],
        }),
      () => chatReplies.sse([]),
    ];
    return withGatewayStack(
      { reply: (call) => replies[call.index]!(), supplierCredit: "10000" },
      async ({ db, received, send }) => {
        for (let index = 0; index < replies.length; index++) {
          const stream = index === replies.length - 1;
          const response = await send({ stream });
          const text = await response.text();
          expect(response.status >= 400 || !text.includes("[DONE]")).toBe(true);
          const outcome = await requestOutcome(db, response.headers.get("x-request-id")!);
          expect(outcome.charged).toBe("0");
          expect(outcome.state).not.toBe("SUCCEEDED");
        }
        expect(received).toHaveLength(replies.length);
      },
    );
  },
  timeout,
);

test(
  "a Gateway refuses to start without durable evidence, and evidence lost after start withholds the terminal, tells JSON clients to query the request instead of claiming it was not recorded, and never re-sends",
  () =>
    withGatewayStack(
      { reply: (call) => (call.body.stream ? chatReplies.sse() : chatReplies.json()) },
      async ({ received, namespace, send, startGateway }) => {
        const queue = { url: process.env.QUEUE_REDIS_URL!, namespace, ack_timeout_ms: 300 };
        await expect(
          startGateway({ queue: { ...queue, required_replicas: 1 } }, "gatewayB"),
        ).rejects.toThrow("usage_queue_unavailable");
        const proxy = await tcpProxy(new URL(queue.url));
        try {
          const gateway = await startGateway(
            { queue: { ...queue, url: proxy.url, required_replicas: 0 } },
            "gatewayB",
          );
          proxy.cut();
          const response = await send({ stream: true, via: gateway });
          expect(response.status).toBe(200);
          const text = await response.text();
          expect(text).toContain("gateway_stream_error");
          expect(text).not.toContain("[DONE]");
          expect(received).toHaveLength(1);
          const json = await send({ via: gateway });
          expect(json.status).toBe(503);
          expect((await json.json()).error).toMatchObject({
            code: "usage_queue_unavailable",
            message:
              "Durable usage acceptance is unavailable; query the original request before retrying",
          });
          expect((await fetch(`${gateway.origin}/readyz`)).status).toBe(503);
          await Bun.sleep(500);
          expect(received).toHaveLength(2);
        } finally {
          await proxy.close();
        }
      },
    ),
  timeout,
);

test(
  "a private upstream address is refused before admission reserves any funds",
  () =>
    withGatewayStack(
      { reply: () => chatReplies.json(), gateway: { allow_private_upstream: false } },
      async ({ db, setup, received, send }) => {
        const response = await send();
        expect(response.status).toBe(502);
        expect((await response.json()).error.code).toBe("unsafe_upstream");
        expect(received).toHaveLength(0);
        expect((await db`SELECT count(*)::integer n FROM gateway_requests`)[0].n).toBe(0);
        expect(
          (
            await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
          )[0].reserved_atoms,
        ).toBe("0");
      },
    ),
  timeout,
);

test(
  "a paused settlement worker leaves durable evidence queued and the restarted worker settles it once",
  () =>
    withGatewayStack(
      { reply: () => chatReplies.json() },
      async ({ db, worker, send, startWorker }) => {
        expect((await worker.stop()).event).toBe("stopped");
        const response = await send();
        expect(response.status).toBe(200);
        await response.text();
        const requestId = response.headers.get("x-request-id")!;
        await Bun.sleep(500);
        expect(
          (
            await db`SELECT count(*)::integer n FROM billing_charges WHERE request_id=${requestId}`
          )[0].n,
        ).toBe(0);
        await startWorker();
        expect(await settledCharge(db, requestId)).toEqual({
          amount: "100",
          state: "SUCCEEDED",
          billing_outcome: "SUCCESS",
        });
        await eventually(
          async () =>
            (
              await db`SELECT count(*)::integer n FROM core_evidence_receipts r JOIN attempts a ON a.id=r.attempt_id WHERE a.request_id=${requestId}`
            )[0].n === 2,
          "both evidence revisions",
        );
      },
    ),
  timeout,
);

async function leaseStates(db: SQL, requestId: string) {
  return [
    ...new Set(
      (await db`SELECT state FROM execution_quota_leases WHERE request_id=${requestId}`).map(
        (row: { state: string }) => row.state,
      ),
    ),
  ].sort();
}

async function onlyRequest(db: SQL) {
  await eventually(
    async () => (await db`SELECT count(*)::integer n FROM attempts`)[0].n === 1,
    "admitted attempt",
  );
  return (await db`SELECT id,request_id FROM attempts`)[0] as { id: string; request_id: string };
}

test(
  "a client that disconnects mid-stream cancels the upstream, keeps the sent attempt's supplier exposure and never leaves quota leases RELEASE_PENDING when Core is busy",
  () => {
    let upstreamCancelled = false;
    return withGatewayStack(
      {
        reply: (call) =>
          call.index === 0
            ? slowStream(chatFrames().slice(0, 1), 50, () => (upstreamCancelled = true))
            : chatReplies.json(),
      },
      async ({ db, setup, received, send }) => {
        const controller = new AbortController();
        const response = await send({ stream: true, signal: controller.signal });
        const requestId = response.headers.get("x-request-id")!;
        const reader = response.body!.getReader();
        await reader.read();
        controller.abort();
        await eventually(
          async () => upstreamCancelled || received[0]!.signal.aborted,
          "upstream cancellation",
        );
        expect(await requestOutcome(db, requestId)).toMatchObject({ charged: "0" });
        await eventually(
          async () => (await leaseStates(db, requestId)).join() === "RELEASED",
          "released quota leases",
        );
        expect(
          await db`SELECT a.state,a.error_code,h.state AS hold FROM attempts a JOIN upstream_holds h ON h.attempt_id=a.id WHERE a.request_id=${requestId}`,
        ).toEqual([{ state: "PARTIAL", error_code: "client_cancelled", hold: "PARTIAL" }]);
        expect(
          (
            await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
          )[0].reserved_atoms,
        ).toBe("344");
        const next = await send();
        expect(next.status).toBe(200);
        await next.text();
      },
    );
  },
  timeout,
);

test(
  "SIGTERM lets an in-flight request finish within the grace period while refusing new work",
  () =>
    withGatewayStack(
      {
        reply: async () => {
          await Bun.sleep(800);
          return chatReplies.json();
        },
      },
      async ({ db, received, gateway, send }) => {
        const pending = send();
        await eventually(async () => received.length === 1, "upstream request");
        const stopped = gateway.stop();
        await Bun.sleep(100);
        expect(
          await send().then(
            (r) => r.status,
            () => "refused",
          ),
        ).not.toBe(200);
        const response = await pending;
        expect(response.status).toBe(200);
        await response.text();
        expect(await stopped).toMatchObject({ event: "stopped", drained: true, grants: 0 });
        expect((await settledCharge(db, response.headers.get("x-request-id")!)).amount).toBe("100");
        expect(received).toHaveLength(1);
      },
    ),
  timeout,
);

test(
  "a request still running when the grace period ends is cut, answered as draining and closed without a charge",
  () => {
    const hold = stall();
    return withGatewayStack(
      {
        reply: async () => {
          await hold.released;
          return chatReplies.json();
        },
        gateway: { shutdown_grace_ms: 300 },
      },
      async ({ db, received, gateway, send }) => {
        try {
          const pending = send();
          await eventually(async () => received.length === 1, "upstream request");
          const stopped = gateway.stop();
          const response = await pending;
          expect(response.status).toBe(503);
          expect((await response.json()).error.code).toBe("draining");
          expect(await stopped).toMatchObject({ event: "stopped", grants: 0 });
          await eventually(async () => received[0]!.signal.aborted, "upstream disconnect");
          const requestId = response.headers.get("x-request-id")!;
          expect(await requestOutcome(db, requestId)).toMatchObject({
            state: "FAILED",
            charged: "0",
          });
          await eventually(
            async () => (await leaseStates(db, requestId)).join() === "RELEASED",
            "released quota leases",
          );
        } finally {
          hold.release();
        }
      },
    );
  },
  timeout,
);

test(
  "two Gateway processes receiving the same idempotency key send upstream once and charge once",
  () =>
    withGatewayStack(
      {
        reply: async () => {
          await Bun.sleep(300);
          return chatReplies.json();
        },
      },
      async ({ db, received, send, startGateway }) => {
        const second = await startGateway({}, "gatewayB");
        const headers = { "idempotency-key": crypto.randomUUID() };
        const responses = await Promise.all([send({ headers }), send({ headers, via: second })]);
        const bodies = await Promise.all(responses.map((r) => r.json()));
        expect(responses.map((r) => r.status).sort()).toEqual([200, 409]);
        const rejected = bodies[responses.findIndex((r) => r.status === 409)];
        expect(["already_accepted", "idempotency_conflict"]).toContain(rejected.error.code);
        expect(received).toHaveLength(1);
        const winner = responses.find((r) => r.status === 200)!.headers.get("x-request-id")!;
        expect((await settledCharge(db, winner)).amount).toBe("100");
        expect((await db`SELECT count(*)::integer n FROM billing_charges`)[0].n).toBe(1);
      },
    ),
  timeout,
);

test(
  "a Gateway killed after sending leaves the attempt's outcome unknown with exposure held, and its restart never re-sends",
  () => {
    const hold = stall();
    return withGatewayStack(
      {
        reply: async (call) => {
          if (call.index === 0) await hold.released;
          return chatReplies.json();
        },
      },
      async ({ db, setup, received, gateway, send, startGateway }) => {
        try {
          const pending = send().catch((error: unknown) => error);
          const attempt = await onlyRequest(db);
          await eventually(async () => received.length === 1, "upstream request");
          gateway.kill("SIGKILL");
          await gateway.exited();
          const restarted = await startGateway();
          hold.release();
          expect(await pending).toBeInstanceOf(Error);
          await Bun.sleep(500);
          expect(received).toHaveLength(1);
          expect(
            (
              await db`SELECT p.state AS permit,h.state AS hold,r.execution_closed_at IS NULL AS open FROM execution_permits p JOIN upstream_holds h ON h.attempt_id=p.attempt_id JOIN gateway_requests r ON r.id=${attempt.request_id} WHERE p.attempt_id=${attempt.id}`
            )[0],
          ).toEqual({ permit: "SEND_GRANTED", hold: "HELD", open: true });
          expect(
            (
              await db`SELECT count(*)::integer n FROM core_evidence_receipts WHERE attempt_id=${attempt.id}`
            )[0].n,
          ).toBe(0);
          expect(
            (
              await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
            )[0].reserved_atoms,
          ).toBe("344");
          const next = await send({ via: restarted });
          expect(next.status).toBe(200);
          await next.text();
          expect(received).toHaveLength(2);
        } finally {
          hold.release();
        }
      },
    );
  },
  timeout,
);

test(
  "a lost begin acknowledgement is resolved by status query: the grant is never used and the request settles as not executed",
  async () => {
    const proxy = await createCommitAckProxy();
    try {
      await withGatewayStack(
        { reply: () => chatReplies.json(), databasePort: proxy.port },
        async ({ db, setup, received, send }) => {
          const observed = proxy.arm(["core_execution_batch_v1", "execution_begin_v1"], 30000);
          const response = await send();
          await observed;
          expect(response.status).toBe(503);
          expect((await response.json()).error.code).toBe("core_unavailable");
          expect(received).toHaveLength(0);
          const requestId = response.headers.get("x-request-id")!;
          expect(await requestOutcome(db, requestId)).toMatchObject({
            state: "FAILED",
            charged: "0",
          });
          expect(
            await db`SELECT a.state,p.state AS permit FROM attempts a JOIN execution_permits p ON p.attempt_id=a.id WHERE a.request_id=${requestId}`,
          ).toEqual([{ state: "NOT_EXECUTED", permit: "SEND_GRANTED" }]);
          expect(
            (
              await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
            )[0].reserved_atoms,
          ).toBe("0");
        },
      );
    } finally {
      await proxy.close();
    }
  },
  timeout,
);

test(
  "a lost admission acknowledgement is resolved by status query: the committed reservation is aborted and both parties' funds are released instead of staying reserved",
  async () => {
    const proxy = await createCommitAckProxy();
    try {
      await withGatewayStack(
        { reply: () => chatReplies.json(), databasePort: proxy.port },
        async ({ db, setup, received, send }) => {
          const observed = proxy.arm(["core_execution_batch_v1", "execution_admit_v1"], 30000);
          const response = await send();
          await observed;
          expect(response.status).toBe(503);
          await response.text();
          expect(received).toHaveLength(0);
          const requestId = response.headers.get("x-request-id")!;
          expect(await requestOutcome(db, requestId)).toMatchObject({
            state: "FAILED",
            charged: "0",
          });
          expect(
            await db`SELECT p.state AS permit FROM attempts a JOIN execution_permits p ON p.attempt_id=a.id WHERE a.request_id=${requestId}`,
          ).toEqual([{ permit: "ABORTED" }]);
          expect(
            (
              await db`SELECT reserved_atoms::text FROM upstream_account_balances WHERE account_id=${setup.provider.account_id}`
            )[0].reserved_atoms,
          ).toBe("0");
          expect(
            (
              await db`SELECT reserved_atoms::text FROM wallets WHERE project_id=${setup.project.id}`
            )[0].reserved_atoms,
          ).toBe("0");
        },
      );
    } finally {
      await proxy.close();
    }
  },
  timeout,
);

test(
  "a killed plugin helper fails the in-flight stream without a terminal or re-send, and the next request uses a restarted helper",
  () =>
    withGatewayStack(
      {
        reply: (call) =>
          call.index === 0
            ? slowStream(chatFrames().slice(0, 1), 100, () => undefined)
            : chatReplies.json(),
      },
      async ({ db, received, gateway, send }) => {
        const response = await send({ stream: true });
        const reader = response.body!.getReader();
        await reader.read();
        const helpers = execFileSync("pgrep", ["-P", String(gateway.pid)])
          .toString()
          .trim()
          .split("\n")
          .filter(Boolean);
        expect(helpers.length).toBeGreaterThan(0);
        for (const pid of helpers) process.kill(Number(pid), "SIGKILL");
        let text = "";
        for (;;) {
          const next = await reader.read();
          if (next.done) break;
          text += new TextDecoder().decode(next.value);
        }
        expect(text).toContain("gateway_stream_error");
        expect(text).not.toContain("[DONE]");
        const requestId = response.headers.get("x-request-id")!;
        expect(await requestOutcome(db, requestId)).toMatchObject({
          state: "FAILED",
          charged: "0",
        });
        await eventually(
          async () => (await leaseStates(db, requestId)).join() === "RELEASED",
          "released quota leases",
        );
        expect(received).toHaveLength(1);
        const next = await send();
        const nextText = await next.text();
        expect({ status: next.status, body: next.status === 200 ? "" : nextText }).toEqual({
          status: 200,
          body: "",
        });
        expect(received).toHaveLength(2);
      },
    ),
  timeout,
);

async function redisDump(url: string, pattern: string) {
  const redis = new RedisClient(url);
  try {
    const values: unknown[] = [];
    for (const key of (await redis.send("KEYS", [pattern])) as string[]) {
      const type = (await redis.send("TYPE", [key])) as string;
      values.push(
        type === "string"
          ? await redis.send("GET", [key])
          : type === "hash"
            ? await redis.send("HGETALL", [key])
            : type === "list"
              ? await redis.send("LRANGE", [key, "0", "-1"])
              : type === "zset"
                ? await redis.send("ZRANGE", [key, "0", "-1"])
                : type === "set"
                  ? await redis.send("SMEMBERS", [key])
                  : type === "stream"
                    ? await redis.send("XRANGE", [key, "-", "+"])
                    : type,
      );
    }
    return JSON.stringify(values);
  } finally {
    redis.close();
  }
}

test(
  "prompt and completion canaries never reach PostgreSQL, Redis, the evidence queue or process logs",
  () => {
    const id = crypto.randomUUID(),
      prompt = `prompt-canary-${id}`,
      output = `output-canary-${id}`;
    return withGatewayStack(
      {
        reply: (call) =>
          call.body.stream
            ? chatReplies.sse([
                {
                  id: "c",
                  model: "synthetic",
                  choices: [{ index: 0, delta: { content: output }, finish_reason: "stop" }],
                },
                ...chatFrames().slice(2),
              ])
            : chatReplies.json(output),
      },
      async ({ f, db, namespace, gateway, worker, send }) => {
        for (const stream of [false, true]) {
          const response = await send({ stream, content: prompt });
          expect(await response.text()).toContain(output);
          await settledCharge(db, response.headers.get("x-request-id")!);
        }
        const logs = [
          (await gateway.stop()) && (await gateway.output()),
          (await worker.stop()) && (await worker.output()),
        ].join("\n");
        const stores = [
          logs,
          await redisDump(process.env.QUEUE_REDIS_URL!, `*${namespace}*`),
          await redisDump(process.env.REDIS_URL!, `*${f.namespace}*`),
        ];
        for (const table of await db`SELECT tablename FROM pg_tables WHERE schemaname='public'`) {
          if (!/^[a-z_][a-z0-9_]*$/.test(table.tablename)) throw new Error("table identifier");
          stores.push(
            JSON.stringify(
              await db.unsafe(`SELECT row_to_json(t)::text AS body FROM ${table.tablename} t`),
            ),
          );
        }
        for (const store of stores) {
          expect(store).not.toContain(prompt);
          expect(store).not.toContain(output);
        }
      },
    );
  },
  timeout,
);

test(
  "a long attempt renews its quota leases until the request closes",
  () => {
    const hold = stall();
    return withGatewayStack(
      {
        reply: async () => {
          await hold.released;
          return chatReplies.json();
        },
        gateway: { quota_renew_ms: 200 },
      },
      async ({ db, received, send }) => {
        try {
          const pending = send();
          const attempt = await onlyRequest(db);
          await eventually(async () => received.length === 1, "upstream request");
          const revisions = async (): Promise<number[]> =>
            (
              await db`SELECT lease_id,(descriptor->>'revision')::numeric AS revision FROM execution_quota_leases WHERE request_id=${attempt.request_id} AND state='ACTIVE' ORDER BY lease_id`
            ).map((row: { revision: string }) => Number(row.revision));
          const before = await revisions();
          expect(before.length).toBeGreaterThan(0);
          await Bun.sleep(1000);
          const after = await revisions();
          expect(after.length).toBe(before.length);
          after.forEach((revision, index) => expect(revision).toBeGreaterThan(before[index]!));
          hold.release();
          const response = await pending;
          expect(response.status).toBe(200);
          await response.text();
          await eventually(
            async () => (await leaseStates(db, attempt.request_id)).join() === "RELEASED",
            "released quota leases",
          );
        } finally {
          hold.release();
        }
      },
    );
  },
  timeout,
);

test(
  "a supplier account awaiting reconciliation makes the v2 Gateway answer 503 no_route without calling upstream instead of core_unavailable",
  () =>
    withGatewayStack({ reply: () => chatReplies.json() }, async ({ db, setup, received, send }) => {
      await db`UPDATE upstream_accounts SET state='RECONCILE_REQUIRED',revision=revision+1 WHERE id=${setup.provider.account_id}`;
      const response = await send();
      expect(response.status).toBe(503);
      expect((await response.json()).error.code).toBe("no_route");
      expect(received).toHaveLength(0);
      expect((await db`SELECT count(*)::integer n FROM billing_holds`)[0].n).toBe(0);
    }),
  timeout,
);
