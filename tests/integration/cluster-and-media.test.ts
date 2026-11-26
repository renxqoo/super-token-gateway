import { expect, test } from "bun:test";
import { once } from "node:events";
import { createAsyncTasksRepository } from "@gateway/ledger";
import { createGatewayRuntime } from "@gateway/gateway";
import {
  makeConfig,
  db,
  waitForRedisState,
  redisUrl,
  internalCall,
  redisProc,
  startEphemeralRedis,
} from "./cluster-and-media-fixture.ts";
import { registerClusterFixture } from "./cluster-and-media-fixture.ts";
import { registerMultimodalCases } from "./multimodal-routes-cases.ts";
import { registerPermitCases } from "./redis-permits-cases.ts";
import { registerMinimaxCases } from "./minimax-routes-cases.ts";

registerClusterFixture();

test("public IP quota is atomic across gateways and fails closed when the Redis connection is lost", async () => {
  const config = makeConfig({
    redisNamespace: `stg_ip_${crypto.randomUUID()}`,
    trustedProxyCidrs: ["127.0.0.1/32"],
    ingressIp: {
      mode: "enforce",
      windowMs: 10_000,
      maxRequests: 2,
      nodeMaxRequests: 32,
      maxPending: 16,
      timeoutMs: 200,
    },
  });
  const first = createGatewayRuntime({ config, db, wasmExecutor: null });
  const second = createGatewayRuntime({ config, db, wasmExecutor: null });
  const models = (forwarded: string) =>
    new Request("http://gateway.test/v1/models", {
      headers: { "x-forwarded-for": forwarded },
    });
  try {
    await Promise.all([first.start(), second.start()]);
    await waitForRedisState(true, first.cluster, second.cluster);
    const burst = await Promise.all(
      Array.from({ length: 8 }, (_, index) =>
        (index % 2 ? first : second).fetchPublic(models(`198.51.100.${index + 1}`), {
          peerAddress: index % 2 ? "192.0.2.1" : "::ffff:c000:201",
        }),
      ),
    );
    expect(burst.map((response) => response.status).sort()).toEqual([
      401, 401, 429, 429, 429, 429, 429, 429,
    ]);
    const limited = burst.find((response) => response.status === 429)!;
    expect(Number(limited.headers.get("retry-after"))).toBeGreaterThan(0);
    expect(await limited.json()).toMatchObject({ error: { code: "ip_rate_limited" } });
    expect(
      (await first.fetchPublic(models("192.0.2.2"), { peerAddress: "127.0.0.1" })).status,
    ).toBe(401);
    expect((await first.fetchPublic(new Request("http://gateway.test/livez"))).status).toBe(200);

    const control = new Bun.RedisClient(redisUrl);
    try {
      expect(await control.send("CLIENT", ["PAUSE", "600", "ALL"])).toBe("OK");
      const paused = await first.fetchPublic(models("192.0.2.4"), { peerAddress: "127.0.0.1" });
      expect(paused.status).toBe(503);
      expect(await paused.json()).toMatchObject({ error: { code: "ingress_quota_unavailable" } });
      expect(first.cluster.isRedisConnected()).toBe(true);
      expect(await control.send("PING", [])).toBe("PONG");
      expect(
        (await first.fetchPublic(models("192.0.2.5"), { peerAddress: "127.0.0.1" })).status,
      ).toBe(401);
    } finally {
      control.close();
    }

    await first.cluster.close();
    const unavailable = await first.fetchPublic(models("192.0.2.3"), { peerAddress: "127.0.0.1" });
    expect(unavailable.status).toBe(503);
    expect(await unavailable.json()).toMatchObject({
      error: { code: "ingress_quota_unavailable" },
    });
  } finally {
    await Promise.all([first.close(), second.close()]);
  }
});

test("symptom: corrupted non-numeric Redis ingress counter admits traffic and overwrites key instead of failing closed", async () => {
  const namespace = `stg_ip_corrupt_${crypto.randomUUID()}`;
  const config = makeConfig({
    redisNamespace: namespace,
    trustedProxyCidrs: ["127.0.0.1/32"],
    ingressIp: {
      mode: "enforce",
      windowMs: 10_000,
      maxRequests: 2,
      nodeMaxRequests: 32,
      maxPending: 16,
      timeoutMs: 200,
    },
  });
  const runtime = createGatewayRuntime({ config, db, wasmExecutor: null });
  const control = new Bun.RedisClient(redisUrl);
  const models = (forwarded: string) =>
    new Request("http://gateway.test/v1/models", {
      headers: { "x-forwarded-for": forwarded },
    });
  try {
    await Promise.all([runtime.start(), control.connect()]);
    await waitForRedisState(true, runtime.cluster);
    expect(
      (await runtime.fetchPublic(models("192.0.2.10"), { peerAddress: "127.0.0.1" })).status,
    ).toBe(401);

    const corruptKey = `${namespace}:ingress:ip:192.0.2.11`;
    await control.send("SET", [corruptKey, "corrupt"]);
    const single = await runtime.fetchPublic(models("192.0.2.11"), { peerAddress: "127.0.0.1" });
    expect(single.status).toBe(503);
    expect(await single.json()).toMatchObject({ error: { code: "ingress_quota_unavailable" } });
    expect(await control.send("GET", [corruptKey])).toBe("corrupt");

    const [badInBatch, goodInBatch] = await Promise.all([
      runtime.fetchPublic(models("192.0.2.11"), { peerAddress: "127.0.0.1" }),
      runtime.fetchPublic(models("192.0.2.12"), { peerAddress: "127.0.0.1" }),
    ]);
    expect(badInBatch.status).toBe(503);
    expect(await badInBatch.json()).toMatchObject({ error: { code: "ingress_quota_unavailable" } });
    expect(goodInBatch.status).toBe(401);
    expect(await control.send("GET", [corruptKey])).toBe("corrupt");
  } finally {
    control.close();
    await runtime.close();
  }
});

test("symptom: idle ingress Redis connection closed by server causes fail-closed 503 burst on healthy Redis", async () => {
  const namespace = `stg_ip_idle_${crypto.randomUUID()}`;
  const config = makeConfig({
    redisNamespace: namespace,
    trustedProxyCidrs: ["127.0.0.1/32"],
    ingressIp: {
      mode: "enforce",
      windowMs: 10_000,
      maxRequests: 16,
      nodeMaxRequests: 64,
      maxPending: 32,
      timeoutMs: 250,
    },
  });
  const runtime = createGatewayRuntime({ config, db, wasmExecutor: null });
  const control = new Bun.RedisClient(redisUrl);
  const parseClientIds = (raw: string) =>
    raw
      .split("\n")
      .map((line) => line.match(/\bid=(\d+)\b/)?.[1])
      .filter((id): id is string => Boolean(id));
  const models = (forwarded: string) =>
    new Request("http://gateway.test/v1/models", {
      headers: { "x-forwarded-for": forwarded },
    });
  try {
    await Promise.all([runtime.start(), control.connect()]);
    await waitForRedisState(true, runtime.cluster);
    const beforeIds = new Set(parseClientIds(String(await control.send("CLIENT", ["LIST"]))));
    expect(
      (await runtime.fetchPublic(models("192.0.2.20"), { peerAddress: "127.0.0.1" })).status,
    ).toBe(401);
    const afterIds = parseClientIds(String(await control.send("CLIENT", ["LIST"])));
    const ingressIds = afterIds.filter((id) => !beforeIds.has(id));
    expect(ingressIds.length).toBeGreaterThanOrEqual(1);
    for (const id of ingressIds) {
      await control.send("CLIENT", ["KILL", "ID", id]);
    }
    await Bun.sleep(40);
    expect(runtime.cluster.isRedisConnected()).toBe(true);
    const resumed = await Promise.all([
      runtime.fetchPublic(models("192.0.2.21"), { peerAddress: "127.0.0.1" }),
      runtime.fetchPublic(models("192.0.2.22"), { peerAddress: "127.0.0.1" }),
      runtime.fetchPublic(models("192.0.2.23"), { peerAddress: "127.0.0.1" }),
      runtime.fetchPublic(models("192.0.2.24"), { peerAddress: "127.0.0.1" }),
    ]);
    expect(resumed.map((r) => r.status)).toEqual([401, 401, 401, 401]);
  } finally {
    control.close();
    await runtime.close();
  }
});

test("HTTP request size and the node body budget are enforced for JSON, multipart and streamed uploads", async () => {
  const boundary = "gateway-ingress-fixture";
  const contentType = `multipart/form-data; boundary=${boundary}`;
  const multipart = new TextEncoder().encode(
    `--${boundary}\r\nContent-Disposition: form-data; name="model"\r\n\r\nfixture-missing\r\n` +
      `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="clip.wav"\r\n` +
      `Content-Type: audio/wav\r\n\r\nfixture\r\n--${boundary}--\r\n`,
  );
  const maxBodyBytes = multipart.byteLength;
  const config = makeConfig({
    maxBodyBytes,
    ingressBodyBufferBytes: maxBodyBytes,
    bodyReadTimeoutMs: 1000,
  });
  const runtime = createGatewayRuntime({ config, db, wasmExecutor: null });
  const abort = new AbortController();
  let server: ReturnType<typeof Bun.serve> | undefined;
  try {
    await runtime.start();
    const billingAccount = await internalCall(
      runtime.internalApp,
      "PUT",
      "/internal/v1/billing-accounts",
      {
        name: "body-budget",
      },
    );
    expect(billingAccount.status).toBe(200);
    const key = await internalCall(runtime.internalApp, "PUT", "/internal/v1/api-keys", {
      name: "body-budget",
      billing_account_id: billingAccount.data.id,
    });
    expect(key.status).toBe(200);
    const auth = { authorization: `Bearer ${key.data.key}` };
    const request = JSON.stringify({ model: "fixture-missing" });
    const json = request + " ".repeat(maxBodyBytes - request.length);
    server = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      fetch(req, listener) {
        return runtime.fetchPublic(req, { getPeerAddress: () => listener.requestIP(req)?.address });
      },
    });
    const chat = new URL("/v1/chat/completions", server.url);
    const transcription = new URL("/v1/audio/transcriptions", server.url);
    const post = (url: URL, body: BodyInit, headers: Record<string, string> = {}) =>
      fetch(url, { method: "POST", headers: { ...auth, ...headers }, body });
    const compressed = await post(chat, Bun.gzipSync(new TextEncoder().encode(request)), {
      "content-type": "application/json",
      "content-encoding": "gzip",
    });
    expect(compressed.status).toBe(415);
    expect(compressed.headers.get("accept-encoding")).toBe("identity");
    expect(await compressed.json()).toMatchObject({
      error: { code: "unsupported_content_encoding" },
    });
    const plain = await post(chat, json, { "content-encoding": "identity" });
    expect(plain.status).toBe(404);
    expect(await plain.json()).toMatchObject({ error: { code: "model_not_found" } });
    expect((await post(chat, json + " ")).status).toBe(413);
    const transcribed = await post(transcription, multipart, { "content-type": contentType });
    expect(transcribed.status).toBe(404);
    expect(await transcribed.json()).toMatchObject({ error: { code: "model_not_found" } });
    const large = new Uint8Array(maxBodyBytes + 1);
    large.set(multipart);
    expect((await post(transcription, large, { "content-type": contentType })).status).toBe(413);
    const chunked = new ReadableStream({
      start(controller) {
        controller.enqueue(new TextEncoder().encode(json + " "));
        controller.close();
      },
    });
    expect((await post(chat, chunked)).status).toBe(413);

    const reading = Promise.withResolvers<void>();
    const pending = runtime.fetchPublic(
      new Request(chat, {
        method: "POST",
        headers: auth,
        signal: abort.signal,
        body: new ReadableStream(
          {
            pull() {
              reading.resolve();
            },
          },
          { highWaterMark: 0 },
        ),
      }),
    );
    await reading.promise;
    const full = await post(chat, request);
    expect(full.status).toBe(503);
    expect(await full.json()).toMatchObject({
      error: { code: "request_body_capacity_unavailable" },
    });
    abort.abort();
    expect((await pending).status).toBe(499);
    expect((await post(chat, json)).status).toBe(404);
    let cancelled = false;
    const timedOut = await runtime.fetchPublic(
      new Request(chat, {
        method: "POST",
        headers: { ...auth, "content-length": String(maxBodyBytes) },
        body: new ReadableStream({
          cancel() {
            cancelled = true;
          },
        }),
      }),
    );
    expect(timedOut.status).toBe(408);
    expect(cancelled).toBe(true);
    expect((await post(chat, json)).status).toBe(404);
    expect(
      (await (await runtime.metricsResponse()).text()).match(
        /^gateway_ingress_body_buffer_reserved_bytes 0$/m,
      ),
    ).not.toBeNull();
  } finally {
    abort.abort();
    await server?.stop(true);
    await runtime.close();
  }
});

test("symptom: bursts of readiness and metrics checks each open a PostgreSQL probe", async () => {
  const config = makeConfig({ redisNamespace: `stg_readiness_${crypto.randomUUID()}` });
  let observe = false;
  let probes = 0;
  let databaseUnavailable = false;
  const observedDatabase = new Proxy(db, {
    apply(target, receiver, args) {
      if (observe) {
        probes++;
        if (databaseUnavailable) return Promise.reject(new Error("Database unavailable"));
      }
      return Reflect.apply(target, receiver, args);
    },
  });
  const runtime = createGatewayRuntime({ config, db: observedDatabase, wasmExecutor: null });
  try {
    await runtime.start();
    await waitForRedisState(true, runtime.cluster);
    observe = true;
    const [metrics, ...responses] = await Promise.all([
      runtime.metricsResponse(),
      ...Array.from({ length: 12 }, () => runtime.publicApp.request("/readyz")),
    ]);
    expect(responses.every((response) => response.status === 200)).toBe(true);
    expect(await metrics.text()).toContain("gateway_ready 1");
    expect(probes).toBe(1);
    expect((await runtime.publicApp.request("/readyz")).status).toBe(200);
    expect(probes).toBe(1);

    databaseUnavailable = true;
    await Bun.sleep(300);
    const unavailable = await runtime.publicApp.request("/readyz");
    expect(unavailable.status).toBe(503);
    expect(await unavailable.json()).toEqual({ status: "db_unavailable" });
    expect((await runtime.publicApp.request("/readyz")).status).toBe(503);
    expect(probes).toBe(2);
    runtime.lifecycle.accepting = false;
    const draining = await runtime.publicApp.request("/readyz");
    expect(await draining.json()).toEqual({ status: "not_ready" });
    expect(probes).toBe(2);
  } finally {
    observe = false;
    await runtime.close();
  }
});

test("symptom: missing and completed video queries and cancellations bypass the shared billingAccount rate limit", async () => {
  const config = makeConfig({ redisNamespace: `stg_video_quota_${crypto.randomUUID()}` });
  const first = createGatewayRuntime({ config, db, wasmExecutor: null });
  const second = createGatewayRuntime({ config, db, wasmExecutor: null });
  try {
    await first.start();
    async function configure(path: string, body: Record<string, unknown>) {
      const { status, data } = await internalCall(first.internalApp, "PUT", path, body);
      expect(status).toBe(200);
      return data;
    }
    const billingAccount = await configure("/internal/v1/billing-accounts", {
      name: "video-quota",
      rpm: 3,
    });
    const other = await configure("/internal/v1/billing-accounts", {
      name: "other-video-quota",
      rpm: 1,
    });
    const primaryKey = await configure("/internal/v1/api-keys", {
      name: "video-quota",
      billing_account_id: billingAccount.id,
      operations: ["videos"],
    });
    const otherKey = await configure("/internal/v1/api-keys", {
      name: "other-video-quota",
      billing_account_id: other.id,
      operations: ["videos"],
    });
    const account = await configure("/internal/v1/upstream-accounts", { name: "video-quota" });
    const channel = await configure("/internal/v1/channels", {
      name: "video-quota",
      account_id: account.id,
      kind: "openai",
      base_url: "http://127.0.0.1:1",
      api_key: "local-fixture",
    });
    const repository = createAsyncTasksRepository(db);
    const task = (billingAccountId: unknown, keyId: unknown, status: "QUEUED" | "SUCCEEDED") =>
      repository.createTask({
        id: `vid_${crypto.randomUUID().replaceAll("-", "")}`,
        request_id: crypto.randomUUID(),
        billing_account_id: String(billingAccountId),
        charge_id: null,
        api_key_id: String(keyId),
        channel_id: String(channel.id),
        model: "fixture-video",
        operation: "videos",
        upstream_task_id: "fixture-task",
        status,
      });
    const [completed, queued, otherQueued] = await Promise.all([
      task(billingAccount.id, primaryKey.id, "SUCCEEDED"),
      task(billingAccount.id, primaryKey.id, "QUEUED"),
      task(other.id, otherKey.id, "QUEUED"),
    ]);
    await second.start();
    await waitForRedisState(true, first.cluster, second.cluster);
    const call = (node: typeof first, key: unknown, id: string, method: "GET" | "POST" = "GET") =>
      node.publicApp.request(`/v1/videos/${id}${method === "POST" ? "/cancel" : ""}`, {
        method,
        headers: { authorization: `Bearer ${key}` },
      });

    const missing = `vid_${crypto.randomUUID().replaceAll("-", "")}`;
    expect((await call(first, primaryKey.key, missing)).status).toBe(404);
    expect((await call(first, primaryKey.key, otherQueued.id)).status).toBe(404);
    const own = await call(second, primaryKey.key, completed.id);
    expect(own.status).toBe(200);
    expect(await own.json()).toMatchObject({ status: "SUCCEEDED" });

    const limited = await Promise.all([
      call(first, primaryKey.key, completed.id),
      call(second, primaryKey.key, queued.id, "POST"),
    ]);
    expect(limited.map((response) => response.status)).toEqual([429, 429]);
    for (const response of limited) {
      expect(await response.json()).toMatchObject({ error: { code: "rate_limit_rpm" } });
    }
    expect((await repository.getTask(queued.id))?.status).toBe("QUEUED");

    const cancelled = await call(second, otherKey.key, otherQueued.id, "POST");
    expect(cancelled.status).toBe(200);
    expect(await cancelled.json()).toMatchObject({ status: "CANCELLED" });
    expect((await call(first, otherKey.key, otherQueued.id)).status).toBe(429);
  } finally {
    await Promise.all([first.close(), second.close()]);
  }
});

test("Redis cluster syncs rate limits, circuit breaker OPEN state, and config epochs across Node A and Node B in real time", async () => {
  let primaryCalls = 0;
  let backupCalls = 0;

  const primaryServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch() {
      primaryCalls++;
      return Response.json({ error: { message: "primary down" } }, { status: 503 });
    },
  });
  const backupServer = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch() {
      backupCalls++;
      return Response.json({
        id: "cmpl_backup",
        object: "chat.completion",
        model: "backup-gpt",
        choices: [
          { index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" },
        ],
        usage: { prompt_tokens: 5, completion_tokens: 5, total_tokens: 10 },
      });
    },
  });

  const nodeA = createGatewayRuntime({
    config: makeConfig(),
    db,
    routerOptions: { failureThreshold: 3, baseOpenMs: 30_000 },
  });
  const nodeB = createGatewayRuntime({
    config: makeConfig({ snapshotPollMs: 250 }),
    db,
    routerOptions: { failureThreshold: 3, baseOpenMs: 30_000 },
  });

  try {
    await nodeA.start();
    await nodeB.start();
    expect(nodeA.cluster.isRedisConnected()).toBe(true);
    expect(nodeB.cluster.isRedisConnected()).toBe(true);

    const { data: billingAccount } = await internalCall(
      nodeA.internalApp,
      "PUT",
      "/internal/v1/billing-accounts",
      {
        name: "cluster-proj",
        currency: "USD",
        rpm: 5,
        tpm: 1_000_000,
        concurrency: 10,
      },
    );
    const billingAccountId = String(billingAccount.id);
    const { data: keyRes } = await internalCall(nodeA.internalApp, "PUT", "/internal/v1/api-keys", {
      billing_account_id: billingAccountId,
      name: "cluster-key",
    });
    const apiKey = String(keyRes.key);

    await internalCall(
      nodeA.internalApp,
      "POST",
      `/internal/v1/wallets/${billingAccountId}/credits`,
      {
        currency: "USD",
        external_ref: "cluster-credit-1",
        amount_atoms: "1000000",
        kind: "CASH",
      },
    );
    const { data: acc } = await internalCall(
      nodeA.internalApp,
      "PUT",
      "/internal/v1/upstream-accounts",
      {
        name: "cluster-acc",
      },
    );
    await internalCall(
      nodeA.internalApp,
      "POST",
      `/internal/v1/upstream-accounts/${acc.id}/recharges`,
      {
        currency: "USD",
        external_ref: "cluster-sup-1",
        amount_atoms: "1000000",
      },
    );

    const { data: chP } = await internalCall(nodeA.internalApp, "PUT", "/internal/v1/channels", {
      account_id: acc.id,
      name: "cluster-primary",
      kind: "openai",
      base_url: `http://127.0.0.1:${primaryServer.port}`,
      api_key: "sk-p",
    });
    const { data: chB } = await internalCall(nodeA.internalApp, "PUT", "/internal/v1/channels", {
      account_id: acc.id,
      name: "cluster-backup",
      kind: "openai",
      base_url: `http://127.0.0.1:${backupServer.port}`,
      api_key: "sk-b",
    });
    await internalCall(nodeA.internalApp, "PUT", "/internal/v1/models", {
      name: "cluster-gpt",
      operations: ["chat"],
      max_output_tokens: 64,
    });
    const { data: custPrice } = await internalCall(
      nodeA.internalApp,
      "POST",
      "/internal/v1/prices",
      {
        side: "CUSTOMER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "10", denominator: "1" },
          output_tokens: { numerator: "20", denominator: "1" },
        },
        ceilings: { input_tokens: "1000", output_tokens: "64" },
      },
    );
    const { data: supPrice } = await internalCall(
      nodeA.internalApp,
      "POST",
      "/internal/v1/prices",
      {
        side: "SUPPLIER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "2", denominator: "1" },
          output_tokens: { numerator: "4", denominator: "1" },
        },
        ceilings: { input_tokens: "1000", output_tokens: "64" },
      },
    );
    await internalCall(nodeA.internalApp, "PUT", "/internal/v1/routes", {
      model_name: "cluster-gpt",
      channel_id: chP.id,
      upstream_model: "p-up",
      operations: ["chat"],
      priority: 10,
      billing_price_id: custPrice.id,
      cost_version_id: supPrice.id,
    });
    await internalCall(nodeA.internalApp, "PUT", "/internal/v1/routes", {
      model_name: "cluster-gpt",
      channel_id: chB.id,
      upstream_model: "b-up",
      operations: ["chat"],
      priority: 5,
      billing_price_id: custPrice.id,
      cost_version_id: supPrice.id,
    });

    const { data: syncedRevision } = await internalCall(
      nodeA.internalApp,
      "PUT",
      "/internal/v1/plugins",
      {
        name: "redis-sync-vendor",
        version: "1.0.0",
        capability: "PROVIDER",
        kind: "DECLARATIVE",
        provider_config: {
          base_dialect: "openai_chat",
          usage_dialect: "openai",
          auth: { scheme: "bearer" },
          endpoints: { default: { path: "/v1/chat/completions" } },
        },
      },
    );

    const activated = await internalCall(
      nodeA.internalApp,
      "POST",
      "/internal/v1/plugin-releases",
      {
        id: crypto.randomUUID(),
        expected_epoch: syncedRevision.config_epoch,
        targets: [{ plugin_id: syncedRevision.id }],
      },
    );
    expect(activated.status, JSON.stringify(activated.data)).toBe(200);

    await Bun.sleep(100);
    expect(nodeB.snapshot.get().models.has("cluster-gpt")).toBe(true);
    expect(nodeB.snapshot.get().providerPluginsByName.has("redis-sync-vendor")).toBe(true);

    const originalPublish = nodeA.cluster.publishConfigEpoch.bind(nodeA.cluster);
    nodeA.cluster.publishConfigEpoch = () => undefined;
    try {
      const { status, data } = await internalCall(
        nodeA.internalApp,
        "POST",
        "/internal/v1/plugin-releases",
        {
          id: crypto.randomUUID(),
          expected_epoch: nodeA.snapshot.get().epoch,
          targets: [{ channel_id: chB.id, provider_plugin_id: syncedRevision.id }],
        },
      );
      expect(status).toBe(200);
      const deadline = performance.now() + 3000;
      while (
        nodeB.snapshot.get().epoch < Number(data.config_epoch) &&
        performance.now() < deadline
      ) {
        await Bun.sleep(20);
      }
      expect(nodeB.snapshot.get().channels.get(String(chB.id))?.provider_plugin_id).toBe(
        String(syncedRevision.id),
      );
    } finally {
      nodeA.cluster.publishConfigEpoch = originalPublish;
    }

    const callNode = (node: typeof nodeA) =>
      node.publicApp.fetch(
        new Request("http://gw.local/v1/chat/completions", {
          method: "POST",
          headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json" },
          body: JSON.stringify({
            model: "cluster-gpt",
            messages: [{ role: "user", content: "ping" }],
            max_tokens: 32,
          }),
        }),
      );

    for (let i = 0; i < 3; i++) {
      const r = await callNode(nodeA);
      expect(r.status).toBe(200);
      expect(r.headers.get("x-gateway-channel")).toBe("cluster-backup");
    }
    expect(primaryCalls).toBe(3);

    await Bun.sleep(80);
    const r4 = await callNode(nodeB);
    expect(r4.status).toBe(200);
    expect(r4.headers.get("x-gateway-channel")).toBe("cluster-backup");
    expect(primaryCalls).toBe(3);

    const r5 = await callNode(nodeA);
    expect(r5.status).toBe(200);
    expect(backupCalls).toBe(5);

    const r6 = await callNode(nodeB);
    expect(r6.status).toBe(429);
    const r6Json = (await r6.json()) as { error: { code: string } };
    expect(r6Json.error.code).toBe("rate_limit_rpm");

    const active = await nodeA.cluster.acquireRateLimit(
      "outage",
      {
        rpm: 100,
        tpm: 100,
        concurrency: 1,
      },
      1,
    );
    if (!active.ok) throw new Error("Could not acquire the permit before testing the outage");
    const redisExited = once(redisProc!, "exit");
    redisProc!.kill("SIGTERM");
    await redisExited;
    await waitForRedisState(false, nodeA.cluster, nodeB.cluster);
    expect(active.signal?.aborted).toBe(false);
    expect((await nodeA.publicApp.request("/readyz")).status).toBe(503);
    const unavailable = await callNode(nodeB);
    expect(unavailable.status).toBe(503);
    expect(await unavailable.json()).toMatchObject({ error: { code: "rate_limit_unavailable" } });

    await startEphemeralRedis(Number(new URL(redisUrl).port));
    await waitForRedisState(true, nodeA.cluster, nodeB.cluster);
    expect(active.signal?.aborted).toBe(true);
    expect((await nodeB.publicApp.request("/readyz")).status).toBe(200);
    expect((await callNode(nodeA)).status).toBe(200);
  } finally {
    await nodeA.close();
    await nodeB.close();
    await primaryServer.stop(true);
    await backupServer.stop(true);
  }
});

registerMultimodalCases();
registerPermitCases();
registerMinimaxCases();
