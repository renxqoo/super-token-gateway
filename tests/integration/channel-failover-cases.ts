import { expect, test } from "bun:test";
import { createGatewayRuntime } from "@gateway/gateway";
import { fakeUpstream, makeConfig, db, setup, internalCall } from "./smart-routing-fixture.ts";

export function registerFailoverCases() {
  test("backup rebuilds the original chat request with its own adapter after the primary rejects a rewritten request", async () => {
    const sent: { channel: string; authorization: string | null; body: Record<string, unknown> }[] =
      [];
    const primary = fakeUpstream("rewrite-primary", {
      async respond(request) {
        sent.push({
          channel: "primary",
          authorization: request.headers.get("authorization"),
          body: await request.json(),
        });
        return Response.json({ error: { message: "Unavailable" } }, { status: 503 });
      },
    });
    const backup = fakeUpstream("rewrite-backup", {
      async respond(request) {
        sent.push({
          channel: "backup",
          authorization: request.headers.get("authorization"),
          body: await request.json(),
        });
        return Response.json({
          id: "backup-chat",
          model: "m",
          choices: [
            {
              index: 0,
              message: { role: "assistant", content: "backup answer" },
              finish_reason: "stop",
            },
          ],
          usage: { prompt_tokens: 9, completion_tokens: 4 },
        });
      },
    });
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const ctx = await setup(runtime, [primary, backup], {
        priority: [10, 0],
        maxAttempts: 2,
        upstreamKeys: ["primary-credential", "backup-credential"],
        adapterOverrides: (index) =>
          index === 0
            ? { request_transforms: [{ op: "move_to", from: "messages", to: "vendor_messages" }] }
            : { request_transforms: [{ op: "force_set", path: "temperature", value: 0.25 }] },
      });
      const response = await ctx.chat();
      expect(response.status).toBe(200);
      expect(await response.json()).toMatchObject({
        choices: [{ message: { content: "backup answer" } }],
      });
      expect(sent.map(({ channel, authorization }) => [channel, authorization])).toEqual([
        ["primary", "Bearer primary-credential"],
        ["backup", "Bearer backup-credential"],
      ]);
      expect(sent[0]?.body).toMatchObject({ vendor_messages: [{ role: "user", content: "hi" }] });
      expect(sent[0]?.body).not.toHaveProperty("messages");
      expect(sent[1]?.body).toMatchObject({
        messages: [{ role: "user", content: "hi" }],
        temperature: 0.25,
      });
      expect(sent[1]?.body).not.toHaveProperty("vendor_messages");
      expect(await ctx.charges()).toEqual({ n: 1, u: 1, atoms: "13" });
    } finally {
      backup.server.stop(true);
      primary.server.stop(true);
      await runtime.close();
    }
  });

  test("image edit failover preserves the uploaded file and repeated form fields after the primary consumes them", async () => {
    const received: {
      channel: string;
      tags: FormDataEntryValue[];
      filename: string;
      mime: string;
      bytes: number[];
    }[] = [];
    async function captureUpload(channel: string, request: Request): Promise<void> {
      const form = await request.formData();
      const image = form.get("image");
      if (!(image instanceof File)) throw new Error("Upstream did not receive the image file");
      received.push({
        channel,
        tags: form.getAll("tag"),
        filename: image.name,
        mime: image.type,
        bytes: Array.from(new Uint8Array(await image.arrayBuffer())),
      });
    }
    const primary = fakeUpstream("multipart-primary", {
      async respond(request) {
        await captureUpload("primary", request);
        return new Response(null, { status: 503 });
      },
    });
    const backup = fakeUpstream("multipart-backup", {
      async respond(request) {
        await captureUpload("backup", request);
        return Response.json({ data: [{ b64_json: "YWJj" }] });
      },
    });
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const fixture = await setup(runtime, [primary, backup], {
        operation: "image_edits",
        priority: [10, 0],
        maxAttempts: 2,
      });
      const form = new FormData();
      form.append("prompt", "Adjust contrast");
      form.append("tag", "first");
      form.append("tag", "second");
      form.append(
        "image",
        new File([new Uint8Array([137, 0, 255, 10])], "source.png", {
          type: "image/png",
        }),
      );
      const response = await fixture.editImage(form);
      expect(response.status).toBe(200);
      expect(await response.json()).toMatchObject({ data: [{ b64_json: "YWJj" }] });
      expect(received.map(({ channel }) => channel)).toEqual(["primary", "backup"]);
      for (const upload of received) {
        expect(upload).toMatchObject({
          tags: ["first", "second"],
          filename: "source.png",
          mime: "image/png",
          bytes: [137, 0, 255, 10],
        });
      }
      expect(await fixture.charges()).toEqual({ n: 1, u: 1, atoms: "5" });
    } finally {
      backup.server.stop(true);
      primary.server.stop(true);
      await runtime.close();
    }
  });

  test.each([
    ["the primary returns a client error", 400, 2, 400, "upstream_client_error"],
    ["the route permits one attempt", 503, 1, 502, "upstream_error"],
  ] as const)(
    "gateway stops before dispatching to a healthy backup when %s",
    async (_, upstreamStatus, maxAttempts, clientStatus, errorCode) => {
      const primary = fakeUpstream("stop-primary", { status: upstreamStatus });
      const backup = fakeUpstream("stop-backup");
      const runtime = createGatewayRuntime({ config: makeConfig(), db });
      try {
        await runtime.start();
        const ctx = await setup(runtime, [primary, backup], { priority: [10, 0], maxAttempts });
        const initialWallet = await ctx.wallet();
        const response = await ctx.chat();
        expect(response.status).toBe(clientStatus);
        expect(await response.json()).toMatchObject({ error: { code: errorCode } });
        expect(primary.calls()).toBe(1);
        expect(backup.calls()).toBe(0);
        expect(await ctx.holdsByState()).toEqual({ RELEASED: 1 });
        expect(await ctx.charges()).toEqual({ n: 0, u: 0, atoms: "0" });
        expect(await ctx.wallet()).toEqual(initialWallet);
      } finally {
        backup.server.stop(true);
        primary.server.stop(true);
        await runtime.close();
      }
    },
  );

  test.each(["heartbeat", "visible text"] as const)(
    "retryable SSE error after %s respects the client output boundary",
    async (prefixKind) => {
      const hasText = prefixKind === "visible text";
      const textFrame =
        'data: {"id":"primary-chat","model":"m","choices":[{"index":0,"delta":{"content":"primary output"},"finish_reason":null}]}\n\n';
      const errorFrame = 'data: {"error":{"code":"vendor_unavailable"}}\n\n';
      const primary = fakeUpstream("prelude-primary", {
        streamFrames: (hasText ? textFrame : ": upstream heartbeat\n\n") + errorFrame,
      });
      const backup = fakeUpstream("prelude-backup");
      const runtime = createGatewayRuntime({ config: makeConfig(), db });
      try {
        await runtime.start();
        const ctx = await setup(runtime, [primary, backup], {
          priority: [10, 0],
          maxAttempts: 2,
          adapterOverrides: {
            error_rules: [
              { path: "error.code", equals: "vendor_unavailable", retry_kind: "retryable" },
            ],
          },
        });
        const response = await ctx.chat(true);
        expect(response.status).toBe(200);
        const wire = await response.text();
        expect(primary.calls()).toBe(1);
        expect(backup.calls()).toBe(hasText ? 0 : 1);
        if (hasText) {
          expect(wire).toMatch(/primary output[\s\S]*upstream_stream_error/);
          expect(wire).not.toContain("data: [DONE]");
          expect(await ctx.charges()).toEqual({ n: 0, u: 0, atoms: "0" });
          expect(await ctx.holdsByState()).toEqual({ UNKNOWN: 1 });
          await ctx.resolveUnknownHolds();
        } else {
          expect(wire).toContain('"content":"prelude-backup"');
          expect(wire).not.toContain("event: error");
          expect(wire.trimEnd()).toEndWith("data: [DONE]");
          expect(await ctx.charges()).toEqual({ n: 1, u: 1, atoms: "15" });
          expect(await ctx.holdsByState()).toEqual({ SETTLED: 1 });
        }
      } finally {
        backup.server.stop(true);
        primary.server.stop(true);
        await runtime.close();
      }
    },
  );

  test("latency: a slow-failing primary near the attempt timeout adds at most one timeout of latency before switching; the client never waits for a second slow channel", async () => {
    const A = fakeUpstream("A", { status: 503, headerDelayMs: 2500 });
    const B = fakeUpstream("B");
    const runtime = createGatewayRuntime({ config: makeConfig({ attemptTimeoutMs: 3000 }), db });
    try {
      await runtime.start();
      const ctx = await setup(runtime, [A, B], { priority: [10, 0] });
      const res = await ctx.chat();
      const elapsedLabel = res.headers.get("x-gateway-channel");
      expect(res.status).toBe(200);
      expect(elapsedLabel?.startsWith("B-")).toBe(true);
      await res.text();
      await ctx.settle();
      const slow = await ctx.health();
      expect(slow[ctx.channels[0]!.id as string]!.consecutive_failures).toBeGreaterThanOrEqual(1);

      A.set({ status: 503, headerDelayMs: 4000 });
      B.set({ status: 503, headerDelayMs: 4000 });
      const before2 = await ctx.wallet();
      const t0 = performance.now();
      const failing = await ctx.chat();
      const waited = performance.now() - t0;
      expect(failing.status).toBe(504);
      expect(((await failing.json()) as { error: { code: string } }).error.code).toBe(
        "upstream_timeout",
      );
      expect(waited).toBeGreaterThan(2800);
      expect(waited).toBeLessThan(7000);
      await ctx.settle();
      const during = await ctx.wallet();
      expect(BigInt(during.spent_atoms)).toBe(BigInt(before2.spent_atoms));
      const unk =
        await db`SELECT held_atoms::text AS h FROM public.billing_holds WHERE billing_account_id = ${ctx.billingAccountId} AND state = 'UNKNOWN'`;
      expect(unk.length).toBe(1);
      expect(BigInt(during.reserved_atoms) - BigInt(before2.reserved_atoms)).toBe(
        BigInt(unk[0]!.h as string),
      );
      expect(await ctx.charges()).toMatchObject({ n: 1, u: 1 });
      expect(await ctx.holdsByState()).toEqual({ SETTLED: 1, UNKNOWN: 1 });
      const a = await ctx.audit();
      expect(a.ok).toBe(true);
      expect(a.open_holds).toBe(0);
      expect(a.unknown_holds).toBe(1);
    } finally {
      await runtime.close();
      A.server.stop(true);
      B.server.stop(true);
    }
  });

  test("exhaustion: every channel down returns the last upstream status with no stranded hold, no charge, and no retry storm (each channel hit once)", async () => {
    const A = fakeUpstream("A", { status: 503 });
    const B = fakeUpstream("B", { status: 429 });
    const C = fakeUpstream("C", { status: 500 });
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const ctx = await setup(runtime, [A, B, C], { priority: [20, 10, 0] });
      const before = await ctx.wallet();
      const res = await ctx.chat();
      expect(res.status).toBe(502);
      const body = (await res.json()) as { error: { code: string } };
      expect(body.error.code).toBe("upstream_error");
      expect(A.calls()).toBe(1);
      expect(B.calls()).toBe(1);
      expect(C.calls()).toBe(1);
      await ctx.settle();
      for (let i = 0; i < 100 && (await ctx.holdsByState()).RELEASED !== 1; i++)
        await Bun.sleep(10);
      const after = await ctx.wallet();
      expect(after).toEqual(before);
      expect(await ctx.charges()).toMatchObject({ n: 0 });
      expect(await ctx.holdsByState()).toEqual({ RELEASED: 1 });
      const a = await ctx.audit();
      expect(a.ok).toBe(true);
      expect(await ctx.holdsByState()).toEqual({ RELEASED: 1 });
    } finally {
      await runtime.close();
      for (const u of [A, B, C]) u.server.stop(true);
    }
  });

  test("exhaustion with only 429s surfaces 429 upstream_rate_limited to the client", async () => {
    const A = fakeUpstream("A", { status: 429 });
    const B = fakeUpstream("B", { status: 429 });
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const ctx = await setup(runtime, [A, B]);
      const res = await ctx.chat();
      expect(res.status).toBe(429);
      expect(((await res.json()) as { error: { code: string } }).error.code).toBe(
        "upstream_rate_limited",
      );
      await ctx.settle();
      for (let i = 0; i < 100 && (await ctx.holdsByState()).RELEASED !== 1; i++)
        await Bun.sleep(10);
      expect(await ctx.charges()).toMatchObject({ n: 0 });
      expect(await ctx.holdsByState()).toEqual({ RELEASED: 1 });
      expect((await ctx.audit()).ok).toBe(true);
    } finally {
      await runtime.close();
      A.server.stop(true);
      B.server.stop(true);
    }
  });

  test("dead channel: ECONNREFUSED on primary switches before dispatch, the customer is never charged for the dead attempt, and a later success closes the circuit via probe", async () => {
    const dead = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: () => new Response("x") });
    const deadPort = dead.port!;
    dead.stop(true);
    const B = fakeUpstream("B");
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const tag = crypto.randomUUID().slice(0, 6);
      const call = async (method: string, path: string, body?: unknown) => {
        const r = await internalCall(runtime.internalApp, method, path, body);
        expect(r.status, JSON.stringify(r.data)).toBe(path === "/internal/v1/prices" ? 201 : 200);
        return r.data as Record<string, unknown>;
      };
      const billingAccount = await call("PUT", "/internal/v1/billing-accounts", {
        name: `sr-dead-${tag}`,
        currency: "USD",
        rpm: 10_000_000,
        tpm: 100_000_000,
        concurrency: 5000,
      });
      await call("POST", `/internal/v1/wallets/${billingAccount.id}/credits`, {
        currency: "USD",
        external_ref: `d-${tag}`,
        amount_atoms: "1000000000",
        kind: "CASH",
      });
      const key = await call("PUT", "/internal/v1/api-keys", {
        billing_account_id: billingAccount.id,
        name: "k",
      });
      const account = await call("PUT", "/internal/v1/upstream-accounts", { name: `d-${tag}` });
      await call("POST", `/internal/v1/upstream-accounts/${account.id}/recharges`, {
        currency: "USD",
        external_ref: `ds-${tag}`,
        amount_atoms: "1000000000",
      });
      const deadCh = await call("PUT", "/internal/v1/channels", {
        account_id: account.id,
        kind: "openai",
        name: `dead-${tag}`,
        base_url: `http://127.0.0.1:${deadPort}/openai`,
        api_key: "k",
        rpm: 10_000_000,
        concurrency: 10_000,
      });
      const liveCh = await call("PUT", "/internal/v1/channels", {
        account_id: account.id,
        kind: "openai",
        name: `live-${tag}`,
        base_url: `${B.url}/openai`,
        api_key: "k",
        rpm: 10_000_000,
        concurrency: 10_000,
      });
      const customer = await call("POST", "/internal/v1/prices", {
        side: "CUSTOMER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "1", denominator: "1" },
          output_tokens: { numerator: "1", denominator: "1" },
        },
        ceilings: { input_tokens: "100", output_tokens: "100" },
      });
      const supplier = await call("POST", "/internal/v1/prices", {
        side: "SUPPLIER",
        currency: "USD",
        rates: {
          input_tokens: { numerator: "1", denominator: "1" },
          output_tokens: { numerator: "1", denominator: "1" },
        },
        ceilings: { input_tokens: "100", output_tokens: "100" },
      });
      const model = `sr-dead-${tag}`;
      await call("PUT", "/internal/v1/models", {
        name: model,
        operations: ["chat"],
        max_output_tokens: 50,
      });
      await call("PUT", "/internal/v1/routes", {
        model_name: model,
        channel_id: deadCh.id,
        upstream_model: "m",
        operations: ["chat"],
        billing_price_id: customer.id,
        cost_version_id: supplier.id,
        priority: 10,
        retry_policy: { max_attempts: 2 },
      });
      await call("PUT", "/internal/v1/routes", {
        model_name: model,
        channel_id: liveCh.id,
        upstream_model: "m",
        operations: ["chat"],
        billing_price_id: customer.id,
        cost_version_id: supplier.id,
        priority: 0,
        retry_policy: { max_attempts: 2 },
      });
      const chat = () =>
        runtime.publicApp.fetch(
          new Request("http://gateway.local/v1/chat/completions", {
            method: "POST",
            headers: { authorization: `Bearer ${key.key}`, "content-type": "application/json" },
            body: JSON.stringify({
              model,
              stream: false,
              messages: [{ role: "user", content: "x" }],
              max_tokens: 20,
            }),
          }),
        );
      const t0 = performance.now();
      const res = await chat();
      const sw = performance.now() - t0;
      expect(res.status).toBe(200);
      expect(res.headers.get("x-gateway-channel")).toBe(String(liveCh.name));
      expect(sw).toBeLessThan(1500);
      await res.text();
      for (let i = 0; i < 100 && runtime.lifecycle.size > 0; i++) await Bun.sleep(10);
      const charges =
        await db`SELECT count(*)::int AS n FROM public.billing_charges WHERE billing_account_id = ${billingAccount.id}`;
      expect(charges[0]!.n).toBe(1);
      const hold =
        await db`SELECT supplier_holds FROM public.billing_holds WHERE billing_account_id = ${billingAccount.id}`;
      const sup = hold[0]!.supplier_holds as { channel_id: string; state: string }[];
      expect(sup.some((x) => x.channel_id === String(deadCh.id) && x.state === "RELEASED")).toBe(
        true,
      );
      expect(sup.some((x) => x.channel_id === String(liveCh.id) && x.state === "SETTLED")).toBe(
        true,
      );
      const w = (
        await internalCall(runtime.internalApp, "GET", `/internal/v1/wallets/${billingAccount.id}`)
      ).data as { spent_atoms: string; reserved_atoms: string };
      expect(BigInt(w.spent_atoms)).toBe(15n);
      expect(BigInt(w.reserved_atoms)).toBe(0n);
    } finally {
      await runtime.close();
      B.server.stop(true);
    }
  });

  test("post-dispatch: upstream dies after SSE first byte -> no failover, estimated settlement, zero duplicate upstream calls", async () => {
    const A = fakeUpstream("A", { truncate: true });
    const B = fakeUpstream("B");
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const ctx = await setup(runtime, [A, B], { priority: [10, 0] });
      const res = await ctx.chat(true);
      expect(res.status).toBe(200);
      const text = await res.text();
      expect(text).not.toContain("[DONE]");
      expect(text).toContain("event: error");
      expect(A.calls()).toBe(1);
      expect(B.calls()).toBe(0);
      const requestId = res.headers.get("x-request-id")!;
      for (let i = 0; i < 100; i++) {
        const rows =
          await db`SELECT state FROM public.billing_holds WHERE request_id = ${requestId}`;
        if (rows[0]?.state === "SETTLED") break;
        await Bun.sleep(10);
      }
      expect(runtime.lifecycle.size).toBe(0);
      expect(await ctx.charges()).toMatchObject({ n: 1 });
      expect(await ctx.holdsByState()).toEqual({ SETTLED: 1 });
      expect((await ctx.audit()).ok).toBe(true);
    } finally {
      await runtime.close();
      A.server.stop(true);
      B.server.stop(true);
    }
  });

  test("LEAST_LATENCY: after at most two samples the slow channel is deprioritised and all remaining traffic goes to the fast peer", async () => {
    const slow = fakeUpstream("slow", { headerDelayMs: 400 });
    const fast = fakeUpstream("fast");
    const runtime = createGatewayRuntime({ config: makeConfig({ attemptTimeoutMs: 3000 }), db });
    try {
      await runtime.start();
      const ctx = await setup(runtime, [slow, fast], {
        strategy: "LEAST_LATENCY",
        priority: [0, 0],
      });
      const picked = { slow: 0, fast: 0 };
      for (let i = 0; i < 12; i++) {
        const r = await ctx.chat();
        expect(r.status).toBe(200);
        picked[r.headers.get("x-gateway-channel")?.startsWith("fast-") ? "fast" : "slow"]++;
        await r.text();
      }
      await ctx.settle();
      expect(picked.slow).toBeLessThanOrEqual(2);
      expect(picked.fast).toBeGreaterThanOrEqual(10);
      const h = await ctx.health();
      const fastRow = h[String(ctx.channels[1]!.id)]!;
      const slowRow = h[String(ctx.channels[0]!.id)];
      expect(fastRow.state).toBe("CLOSED");
      expect(fastRow.ewma_latency_ms).toBeLessThan(slowRow?.ewma_latency_ms ?? Infinity);
      expect(await ctx.charges()).toMatchObject({ n: 12, u: 12 });
      expect((await ctx.audit()).ok).toBe(true);
    } finally {
      await runtime.close();
      slow.server.stop(true);
      fast.server.stop(true);
    }
  });

  test("429 cooldown: a rate-limited channel is skipped for the retry-after window and receives no further calls until the cooldown passes", async () => {
    const A = fakeUpstream("A", { status: 429 });
    const B = fakeUpstream("B");
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const ctx = await setup(runtime, [A, B], { priority: [10, 0] });
      const first = await ctx.chat();
      expect(first.status).toBe(200);
      expect(first.headers.get("x-gateway-channel")?.startsWith("B-")).toBe(true);
      await first.text();
      for (let i = 0; i < 10; i++) {
        const r = await ctx.chat();
        expect(r.status).toBe(200);
        expect(r.headers.get("x-gateway-channel")?.startsWith("B-")).toBe(true);
        await r.text();
      }
      await ctx.settle();
      expect(A.calls()).toBe(1);
      expect(B.calls()).toBe(11);
      const h = await ctx.health();
      const aRow = h[String(ctx.channels[0]!.id)]!;
      expect(aRow.cooldown_until_ms!).toBeGreaterThan(performance.now());
      expect(await ctx.charges()).toMatchObject({ n: 11, u: 11 });
      expect((await ctx.audit()).ok).toBe(true);
    } finally {
      await runtime.close();
      A.server.stop(true);
      B.server.stop(true);
    }
  });

  test("symptom: unary client abort during retryable primary failure does not fail over to backup channel", async () => {
    const primaryArrived = Promise.withResolvers<void>();
    const releasePrimary = Promise.withResolvers<void>();
    const A = fakeUpstream("abort-primary", {
      async respond() {
        primaryArrived.resolve();
        await releasePrimary.promise;
        return new Response(JSON.stringify({ error: { message: "Overloaded" } }), {
          status: 503,
          headers: { "content-type": "application/json" },
        });
      },
    });
    const B = fakeUpstream("abort-backup");
    const runtime = createGatewayRuntime({
      config: makeConfig({ unaryClientAbortGraceMs: 5000, attemptTimeoutMs: 5000 }),
      db,
    });
    try {
      await runtime.start();
      const ctx = await setup(runtime, [A, B], { priority: [10, 0], maxAttempts: 2 });
      const ac = new AbortController();
      const pending = ctx.chat(false, ac.signal);
      await primaryArrived.promise;
      ac.abort();
      await Bun.sleep(20);
      releasePrimary.resolve();
      const res = await pending;
      expect(res.status).toBe(499);
      expect(A.calls()).toBe(1);
      expect(B.calls()).toBe(0);
      expect((await ctx.audit()).ok).toBe(true);
    } finally {
      await runtime.close();
      A.server.stop(true);
      B.server.stop(true);
    }
  });

  test("symptom: unary failover from short-timeout primary to long-timeout backup extends hold expires_at", async () => {
    let observedExpiryMs = 0;
    const A = fakeUpstream("short-primary", { status: 503 });
    const B = fakeUpstream("long-backup", {
      async respond() {
        const rows = await db<{ exp_ms: string }[]>`
          SELECT (extract(epoch FROM expires_at) * 1000)::bigint::text AS exp_ms
          FROM public.billing_holds
          WHERE state = 'HELD'
          ORDER BY created_at DESC
          LIMIT 1
        `;
        observedExpiryMs = Number(rows[0]?.exp_ms ?? "0");
        return Response.json({
          id: "backup-ok",
          model: "m",
          choices: [
            { index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" },
          ],
          usage: { prompt_tokens: 5, completion_tokens: 3 },
        });
      },
    });
    const runtime = createGatewayRuntime({
      config: makeConfig({
        holdTtlMs: 20_000,
        attemptTimeoutMs: 1_500,
        unaryClientAbortGraceMs: 60_000,
      }),
      db,
    });
    try {
      await runtime.start();
      const ctx = await setup(runtime, [A, B], { priority: [10, 0], maxAttempts: 2 });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        id: ctx.routes[0]!.id,
        model_name: ctx.model,
        channel_id: ctx.channels[0]!.id,
        upstream_model: "short-primary",
        operations: ["chat"],
        billing_price_id: ctx.customer.id,
        cost_version_id: ctx.supplierPrices[0]!.id,
        priority: 10,
        retry_policy: { max_attempts: 2, attempt_timeout_ms: 1_500 },
      });
      await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        id: ctx.routes[1]!.id,
        model_name: ctx.model,
        channel_id: ctx.channels[1]!.id,
        upstream_model: "long-backup",
        operations: ["chat"],
        billing_price_id: ctx.customer.id,
        cost_version_id: ctx.supplierPrices[0]!.id,
        priority: 0,
        retry_policy: { max_attempts: 2, attempt_timeout_ms: 180_000 },
      });
      const startedMs = Date.now();
      const res = await ctx.chat();
      expect(res.status).toBe(200);
      await res.json();
      expect(A.calls()).toBe(1);
      expect(B.calls()).toBe(1);
      expect(observedExpiryMs - startedMs).toBeGreaterThanOrEqual(200_000);
      expect((await ctx.audit()).ok).toBe(true);
    } finally {
      await runtime.close();
      A.server.stop(true);
      B.server.stop(true);
    }
  });

  test("cross-protocol and failover routes quote tightened per-candidate input reservations", async () => {
    const primary = fakeUpstream("quote-primary", { status: 503 });
    const backup = fakeUpstream("quote-backup", {
      async respond() {
        return Response.json({
          id: "msg_backup",
          type: "message",
          role: "assistant",
          model: "claude-sonnet",
          content: [{ type: "text", text: "ok" }],
          stop_reason: "end_turn",
          usage: { input_tokens: 12, output_tokens: 4 },
        });
      },
    });
    const runtime = createGatewayRuntime({ config: makeConfig(), db });
    try {
      await runtime.start();
      const ctx = await setup(runtime, [primary, backup], {
        priority: [10, 0],
        maxAttempts: 2,
        prices: {
          customer: {
            rates: {
              input_tokens: { numerator: "1", denominator: "1" },
              output_tokens: { numerator: "1", denominator: "1" },
              cache_write_tokens: { numerator: "2", denominator: "1" },
            },
            ceilings: {
              input_tokens: "100000",
              output_tokens: "100",
              cache_write_tokens: "100000",
            },
          },
          supplier: {
            rates: {
              input_tokens: { numerator: "1", denominator: "1" },
              output_tokens: { numerator: "1", denominator: "1" },
              cache_write_tokens: { numerator: "2", denominator: "1" },
            },
            ceilings: {
              input_tokens: "100000",
              output_tokens: "100",
              cache_write_tokens: "100000",
            },
          },
        },
      });
      const anthropicChannel = await internalCall(
        runtime.internalApp,
        "PUT",
        "/internal/v1/channels",
        {
          account_id: ctx.channels[1]!.account_id,
          kind: "anthropic",
          name: `anthropic-backup-${crypto.randomUUID().slice(0, 6)}`,
          base_url: `${backup.url}/anthropic`,
          api_key: "k",
          rpm: 10_000_000,
          concurrency: 10_000,
        },
      );
      expect(anthropicChannel.status).toBe(200);
      const updatedRoute = await internalCall(runtime.internalApp, "PUT", "/internal/v1/routes", {
        id: ctx.routes[1]!.id,
        model_name: ctx.model,
        channel_id: anthropicChannel.data.id,
        upstream_model: "claude-sonnet",
        operations: ["chat"],
        billing_price_id: ctx.customer.id,
        cost_version_id: ctx.supplierPrices[0]!.id,
        priority: 0,
        retry_policy: { max_attempts: 2 },
      });
      expect(updatedRoute.status).toBe(200);
      const res = await ctx.chat();
      expect(res.status).toBe(200);
      const requestId = res.headers.get("x-request-id")!;
      await res.json();
      const [hold] = await db<
        {
          held_atoms: string;
          supplier_holds: { held_atoms: string; channel_id: string }[];
        }[]
      >`
        SELECT held_atoms::text, supplier_holds
        FROM public.billing_holds
        WHERE request_id = ${requestId}::uuid
      `;
      expect(BigInt(hold!.supplier_holds[0]!.held_atoms)).toBe(300_020n);
      expect(BigInt(hold!.supplier_holds[1]!.held_atoms)).toBeLessThan(10_000n);
      expect((await ctx.audit()).ok).toBe(true);
    } finally {
      await runtime.close();
      primary.server.stop(true);
      backup.server.stop(true);
    }
  });
}
