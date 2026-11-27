import { expect, test } from "bun:test";
import { createServer } from "node:net";
import { validateAuthCredentials } from "../cloud-auth.ts";
import { isPublicAddress, validateBaseUrl } from "../ip.ts";
import { createTransport, safeFetch } from "../network.ts";

test("symptom: fast handshakes can start a new dial burst before the rate budget refills", async () => {
  const arrivals: number[] = [];
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch() {
      arrivals.push(performance.now());
      return new Response("ok", { headers: { connection: "close" } });
    },
  });
  const transport = createTransport(8, {
    dial: { concurrency: 1, startsPerSecond: 10, maxQueueWaitMs: 1000 },
  });
  try {
    const replies = await Promise.all(
      Array.from({ length: 3 }, () => transport.fetch(server.url, {}, true)),
    );
    expect(await Promise.all(replies.map((response) => response.text()))).toEqual([
      "ok",
      "ok",
      "ok",
    ]);
    expect(arrivals[2]! - arrivals[0]!).toBeGreaterThanOrEqual(160);
  } finally {
    transport.close();
    server.stop(true);
  }
});

test("symptom: streamed request bodies can bypass the upstream dial rate limit", async () => {
  const arrivals: number[] = [];
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      arrivals.push(performance.now());
      return new Response(await request.text(), { headers: { connection: "close" } });
    },
  });
  const transport = createTransport(4, {
    dial: { concurrency: 1, startsPerSecond: 10, maxQueueWaitMs: 1000 },
  });
  try {
    const calls = Array.from({ length: 3 }, () =>
      transport.fetch(
        server.url,
        {
          method: "POST",
          body: new Blob(["upload"]).stream(),
        },
        true,
      ),
    );
    expect(await Promise.all(calls.map(async (call) => (await call).text()))).toEqual([
      "upload",
      "upload",
      "upload",
    ]);
    expect(arrivals[2]! - arrivals[0]!).toBeGreaterThanOrEqual(160);
  } finally {
    transport.close();
    server.stop(true);
  }
});

test("symptom: buffered and streamed upstream requests to the same host split dial concurrency and rate caps", async () => {
  const arrivals: number[] = [];
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      arrivals.push(performance.now());
      return new Response(await request.text(), { headers: { connection: "close" } });
    },
  });
  const transport = createTransport(4, {
    dial: { concurrency: 1, startsPerSecond: 10, maxQueueWaitMs: 1000 },
  });
  try {
    const calls = [
      transport.fetch(server.url, { method: "POST", body: "buffered-1" }, true),
      transport.fetch(
        server.url,
        { method: "POST", body: new Blob(["streamed-1"]).stream() },
        true,
      ),
      transport.fetch(server.url, { method: "POST", body: "buffered-2" }, true),
    ];
    expect(await Promise.all(calls.map(async (call) => (await call).text()))).toEqual([
      "buffered-1",
      "streamed-1",
      "buffered-2",
    ]);
    expect(arrivals[1]! - arrivals[0]!).toBeGreaterThanOrEqual(80);
    expect(arrivals[2]! - arrivals[0]!).toBeGreaterThanOrEqual(160);
  } finally {
    transport.close();
    server.stop(true);
  }
});

test("symptom: an ordinary exchange can block a prepared dispatch while holding its pool slot", async () => {
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: () => new Response("ok", { headers: { connection: "close" } }),
  });
  const transport = createTransport(1, {
    dial: { concurrency: 1, startsPerSecond: 1000, maxQueueWaitMs: 100 },
  });
  try {
    const prepared = await transport.prepareDispatch(server.url, 0, {}, true);
    const ordinary = transport.fetch(server.url, {}, true);
    await Bun.sleep(5);
    const replies = await Promise.all([prepared.fetchFast({}), ordinary]);
    expect(
      replies.map((reply) => ("status" in reply ? reply.status : reply.response.status)),
    ).toEqual([200, 200]);
  } finally {
    transport.close();
    server.stop(true);
  }
});

test("symptom: a queued preflight times out despite a reusable upstream socket returning", async () => {
  const entered = Promise.withResolvers<void>();
  const returnFirst = Promise.withResolvers<void>();
  let arrivals = 0;
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch() {
      if (++arrivals === 1) {
        entered.resolve();
        await returnFirst.promise;
      }
      return new Response("ok");
    },
  });
  const transport = createTransport(4, {
    dial: { concurrency: 1, startsPerSecond: 1, maxQueueWaitMs: 200 },
  });
  try {
    const first = transport.fetch(server.url, {}, true);
    await entered.promise;
    const second = transport.prepareDispatch(server.url, 0, {}, true);
    await Bun.sleep(10);
    returnFirst.resolve();
    expect(await (await first).text()).toBe("ok");
    const ready = await second;
    expect(await ready.fetchFast({})).toMatchObject({ fast: true, status: 200 });
    expect(arrivals).toBe(2);
  } finally {
    returnFirst.resolve();
    transport.close();
    server.stop(true);
  }
});

test("rejects private, mapped, metadata, reserved and ambiguous destinations", () => {
  for (const address of [
    "127.0.0.1",
    "10.0.0.1",
    "169.254.169.254",
    "100.64.0.1",
    "192.168.1.1",
    "::1",
    "::ffff:127.0.0.1",
    "::ffff:7f00:1",
    "fc00::1",
    "fe80::1",
    "2001:db8::1",
    "0.0.0.0",
    "224.0.0.1",
  ])
    expect(isPublicAddress(address)).toBe(false);
  expect(isPublicAddress("1.1.1.1")).toBe(true);
  expect(isPublicAddress("2606:4700:4700::1111")).toBe(true);
  for (const url of [
    "http://example.com",
    "https://user:pass@example.com",
    "https://example.com?secret=x",
    "https://127.0.0.1",
    "https://[::1]",
  ])
    expect(() => validateBaseUrl(url, false)).toThrow();
});

test("mixed public IPv4 and reserved IPv6 answers are rejected before dispatch", async () => {
  const url = new URL("https://provider.example/v1/models");
  const publicAddress = { address: "1.1.1.1", family: 4 };
  const dispatched: string[] = [];
  const send = async (target: URL) => {
    dispatched.push(target.hostname);
    return new Response("fixture");
  };
  await expect(
    safeFetch(url, {}, false, [], send, async () => [
      publicAddress,
      { address: "2001:2::45", family: 6 },
    ]),
  ).rejects.toMatchObject({ code: "unsafe_upstream" });
  expect(dispatched).toEqual([]);

  await expect(
    safeFetch(url, {}, true, [], send, async () => [
      publicAddress,
      { address: "fe80::1%lo0", family: 6 },
    ]),
  ).rejects.toMatchObject({ code: "upstream_dns" });
  expect(dispatched).toEqual([]);

  const response = await safeFetch(url, {}, false, [], send, async () => [publicAddress]);
  expect(await response.text()).toBe("fixture");
  expect(dispatched).toEqual([publicAddress.address]);
});

test("repeated upstream bursts reuse sockets instead of exhausting local ports", async () => {
  const ports = new Set<number>();
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request, server) {
      const peer = server.requestIP(request);
      if (peer) ports.add(peer.port);
      await Bun.sleep(2);
      return Response.json({ completed: true });
    },
  });
  const transport = createTransport(512);
  try {
    for (let round = 0; round < 4; round++)
      await Promise.all(
        Array.from({ length: 128 }, async () => {
          const controller = new AbortController();
          const response = await transport.fetch(
            new URL(`http://127.0.0.1:${server.port}/`),
            { signal: controller.signal },
            true,
          );
          expect(await response.json()).toEqual({ completed: true });
          controller.abort();
        }),
      );
    expect(ports.size).toBeLessThanOrEqual(128);
  } finally {
    transport.close();
    await server.stop(true);
  }
});

test("transport keeps redirects manual and validates addresses before connecting", async () => {
  let calls = 0;
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(request) {
      calls++;
      expect(request.headers.get("host")).toBe(`localhost:${server.port}`);
      return new Response(null, { status: 302, headers: { location: "/secret" } });
    },
  });
  const transport = createTransport(8);
  const url = new URL(`http://localhost:${server.port}/`);
  try {
    await expect(transport.fetch(url, {}, false)).rejects.toMatchObject({
      code: "unsafe_upstream",
    });
    expect(calls).toBe(0);
    const response = await transport.fetch(url, {}, true);
    expect(response.status).toBe(302);
    expect(response.headers.get("location")).toBe("/secret");
    expect(calls).toBe(1);
    await response.body?.cancel();
  } finally {
    transport.close();
    await server.stop(true);
  }
});

test("compressed upstream bodies are decoded before enforcing the response byte limit", async () => {
  const { gzipSync, brotliCompressSync, zstdCompressSync } = await import("node:zlib");
  const { readBounded } = await import("../network.ts");
  const text = "a".repeat(8192);
  const encoders = { gzip: gzipSync, br: brotliCompressSync, zstd: zstdCompressSync };
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(request) {
      const encoding = new URL(request.url).pathname.slice(1) as keyof typeof encoders;
      return new Response(encoders[encoding](text), { headers: { "content-encoding": encoding } });
    },
  });
  const transport = createTransport(8);
  try {
    for (const encoding of Object.keys(encoders)) {
      const response = await transport.fetch(
        new URL(`http://127.0.0.1:${server.port}/${encoding}`),
        {},
        true,
      );
      expect(response.headers.get("content-encoding")).toBeNull();
      expect(await response.text()).toBe(text);
      const oversized = await transport.fetch(
        new URL(`http://127.0.0.1:${server.port}/${encoding}`),
        {},
        true,
      );
      await expect(readBounded(oversized.body, 1024)).rejects.toMatchObject({
        code: "body_too_large",
      });
    }
  } finally {
    transport.close();
    await server.stop(true);
  }
});

test("closing a saturated pool rejects queued requests and abort frees capacity", async () => {
  let entered = 0;
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch() {
      entered++;
      await Bun.sleep(80);
      return new Response("completed");
    },
  });
  const transport = createTransport(1);
  const url = new URL(`http://127.0.0.1:${server.port}/`);
  try {
    const abort = new AbortController();
    const first = transport.fetch(url, { signal: abort.signal }, true);
    void first.catch(() => undefined);
    while (!entered) await Bun.sleep(1);
    abort.abort();
    await expect(first).rejects.toThrow();
    expect(await (await transport.fetch(url, {}, true)).text()).toBe("completed");
    const pending = Array.from({ length: 4 }, () =>
      transport.fetch(url, {}, true).then((response) => response.text()),
    );
    for (const promise of pending) void promise.catch(() => undefined);
    await Bun.sleep(5);
    transport.close();
    for (const promise of pending) await expect(promise).rejects.toThrow();
    await expect(transport.fetch(url, {}, true)).rejects.toMatchObject({
      code: "upstream_transport_closed",
    });
  } finally {
    transport.close();
    await server.stop(true);
  }
});

test("a queued upstream abort returns before the active socket frees and proves no dispatch", async () => {
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const seen: string[] = [];
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      const path = new URL(request.url).pathname;
      seen.push(path);
      if (path === "/first") {
        entered.resolve();
        await release.promise;
      }
      return new Response("completed");
    },
  });
  const transport = createTransport(1);
  const url = (path: string) => new URL(`http://127.0.0.1:${server.port}${path}`);
  const first = transport.fetch(url("/first"), {}, true).then((response) => response.text());
  const abort = new AbortController();
  let watchdog: ReturnType<typeof setTimeout> | undefined;
  try {
    await entered.promise;
    const queued = transport.fetch(url("/queued"), { method: "POST", signal: abort.signal }, true);
    await Bun.sleep(5);
    abort.abort(new Error("Upstream response header deadline"));
    const result = await Promise.race([
      queued.catch((error) => error),
      new Promise<string>((resolve) => {
        watchdog = setTimeout(() => resolve("still waiting for the active socket"), 250);
      }),
    ]);
    expect(result).toMatchObject({ code: "upstream_not_dispatched" });
    release.resolve();
    await expect(first).resolves.toBe("completed");
    expect(await (await transport.fetch(url("/after"), {}, true)).text()).toBe("completed");
    expect(seen).toEqual(["/first", "/after"]);
  } finally {
    clearTimeout(watchdog);
    release.resolve();
    await first.catch(() => undefined);
    transport.close();
    await server.stop(true);
  }
});

test("an expired send grant never writes the queued upstream request", async () => {
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const received: string[] = [];
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      received.push(new URL(request.url).pathname);
      entered.resolve();
      await release.promise;
      return new Response("ok");
    },
  });
  const transport = createTransport(1);
  const base = new URL(`http://127.0.0.1:${server.port}`);
  const occupied = transport.fetch(base, {}, true).then((response) => response.text());
  try {
    await entered.promise;
    const delayed = transport
      .fetch(
        new URL("/late", base),
        {
          method: "POST",
          body: "authorized only before deadline",
          dispatchDeadline: performance.now() + 20,
        },
        true,
      )
      .catch((error: unknown) => error);
    await Bun.sleep(40);
    release.resolve();
    expect(await occupied).toBe("ok");
    expect(await delayed).toMatchObject({ code: "upstream_not_dispatched" });
    expect(received).toEqual(["/"]);
  } finally {
    release.resolve();
    await occupied.catch(() => undefined);
    transport.close();
    await server.stop(true);
  }
});

test("aborting an upstream body stops the socket and allows the next request", async () => {
  let cancelled = false;
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(request) {
      if (new URL(request.url).pathname === "/ok") return new Response("ready");
      return new Response(
        new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode("first"));
          },
          cancel() {
            cancelled = true;
          },
        }),
      );
    },
  });
  const transport = createTransport(2);
  try {
    const controller = new AbortController();
    const response = await transport.fetch(
      new URL(`http://127.0.0.1:${server.port}/slow`),
      { signal: controller.signal },
      true,
    );
    const reader = response.body!.getReader();
    expect(new TextDecoder().decode((await reader.read()).value)).toBe("first");
    controller.abort();
    await expect(reader.read()).rejects.toThrow();
    reader.releaseLock();
    const deadline = Date.now() + 1000;
    while (!cancelled && Date.now() < deadline) await Bun.sleep(2);
    expect(cancelled).toBe(true);
    expect(
      await (await transport.fetch(new URL(`http://127.0.0.1:${server.port}/ok`), {}, true)).text(),
    ).toBe("ready");
  } finally {
    transport.close();
    await server.stop(true);
  }
});

test("configured environment proxies cannot intercept pinned upstream requests", async () => {
  let intercepted = 0;
  const proxy = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch() {
      intercepted++;
      return new Response("intercepted");
    },
  });
  const target = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch() {
      return new Response("direct");
    },
  });
  const saved = {
    HTTP_PROXY: process.env.HTTP_PROXY,
    http_proxy: process.env.http_proxy,
    NO_PROXY: process.env.NO_PROXY,
    no_proxy: process.env.no_proxy,
  };
  const transport = createTransport(8);
  try {
    process.env.HTTP_PROXY = process.env.http_proxy = `http://127.0.0.1:${proxy.port}`;
    process.env.NO_PROXY = process.env.no_proxy = "";
    expect(
      await (await transport.fetch(new URL(`http://127.0.0.1:${target.port}/`), {}, true)).text(),
    ).toBe("direct");
    expect(intercepted).toBe(0);
  } finally {
    for (const [name, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
    transport.close();
    await proxy.stop(true);
    await target.stop(true);
  }
});

test("F7: transport DNS cache coalesces lookups within TTL while still enforcing per-call SSRF rules", async () => {
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch() {
      return Response.json({ ok: true });
    },
  });
  let lookups = 0;
  const transport = createTransport(16, {
    dnsCacheTtlMs: 80,
    dnsLookup: async () => {
      lookups++;
      await Bun.sleep(10);
      return [{ address: "127.0.0.1", family: 4 }];
    },
  });
  const url = new URL(`http://cached-upstream.example:${server.port}/`);
  try {
    const burst = await Promise.all(
      Array.from({ length: 8 }, () => transport.fetchFast(url, {}, true)),
    );
    expect(lookups).toBe(1);
    for (const item of burst) {
      expect(item.fast).toBe(true);
      if (item.fast) {
        expect(item.status).toBe(200);
        expect(new TextDecoder().decode(item.body)).toBe('{"ok":true}');
      }
    }
    await expect(transport.fetch(url, {}, false)).rejects.toMatchObject({
      code: "unsafe_upstream",
    });
    expect(lookups).toBe(1);
    await Bun.sleep(100);
    const afterExpiry = await transport.fetchFast(url, {}, true);
    expect(afterExpiry.fast).toBe(true);
    expect(lookups).toBe(2);
  } finally {
    transport.close();
    await server.stop(true);
  }
});

test("symptom: slow DNS lookup honours onCancelRegister and dispatchDeadline without AbortSignal", async () => {
  let resolveSlow!: (addrs: { address: string; family: number }[]) => void;
  const transport = createTransport(4, {
    dnsLookup: () =>
      new Promise((resolve) => {
        resolveSlow = resolve;
      }),
  });
  const url = new URL("http://slow-dns.example/");
  try {
    const cancelHolder: { fn: ((reason: unknown) => void) | null } = { fn: null };
    const p1 = transport.fetchFast(
      url,
      {
        onCancelRegister: (cb) => {
          cancelHolder.fn = cb;
          return () => {
            if (cancelHolder.fn === cb) cancelHolder.fn = null;
          };
        },
      },
      true,
    );
    await Bun.sleep(10);
    cancelHolder.fn?.(new Error("client aborted"));
    await expect(p1).rejects.toMatchObject({ code: "upstream_not_dispatched" });

    const p2 = transport.fetchFast(url, { dispatchDeadline: performance.now() + 25 }, true);
    await expect(p2).rejects.toMatchObject({ code: "upstream_not_dispatched" });
    resolveSlow([{ address: "127.0.0.1", family: 4 }]);
  } finally {
    transport.close();
  }
});

test("symptom: validateAuthCredentials rejects CRLF and Unicode-to-Latin1 bearer and header secrets", () => {
  expect(() => validateAuthCredentials({ scheme: "bearer" }, "sk-ok-123")).not.toThrow();
  expect(() =>
    validateAuthCredentials({ scheme: "bearer" }, "sk-bad\r\nX-Injected: yes"),
  ).toThrow();
  expect(() =>
    validateAuthCredentials({ scheme: "bearer" }, "sk-bad\u010d\u010aX-Injected: yes"),
  ).toThrow();
  expect(() =>
    validateAuthCredentials(
      { scheme: "header", header_name: "x-api-key" },
      "key\r\nX-Injected: yes",
    ),
  ).toThrow();
});

test("a write followed by an upstream disconnect does not replay the request to another address", async () => {
  let received = Buffer.alloc(0);
  const primary = createServer((socket) => {
    socket.on("data", (bytes) => {
      received = Buffer.concat([received, typeof bytes === "string" ? Buffer.from(bytes) : bytes]);
      if (received.includes("one authorized request")) socket.destroy();
    });
  });
  await new Promise<void>((resolve) => primary.listen(0, "127.0.0.1", resolve));
  const address = primary.address();
  if (!address || typeof address === "string") throw new Error("HTTP fixture did not bind");
  let backupCalls = 0;
  const backup = Bun.serve({
    hostname: "::1",
    port: address.port,
    fetch() {
      backupCalls++;
      return new Response("duplicate");
    },
  });
  const transport = createTransport(1, {
    dnsLookup: async () => [
      { address: "127.0.0.1", family: 4 },
      { address: "::1", family: 6 },
    ],
  });
  try {
    await expect(
      transport.fetchFast(
        new URL(`http://fallback.example:${address.port}/`),
        {
          method: "POST",
          body: "one authorized request",
          signal: AbortSignal.timeout(1000),
        },
        true,
      ),
    ).rejects.toThrow(/Upstream (closed|connection closed)/);
    expect(received.toString()).toEndWith("one authorized request");
    expect(backupCalls).toBe(0);
  } finally {
    transport.close();
    await backup.stop(true);
    await new Promise<void>((resolve) => primary.close(() => resolve()));
  }
});
