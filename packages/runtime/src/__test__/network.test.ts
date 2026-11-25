import { expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { gzipSync } from "node:zlib";
import { validateAuthCredentials } from "../cloud-auth.ts";
import { createTransport, isPublicAddress, safeFetch, validateBaseUrl } from "../network.ts";

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

test("TLS preserves SNI and classifies rejected handshakes as undispatched", async () => {
  const { createServer } = await import("node:https");
  const { createServer: createTcpServer } = await import("node:net");
  const key = await Bun.file(new URL("./fixtures/upstream-key.fixture", import.meta.url)).text();
  const cert = await Bun.file(new URL("./fixtures/upstream-cert.fixture", import.meta.url)).text();
  let clientHello: Uint8Array = Buffer.alloc(0);
  const wire = createTcpServer((socket) =>
    socket.once("data", (bytes) => {
      clientHello = typeof bytes === "string" ? Buffer.from(bytes) : bytes;
      socket.destroy();
    }),
  );
  await new Promise<void>((resolve) => wire.listen(0, "127.0.0.1", resolve));
  let accepted = 0;
  const server = createServer({ key, cert }, (_request, response) => {
    accepted++;
    response.end("unsafe");
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("TLS fixture did not bind");
  const transport = createTransport(8);
  try {
    await expect(
      transport.fetch(
        new URL(`https://localhost:${address.port}/`),
        { method: "POST", body: "fixture" },
        true,
      ),
    ).rejects.toMatchObject({
      code: "upstream_tls",
      cause: { code: "DEPTH_ZERO_SELF_SIGNED_CERT" },
    });
    expect(accepted).toBe(0);
    const wireAddress = wire.address();
    if (!wireAddress || typeof wireAddress === "string")
      throw new Error("TLS wire fixture did not bind");
    await expect(
      transport.fetch(new URL(`https://localhost:${wireAddress.port}/`), {}, true),
    ).rejects.toMatchObject({ code: "upstream_tls" });
    expect(Buffer.from(clientHello).includes(Buffer.from("localhost"))).toBe(true);
  } finally {
    transport.close();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await new Promise<void>((resolve) => wire.close(() => resolve()));
  }
});

test("an IP-literal HTTPS upstream handshakes without SNI instead of failing before connecting", async () => {
  const { createServer: createTcpServer } = await import("node:net");
  let clientHello: Uint8Array = Buffer.alloc(0);
  const wire = createTcpServer((socket) =>
    socket.once("data", (bytes) => {
      clientHello = typeof bytes === "string" ? Buffer.from(bytes) : bytes;
      socket.destroy();
    }),
  );
  await new Promise<void>((resolve) => wire.listen(0, "127.0.0.1", resolve));
  const address = wire.address();
  if (!address || typeof address === "string") throw new Error("TLS wire fixture did not bind");
  const transport = createTransport(8);
  try {
    await expect(
      transport.fetch(new URL(`https://127.0.0.1:${address.port}/`), {}, true),
    ).rejects.toMatchObject({ code: "upstream_tls" });
    expect(clientHello.length).toBeGreaterThan(0);
    expect(Buffer.from(clientHello).includes(Buffer.from("127.0.0.1"))).toBe(false);
  } finally {
    transport.close();
    await new Promise<void>((resolve) => wire.close(() => resolve()));
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

test("bun HTTP/1.1 transport rejects Unicode-to-Latin1 CRLF injection without leaking connection slots", async () => {
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch() {
      return new Response("ok", { headers: { "content-length": "2" } });
    },
  });
  const transport = createTransport(1);
  try {
    const url = new URL(`http://127.0.0.1:${server.port}/`);
    await expect(
      transport.fetch(
        url,
        { headers: { "x-inject": "foo\u010d\u010aTransfer-Encoding: chunked" } },
        true,
      ),
    ).rejects.toThrow(TypeError);
    await expect(transport.fetch(url, { headers: { "x-bad": "a\r\nb" } }, true)).rejects.toThrow(
      TypeError,
    );
    await expect(transport.fetch(url, { headers: { "bad name": "value" } }, true)).rejects.toThrow(
      TypeError,
    );
    const okResp = await transport.fetch(url, { headers: { "content-length": "50" } }, true);
    expect(okResp.status).toBe(200);
    expect(await okResp.text()).toBe("ok");
    const nextResp = await transport.fetch(url, {}, true);
    expect(nextResp.status).toBe(200);
    expect(await nextResp.text()).toBe("ok");
  } finally {
    transport.close();
    await server.stop(true);
  }
});

test("bun HTTP/1.1 transport completes Content-Length 0 for identity encoding, SSE, and gzip without hanging", async () => {
  const modes = [
    "HTTP/1.1 200 OK\r\nContent-Encoding: identity\r\nContent-Length: 0\r\n\r\n",
    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 0\r\n\r\n",
    "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 0\r\n\r\n",
  ];
  let idx = 0;
  const server = createServer((socket) => {
    socket.on("data", () => {
      const payload = modes[idx++] ?? modes[0]!;
      socket.write(payload);
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;
  const transport = createTransport(1);
  try {
    const url = new URL(`http://127.0.0.1:${port}/`);
    for (let i = 0; i < 3; i++) {
      const res = await transport.fetch(url, { signal: AbortSignal.timeout(1500) }, true);
      expect(res.status).toBe(200);
      expect(await res.text()).toBe("");
    }
  } finally {
    transport.close();
    server.close();
  }
});

test("bun HTTP/1.1 transport rejects conflicting framing headers and invalid response headers without hanging", async () => {
  const badResponses = [
    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 5\r\n\r\n0\r\n\r\n",
    "HTTP/1.1 200 OK\r\nBad Header : value\r\nContent-Length: 0\r\n\r\n",
    "HTTP/1.1 200 OK\r\nContent-Length: 5, 6\r\n\r\nhello",
    "HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\n\r\n",
    "HTTP/1.1 200 OK\r\nContent-Length: \n5\r\n\r\nhello",
    "HTTP/1.1 200 OK\r\nX-Bare: foo\r\r\nContent-Length: 0\r\n\r\n",
    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n\n1\r\na\r\n0\r\n\r\n",
  ];
  let idx = 0;
  const server = createServer((socket) => {
    socket.once("data", () => {
      const raw = badResponses[idx++];
      if (raw) socket.end(raw);
      else socket.end("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;
  const transport = createTransport(1);
  try {
    const url = new URL(`http://127.0.0.1:${port}/`);
    for (let i = 0; i < badResponses.length; i++) {
      await expect(
        transport.fetch(url, { signal: AbortSignal.timeout(1500) }, true).then((res) => res.text()),
      ).rejects.toThrow();
    }
    const okResp = await transport.fetch(url, { signal: AbortSignal.timeout(1500) }, true);
    expect(await okResp.text()).toBe("ok");
  } finally {
    transport.close();
    server.close();
  }
});

test("bun HTTP/1.1 transport tears down socket and releases slot when cancelling compressed stream", async () => {
  let reqNum = 0;
  const part = gzipSync(Buffer.from("hello compressed stream"));
  const server = createServer((socket) => {
    socket.once("data", () => {
      reqNum++;
      if (reqNum === 1) {
        socket.write(
          `HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: ${part.byteLength + 100}\r\n\r\n`,
        );
        socket.write(part.subarray(0, 10));
      } else {
        socket.end("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
      }
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;
  const transport = createTransport(1);
  try {
    const url = new URL(`http://127.0.0.1:${port}/`);
    const res1 = await transport.fetch(url, { signal: AbortSignal.timeout(1500) }, true);
    await res1.body?.cancel();
    const res2 = await transport.fetch(url, { signal: AbortSignal.timeout(1500) }, true);
    expect(await res2.text()).toBe("ok");
  } finally {
    transport.close();
    server.close();
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

test("symptom: native TLS handshake rejects CA-trusted mismatched DNS SAN and IP-literal against DNS-only cert", async () => {
  const dir = mkdtempSync(join(tmpdir(), "stg-tls-id-"));
  try {
    spawnSync(
      "openssl",
      [
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        join(dir, "ca.key"),
        "-out",
        join(dir, "ca.crt"),
        "-days",
        "1",
        "-subj",
        "/CN=StgTestCA",
      ],
      { stdio: "pipe" },
    );
    spawnSync(
      "openssl",
      [
        "req",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        join(dir, "srv.key"),
        "-out",
        join(dir, "srv.csr"),
        "-subj",
        "/CN=wrong.example",
      ],
      { stdio: "pipe" },
    );
    writeFileSync(join(dir, "ext.cnf"), "subjectAltName=DNS:wrong.example\n");
    spawnSync(
      "openssl",
      [
        "x509",
        "-req",
        "-in",
        join(dir, "srv.csr"),
        "-CA",
        join(dir, "ca.crt"),
        "-CAkey",
        join(dir, "ca.key"),
        "-CAcreateserial",
        "-out",
        join(dir, "srv.crt"),
        "-days",
        "1",
        "-extfile",
        join(dir, "ext.cnf"),
      ],
      { stdio: "pipe" },
    );
    const cert = readFileSync(join(dir, "srv.crt"), "utf8");
    const key = readFileSync(join(dir, "srv.key"), "utf8");
    const srv = Bun.serve({
      hostname: "127.0.0.1",
      port: 0,
      tls: { cert, key },
      fetch: () => Response.json({ ok: true }),
    });
    const probeScript = join(dir, "probe.ts");
    const networkMod = new URL("../network.ts", import.meta.url).pathname;
    writeFileSync(
      probeScript,
      `
import { createTransport } from ${JSON.stringify(networkMod)};
const port = Number(process.env.TEST_PORT);
const t = createTransport(4, { dnsLookup: async () => [{ address: "127.0.0.1", family: 4 }] });
const out: Record<string, string> = {};
try {
  await t.fetchFast(new URL("https://right.example:" + port + "/"), {}, true);
  out.dnsMismatch = "ACCEPTED";
} catch (e) {
  out.dnsMismatch = (e as { code?: string }).code ?? String(e);
}
try {
  await t.fetchFast(new URL("https://127.0.0.1:" + port + "/"), {}, true);
  out.ipMismatch = "ACCEPTED";
} catch (e) {
  out.ipMismatch = (e as { code?: string }).code ?? String(e);
}
try {
  const ok = await t.fetchFast(new URL("https://wrong.example:" + port + "/"), {}, true);
  out.validMatch = ok.fast ? String(ok.status) : String(ok.response.status);
} catch (e) {
  out.validMatch = (e as { code?: string }).code ?? String(e);
}
t.close();
console.log(JSON.stringify(out));
`,
    );
    try {
      const child = Bun.spawn([process.execPath, probeScript], {
        env: {
          PATH: process.env.PATH ?? "/usr/bin:/bin",
          NODE_EXTRA_CA_CERTS: join(dir, "ca.crt"),
          TEST_PORT: String(srv.port),
        },
        stdout: "pipe",
        stderr: "pipe",
      });
      const [code, stdout, stderr] = await Promise.all([
        child.exited,
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      expect({ code, stderr }).toEqual({ code: 0, stderr: "" });
      expect(JSON.parse(stdout.trim())).toEqual({
        dnsMismatch: "upstream_tls",
        ipMismatch: "upstream_tls",
        validMatch: "200",
      });
    } finally {
      await srv.stop(true);
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("symptom: a stalled TLS address prevents dispatch to a healthy DNS answer", async () => {
  const dir = mkdtempSync(join(tmpdir(), "stg-tls-addresses-"));
  const certPath = join(dir, "cert.pem");
  const keyPath = join(dir, "key.pem");
  const generated = spawnSync("openssl", [
    "req",
    "-x509",
    "-newkey",
    "rsa:2048",
    "-nodes",
    "-days",
    "1",
    "-subj",
    "/CN=fallback.example",
    "-addext",
    "subjectAltName=DNS:fallback.example",
    "-keyout",
    keyPath,
    "-out",
    certPath,
  ]);
  expect(generated.status).toBe(0);
  const sockets = new Set<import("node:net").Socket>();
  let hellos = 0;
  const stalled = createServer((socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
    socket.once("data", () => {
      hellos++;
    });
  });
  await new Promise<void>((resolve) => stalled.listen(0, "127.0.0.1", resolve));
  const address = stalled.address();
  if (!address || typeof address === "string") throw new Error("TLS fixture did not bind");
  const requests: { host: string | null; payload: string }[] = [];
  const healthy = Bun.serve({
    hostname: "::1",
    port: address.port,
    tls: { key: readFileSync(keyPath), cert: readFileSync(certPath) },
    async fetch(request) {
      requests.push({ host: request.headers.get("host"), payload: await request.text() });
      return new Response("ok", { headers: { connection: "close" } });
    },
  });
  try {
    const run = join(dir, "probe.ts");
    const networkMod = new URL("../network.ts", import.meta.url).pathname;
    writeFileSync(
      run,
      `
import { createTransport } from ${JSON.stringify(networkMod)};
const t = createTransport(2, { dial: { concurrency: 1, maxQueueWaitMs: 150 }, dnsLookup: async () => [
  { address: "127.0.0.1", family: 4 }, { address: "::1", family: 6 },
] });
const url = new URL("https://fallback.example:${address.port}/");
try {
  const prepared = await t.prepareDispatch(url, 8, {}, true);
  const a = await prepared.fetchFast({method: "POST", body: "prepared", dispatchDeadline: performance.now() + 800});
  console.log(a.fast ? a.status : a.response.status);
  const b = await t.fetch(url, {method: "POST", body: new Blob(["streamed"]).stream(), dispatchDeadline: performance.now() + 800}, true);
  console.log(await b.text());
  const c = await t.fetchFast(url, {method: "POST", body: "buffered", dispatchDeadline: performance.now() + 800}, true);
  console.log(c.fast ? c.status : c.response.status);
  const reserved = await t.prepareDispatch(url, 0, {}, true);
  try {
    const fallback = await t.prepareDispatch(url, 5, {dispatchDeadline: performance.now() + 800}, true);
    const d = await fallback.fetchFast({method: "POST", body: "queued", dispatchDeadline: performance.now() + 800});
    console.log(d.fast ? d.status : d.response.status);
  } finally { reserved.release(); }
} finally { t.close(); }
`,
    );
    const child = Bun.spawn([process.execPath, run], {
      env: { ...process.env, NODE_EXTRA_CA_CERTS: certPath },
      stdout: "pipe",
      stderr: "pipe",
    });
    const [code, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    expect({ code, stdout, stderr }).toEqual({
      code: 0,
      stdout: "200\nok\n200\n200\n",
      stderr: "",
    });
    expect(hellos).toBe(3);
    expect(requests).toEqual([
      { host: `fallback.example:${address.port}`, payload: "prepared" },
      { host: `fallback.example:${address.port}`, payload: "streamed" },
      { host: `fallback.example:${address.port}`, payload: "buffered" },
      { host: `fallback.example:${address.port}`, payload: "queued" },
    ]);
  } finally {
    await healthy.stop(true);
    for (const socket of sockets) socket.destroy();
    await new Promise<void>((resolve) => stalled.close(() => resolve()));
    rmSync(dir, { recursive: true, force: true });
  }
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
