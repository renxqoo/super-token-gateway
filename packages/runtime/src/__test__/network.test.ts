import { expect, test } from "bun:test";
import { createTransport, isPublicAddress, validateBaseUrl } from "../network.ts";

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
