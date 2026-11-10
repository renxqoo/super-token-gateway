import { expect, test } from "bun:test";
import { createTransport } from "../network.ts";

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

test("TLS keeps the original SNI and rejects an untrusted upstream certificate", async () => {
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
  const server = createServer({ key, cert }, (_request, response) => response.end("unsafe"));
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("TLS fixture did not bind");
  const transport = createTransport(8);
  try {
    await expect(
      transport.fetch(new URL(`https://localhost:${address.port}/`), {}, true),
    ).rejects.toMatchObject({ code: "DEPTH_ZERO_SELF_SIGNED_CERT" });
    const wireAddress = wire.address();
    if (!wireAddress || typeof wireAddress === "string")
      throw new Error("TLS wire fixture did not bind");
    await expect(
      transport.fetch(new URL(`https://localhost:${wireAddress.port}/`), {}, true),
    ).rejects.toThrow();
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
