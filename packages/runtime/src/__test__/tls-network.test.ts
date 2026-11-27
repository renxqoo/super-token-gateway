import { expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createConnection, createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createTransport } from "../network.ts";

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

test("symptom: transient single-address TLS handshake reset fails without a safe pre-dispatch retry", async () => {
  const dir = mkdtempSync(join(tmpdir(), "stg-tls-retry-"));
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
    "/CN=retry.example",
    "-addext",
    "subjectAltName=DNS:retry.example",
    "-keyout",
    keyPath,
    "-out",
    certPath,
  ]);
  expect(generated.status).toBe(0);

  const requests: string[] = [];
  let dropAfterRead = false;
  const openSockets = new Set<import("node:net").Socket>();
  const backend = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    tls: { key: readFileSync(keyPath), cert: readFileSync(certPath) },
    async fetch(request) {
      requests.push(await request.text());
      if (dropAfterRead) {
        for (const socket of openSockets) socket.destroy();
        await Bun.sleep(20);
      }
      return new Response("ok", { headers: { connection: "close" } });
    },
  });

  let mode: "reset_first" | "pass" | "plaintext" | "always_reset" | "post_dispatch_drop" =
    "reset_first";
  let modeConnections = 0;
  const proxy = createServer((client) => {
    openSockets.add(client);
    client.once("close", () => openSockets.delete(client));
    modeConnections++;
    if (mode === "always_reset" || (mode === "reset_first" && modeConnections === 1)) {
      client.once("data", () => client.destroy());
      return;
    }
    if (mode === "plaintext") {
      client.once("data", () => client.end("HTTP/1.1 502 Bad Gateway\r\n\r\n"));
      return;
    }
    const upstream = createConnection({ host: "127.0.0.1", port: backend.port! });
    openSockets.add(upstream);
    upstream.once("close", () => openSockets.delete(upstream));
    client.pipe(upstream);
    upstream.pipe(client);
    client.on("error", () => upstream.destroy());
    upstream.on("error", () => client.destroy());
  });
  await new Promise<void>((resolve) => proxy.listen(0, "127.0.0.1", resolve));
  const proxyAddr = proxy.address();
  if (!proxyAddr || typeof proxyAddr === "string") throw new Error("Proxy fixture did not bind");

  const control = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch(request) {
      const next = new URL(request.url).searchParams.get("mode") as typeof mode | null;
      const count = modeConnections;
      if (next) {
        mode = next;
        dropAfterRead = next === "post_dispatch_drop";
        modeConnections = 0;
      }
      return Response.json({ count });
    },
  });

  try {
    const run = join(dir, "probe.ts");
    const networkMod = new URL("../network.ts", import.meta.url).pathname;
    writeFileSync(
      run,
      `
import { createTransport } from ${JSON.stringify(networkMod)};
const controlUrl = ${JSON.stringify(control.url.toString())};
const setMode = async (m: string) => {
  const res = await fetch(controlUrl + "?mode=" + m);
  return ((await res.json()) as { count: number }).count;
};
const getCount = async () => {
  const res = await fetch(controlUrl);
  return ((await res.json()) as { count: number }).count;
};
const t = createTransport(1, {
  dnsLookup: async () => [{ address: "127.0.0.1", family: 4 }],
});
const okUrl = new URL("https://retry.example:${proxyAddr.port}/");
const badHostUrl = new URL("https://wrong.example:${proxyAddr.port}/");
const out: Record<string, unknown> = {};
try {
  await setMode("reset_first");
  const prepared = await t.prepareDispatch(okUrl, 8, {}, true);
  const r1 = await prepared.fetchFast({
    method: "POST",
    body: "prepared",
    dispatchDeadline: performance.now() + 2000,
  });
  out.prepared = {
    status: r1.fast ? r1.status : r1.response.status,
    conns: await getCount(),
  };

  await setMode("reset_first");
  const r2 = await t.fetchFast(
    okUrl,
    { method: "POST", body: "buffered", dispatchDeadline: performance.now() + 2000 },
    true,
  );
  out.fetchFast = {
    status: r2.fast ? r2.status : r2.response.status,
    conns: await getCount(),
  };

  await setMode("reset_first");
  const r3 = await t.fetch(
    okUrl,
    { method: "POST", body: "fetch", dispatchDeadline: performance.now() + 2000 },
    true,
  );
  out.fetch = {
    status: r3.status,
    body: await r3.text(),
    conns: await getCount(),
  };

  await setMode("pass");
  try {
    await t.fetchFast(badHostUrl, { method: "POST", body: "mismatch" }, true);
    out.mismatch = { code: "ACCEPTED", conns: await getCount() };
  } catch (e) {
    out.mismatch = { code: (e as { code?: string }).code, conns: await getCount() };
  }

  await setMode("plaintext");
  try {
    await t.fetchFast(okUrl, { method: "POST", body: "plaintext" }, true);
    out.plaintext = { code: "ACCEPTED", conns: await getCount() };
  } catch (e) {
    out.plaintext = { code: (e as { code?: string }).code, conns: await getCount() };
  }

  await setMode("always_reset");
  try {
    await t.fetchFast(okUrl, { method: "POST", body: "always" }, true);
    out.alwaysReset = { code: "ACCEPTED", conns: await getCount() };
  } catch (e) {
    out.alwaysReset = { code: (e as { code?: string }).code, conns: await getCount() };
  }

  await setMode("post_dispatch_drop");
  try {
    const rDrop = await t.fetchFast(okUrl, { method: "POST", body: "dispatched" }, true);
    if (!rDrop.fast) await rDrop.response.text();
    out.postDispatch = { ok: true, conns: await getCount() };
  } catch {
    out.postDispatch = { ok: false, conns: await getCount() };
  }
} finally {
  t.close();
}
console.log(JSON.stringify(out));
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
    expect({ code, stderr }).toEqual({ code: 0, stderr: "" });
    expect(JSON.parse(stdout.trim())).toEqual({
      prepared: { status: 200, conns: 2 },
      fetchFast: { status: 200, conns: 2 },
      fetch: { status: 200, body: "ok", conns: 2 },
      mismatch: { code: "upstream_tls", conns: 1 },
      plaintext: { code: "upstream_tls", conns: 1 },
      alwaysReset: { code: "upstream_tls", conns: 2 },
      postDispatch: { ok: false, conns: 1 },
    });
    expect(requests).toEqual(["prepared", "buffered", "fetch", "dispatched"]);
  } finally {
    await control.stop(true);
    await backend.stop(true);
    for (const socket of openSockets) socket.destroy();
    await new Promise<void>((resolve) => proxy.close(() => resolve()));
    rmSync(dir, { recursive: true, force: true });
  }
});
