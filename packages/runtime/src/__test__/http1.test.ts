import { expect, test } from "bun:test";
import { createServer } from "node:net";
import { gzipSync } from "node:zlib";
import { createTransport } from "../network.ts";

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

test("bun HTTP/1.1 fetchFast buffers single-packet chunked and Content-Length SSE while streaming incremental chunks", async () => {
  const sseChunk1 = 'data: {"choices":[{"delta":{"content":"hi"}}]}\n\n';
  const sseChunk2 = "data: [DONE]\n\n";
  const expectedSse = sseChunk1 + sseChunk2;
  const encodeChunk = (text: string) => `${Buffer.byteLength(text).toString(16)}\r\n${text}\r\n`;
  let reqNum = 0;
  let connections = 0;
  const server = createServer((socket) => {
    connections++;
    socket.on("data", () => {
      reqNum++;
      if (reqNum === 1) {
        socket.write(
          "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream; charset=utf-8\r\nTransfer-Encoding: chunked\r\n\r\n" +
            encodeChunk(sseChunk1) +
            encodeChunk(sseChunk2) +
            "0\r\n\r\n",
        );
      } else if (reqNum === 2) {
        socket.write(
          `HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: ${Buffer.byteLength(expectedSse)}\r\n\r\n${expectedSse}`,
        );
      } else if (reqNum === 3) {
        socket.write(
          "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n" +
            encodeChunk(sseChunk1),
        );
        setTimeout(() => {
          if (!socket.destroyed) socket.write(encodeChunk(sseChunk2) + "0\r\n\r\n");
        }, 30);
      } else if (reqNum === 4) {
        socket.end(
          "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\nbad\r\n0\r\n\r\n",
        );
      } else {
        socket.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
      }
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  const port = typeof address === "object" && address ? address.port : 0;
  const transport = createTransport(1);
  try {
    const url = new URL(`http://127.0.0.1:${port}/`);
    const decoder = new TextDecoder();

    const r1 = await transport.fetchFast(url, { signal: AbortSignal.timeout(1500) }, true);
    expect(r1.fast).toBe(true);
    if (r1.fast) {
      expect(r1.status).toBe(200);
      expect(decoder.decode(r1.body)).toBe(expectedSse);
    }

    const r2 = await transport.fetchFast(url, { signal: AbortSignal.timeout(1500) }, true);
    expect(r2.fast).toBe(true);
    if (r2.fast) {
      expect(r2.status).toBe(200);
      expect(decoder.decode(r2.body)).toBe(expectedSse);
    }

    const r3 = await transport.fetchFast(url, { signal: AbortSignal.timeout(1500) }, true);
    expect(r3.fast).toBe(false);
    if (!r3.fast) {
      expect(r3.response.status).toBe(200);
      expect(await r3.response.text()).toBe(expectedSse);
    }
    expect(connections).toBe(1);

    await expect(
      transport
        .fetchFast(url, { signal: AbortSignal.timeout(1500) }, true)
        .then((res) => (res.fast ? decoder.decode(res.body) : res.response.text())),
    ).rejects.toThrow();

    const r5 = await transport.fetchFast(url, { signal: AbortSignal.timeout(1500) }, true);
    expect(r5.fast).toBe(true);
    if (r5.fast) {
      expect(r5.status).toBe(200);
      expect(decoder.decode(r5.body)).toBe("ok");
    }
    expect(connections).toBe(2);
  } finally {
    transport.close();
    server.close();
  }
});
