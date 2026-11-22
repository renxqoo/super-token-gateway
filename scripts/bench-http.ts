import { get as httpGet, request as httpRequest, type Agent } from "node:http";

export function benchmarkConcurrency(raw: string | undefined, fallback: number[]): number[] {
  if (!raw) return fallback;
  const values = raw.split(",").map(Number);
  if (values.some((value) => !Number.isInteger(value) || value < 1 || value > 8192)) {
    throw new Error("BENCH_JSON_CONCURRENCY requires comma-separated integers from 1 to 8192");
  }
  return values;
}

export function postJsonKeepAlive(
  agent: Agent,
  port: number,
  apiKey: string,
  payload: Buffer,
  expectDone: boolean,
  expectedText?: string,
): Promise<{ ok: boolean; status: number; errText?: string }> {
  return new Promise((resolveDone) => {
    const req = httpRequest(
      {
        hostname: "127.0.0.1",
        port,
        path: "/v1/chat/completions",
        method: "POST",
        agent,
        headers: {
          authorization: `Bearer ${apiKey}`,
          "content-type": "application/json",
          "content-length": payload.byteLength,
        },
      },
      (res) => {
        const status = res.statusCode ?? 0;
        let sawDone = !expectDone;
        let tail = "";
        let fixtureBody = "";
        const errChunks: Buffer[] = [];
        res.on("data", (chunk: Buffer) => {
          if (expectedText && fixtureBody.length < 16384) fixtureBody += chunk.toString("utf8");
          if (status !== 200 && errChunks.length < 4) errChunks.push(chunk);
          if (!sawDone && status === 200) {
            const next = tail + chunk.toString("utf8");
            const terminal = "data: [DONE]\n\n";
            sawDone = next.includes(terminal);
            tail = next.slice(1 - terminal.length);
          }
        });
        res.on("end", () =>
          resolveDone({
            ok:
              status === 200 &&
              sawDone &&
              (expectedText === undefined ||
                (fixtureBody.includes(expectedText) && !fixtureBody.includes("alice@example.com"))),
            status,
            ...(status !== 200
              ? { errText: Buffer.concat(errChunks).toString("utf8").slice(0, 160) }
              : {}),
          }),
        );
        res.on("error", (e) => resolveDone({ ok: false, status, errText: e.message }));
      },
    );
    req.on("error", (e) => resolveDone({ ok: false, status: 0, errText: e.message }));
    req.setTimeout(expectedText ? 15_000 : 360_000, () =>
      req.destroy(new Error("Benchmark HTTP request timed out")),
    );
    req.end(payload);
  });
}

export async function warmHttpConnections(agent: Agent, port: number, target: number) {
  for (let warm = Math.min(64, target); ; warm = Math.min(warm + 64, target)) {
    await Promise.all(
      Array.from(
        { length: warm },
        () =>
          new Promise<void>((done, fail) => {
            const req = httpGet({ host: "127.0.0.1", port, path: "/livez", agent }, (response) => {
              response.resume();
              response.on("error", fail);
              response.on("end", () => {
                if (response.statusCode === 200) done();
                else fail(new Error(`Warmup HTTP status ${response.statusCode}`));
              });
            });
            req.on("error", fail);
            req.setTimeout(5000, () => req.destroy(new Error("Warmup HTTP request timed out")));
          }),
      ),
    );
    if (warm === target) return;
    await Bun.sleep(10);
  }
}
