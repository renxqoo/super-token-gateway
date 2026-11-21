import { request as httpRequest, type Agent } from "node:http";

export function postJsonKeepAlive(
  agent: Agent,
  port: number,
  apiKey: string,
  payload: Buffer,
  expectDone: boolean,
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
        const errChunks: Buffer[] = [];
        res.on("data", (chunk: Buffer) => {
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
            ok: status === 200 && sawDone,
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
    req.setTimeout(360_000, () => req.destroy(new Error("Benchmark HTTP request timed out")));
    req.end(payload);
  });
}
