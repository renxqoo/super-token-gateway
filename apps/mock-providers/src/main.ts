import { createMockApp } from "./app.ts";
const port = Number(process.env.MOCK_PORT ?? 9088);
const token = process.env.MOCK_CONTROL_TOKEN ?? crypto.randomUUID();
const { app } = createMockApp(token);
const tlsKey = process.env.MOCK_TLS_KEY_FILE,
  tlsCert = process.env.MOCK_TLS_CERT_FILE;
if (Boolean(tlsKey) !== Boolean(tlsCert))
  throw new Error("MOCK_TLS_KEY_FILE and MOCK_TLS_CERT_FILE must be set together");
const server = Bun.serve({
  hostname: process.env.MOCK_HOST ?? "127.0.0.1",
  port,
  fetch: app.fetch,
  idleTimeout: 30,
  ...(tlsKey && tlsCert ? { tls: { key: Bun.file(tlsKey), cert: Bun.file(tlsCert) } } : {}),
});
console.log(JSON.stringify({ service: "mock-model-providers", port: server.port }));
process.on("SIGTERM", () => {
  void server.stop();
});
