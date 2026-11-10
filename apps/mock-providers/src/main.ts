import { createMockApp } from "./app.ts";
const port = Number(process.env.MOCK_PORT ?? 9088);
const token = process.env.MOCK_CONTROL_TOKEN ?? crypto.randomUUID();
const { app } = createMockApp(token);
const server = Bun.serve({
  hostname: process.env.MOCK_HOST ?? "127.0.0.1",
  port,
  fetch: app.fetch,
  idleTimeout: 30,
});
console.log(JSON.stringify({ service: "mock-model-providers", port: server.port }));
process.on("SIGTERM", () => {
  void server.stop();
});
