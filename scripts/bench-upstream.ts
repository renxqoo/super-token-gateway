const port = Number(process.env.MOCK_PORT);
const token = process.env.MOCK_CONTROL_TOKEN;
if (!Number.isSafeInteger(port) || port < 1 || port > 65535 || !token)
  throw new Error("Benchmark fixture requires a port and control token");
const output = JSON.stringify({
  id: "chatcmpl-fixture",
  object: "chat.completion",
  model: "demo",
  choices: [
    { index: 0, message: { role: "assistant", content: "Hello, 世界 🌍" }, finish_reason: "stop" },
  ],
  usage: {
    prompt_tokens: 10,
    completion_tokens: 5,
    total_tokens: 15,
    prompt_tokens_details: { cached_tokens: 3 },
  },
});
let requests = 0;
const server = Bun.serve({
  hostname: "127.0.0.1",
  port,
  idleTimeout: 30,
  async fetch(request) {
    const { pathname } = new URL(request.url);
    if (pathname === "/healthz") return Response.json({ status: "ready" });
    if (
      pathname === "/__control/stats" &&
      request.headers.get("authorization") === `Bearer ${token}`
    )
      return Response.json({
        requests,
        active: 0,
        cancellations: 0,
        bytes: requests * Buffer.byteLength(output),
      });
    if (pathname !== "/openai/v1/chat/completions" || request.method !== "POST")
      return new Response(null, { status: 404 });
    await request.arrayBuffer();
    requests++;
    return new Response(output, { headers: { "content-type": "application/json" } });
  },
});
process.on("SIGTERM", () => void server.stop());
