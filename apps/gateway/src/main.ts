import { createTerminalBudget } from "./terminal-budget.ts";
import { createCoreRpcClient } from "@gateway/core-client";
import { Lifecycle, UsageProducer, createTransport, evidenceChannel } from "@gateway/runtime";
import { createGatewayApp } from "./app.ts";
import { createSendRegistry, type ChatDependencies } from "./chat.ts";
import { loadGatewayConfig } from "./config.ts";
import { createPackageCache, loadPackageTrust } from "./packages.ts";
import { startResourcePolling } from "./resource-poller.ts";
import { createGatewayPluginRuntime } from "./plugin-runtime.ts";

const config = await loadGatewayConfig();
const trust = await loadPackageTrust(config.plugins.trust_policy);
const host = await createGatewayPluginRuntime({
  executable: config.plugins.helper_executable,
  trust,
});
const core = createCoreRpcClient({
  origin: config.core.origin,
  servername: config.core.servername,
  audience: config.core.audience,
  expectedPeerSha256: config.core.peer_sha256,
  expectedSubjectAltname: config.core.subject_altname,
  key: await Bun.file(config.core.key_file).text(),
  cert: await Bun.file(config.core.cert_file).text(),
  ca: await Bun.file(config.core.ca_file).text(),
});
const producer = new UsageProducer({
  channel: evidenceChannel,
  url: config.queue.url,
  namespace: config.queue.namespace,
  replicas: config.queue.required_replicas,
  timeoutMs: config.queue.ack_timeout_ms,
  maxInFlight: config.max_inflight,
  requireAllReplicas: config.production,
});
const transport = createTransport(config.upstream_pool_size);
const grants = createSendRegistry();
const lifecycle = new Lifecycle(config.max_inflight);
async function release() {
  transport.close();
  producer.close();
  await host.close();
  await core.close();
}
try {
  await core.startup();
  await producer.ready();
} catch (error) {
  await release();
  throw error;
}
const deps: ChatDependencies = {
  config,
  core,
  packages: createPackageCache(
    (digest, deadlineAt) => core.pluginPackage(digest, { deadlineAt }),
    trust,
  ),
  host,
  transport,
  grants,
  terminalBudget: createTerminalBudget(config.max_inflight),
  publish: (events) => producer.publish({ events }),
};
const poller = startResourcePolling(deps, config.resource_poll_ms, () =>
  console.log(JSON.stringify({ service: "gateway", event: "resource_poll_failed" })),
);
const app = createGatewayApp({
  ...deps,
  lifecycle,
  ready: async () => {
    await core.current();
    await producer.ready();
  },
});
const server = Bun.serve({
  hostname: config.hostname,
  port: config.port,
  maxRequestBodySize: config.max_body_bytes,
  idleTimeout: 30,
  fetch: async (request, server) => {
    const response = await app.fetch(request, {
      peerAddress: server.requestIP(request)?.address,
    });
    if (response.headers.get("content-type")?.includes("text/event-stream"))
      server.timeout(request, 0);
    return response;
  },
  error: () =>
    Response.json(
      { error: { code: "server_error", message: "Gateway server error" } },
      { status: 500 },
    ),
});
console.log(JSON.stringify({ service: "gateway", event: "ready", port: server.port }));
let stopping = false;
async function shutdown() {
  if (stopping) return;
  stopping = true;
  lifecycle.accepting = false;
  const stopped = server.stop(false);
  const drained = await lifecycle.drain(config.shutdown_grace_ms, 5000);
  grants.close();
  await poller.close();
  await Promise.race([stopped, Bun.sleep(1000)]);
  await server.stop(true);
  await release();
  console.log(
    JSON.stringify({ service: "gateway", event: "stopped", drained, grants: grants.size() }),
  );
  process.exit(0);
}
process.on("SIGTERM", () => void shutdown());
process.on("SIGINT", () => void shutdown());
