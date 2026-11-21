import { createServer } from "node:net";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { join, resolve } from "node:path";

async function localPort(): Promise<number> {
  const socket = createServer().listen(0, "127.0.0.1");
  await once(socket, "listening");
  const address = socket.address();
  await new Promise<void>((done) => socket.close(() => done()));
  if (!address || typeof address === "string") throw new Error("Could not allocate a test port");
  return address.port;
}

type Delivery = { status?: string; commonLabels?: { alertname?: string; severity?: string } };
const delivered = Promise.withResolvers<Delivery>();
const root = await mkdtemp("/tmp/stg-alerts-");
const children: Bun.Subprocess[] = [];
let samples = 0;
const endpoint = Bun.serve({
  hostname: "127.0.0.1",
  port: 0,
  async fetch(req) {
    if (new URL(req.url).pathname === "/metrics") {
      samples++;
      return new Response(
        `# TYPE gateway_ledger_commit_failures_total counter\ngateway_ledger_commit_failures_total ${Number(samples > 3)}\n`,
        { headers: { "content-type": "text/plain; version=0.0.4" } },
      );
    }
    if (req.method === "POST") {
      const notification = (await req.json()) as Delivery;
      if (notification.commonLabels?.alertname === "STGLedgerCommitFailure")
        delivered.resolve(notification);
      return new Response(null, { status: 204 });
    }
    return new Response(null, { status: 404 });
  },
});
let timeout: ReturnType<typeof setTimeout> | undefined;
try {
  const [managerPort, prometheusPort] = await Promise.all([localPort(), localPort()]);
  const amConfig = join(root, "alertmanager.yml");
  const promConfig = join(root, "prometheus.yml");
  const template = await Bun.file("infra/monitoring/alertmanager.example.yml").text();
  await Bun.write(
    amConfig,
    template
      .replace("https://alert-receiver.internal/stg", `http://127.0.0.1:${endpoint.port}/stg`)
      .replace("group_wait: 15s", "group_wait: 1s"),
  );
  await Bun.write(
    promConfig,
    `global:
  scrape_interval: 1s
  evaluation_interval: 1s
rule_files:
  - ${JSON.stringify(resolve("infra/monitoring/alerts.yml"))}
alerting:
  alertmanagers:
    - static_configs:
        - targets: ["127.0.0.1:${managerPort}"]
scrape_configs:
  - job_name: stg-gateway
    static_configs:
      - targets: ["127.0.0.1:${endpoint.port}"]
        labels:
          stg_cluster: delivery-test
`,
  );
  const started = performance.now();
  children.push(
    Bun.spawn(
      [
        process.env.ALERTMANAGER_BIN ?? "alertmanager",
        `--config.file=${amConfig}`,
        `--storage.path=${join(root, "alerts")}`,
        `--web.listen-address=127.0.0.1:${managerPort}`,
        "--cluster.listen-address=",
      ],
      { stdout: "ignore", stderr: "inherit" },
    ),
  );
  children.push(
    Bun.spawn(
      [
        process.env.PROMETHEUS_BIN ?? "prometheus",
        `--config.file=${promConfig}`,
        `--storage.tsdb.path=${join(root, "metrics")}`,
        `--web.listen-address=127.0.0.1:${prometheusPort}`,
      ],
      { stdout: "ignore", stderr: "inherit" },
    ),
  );
  const receipt = await Promise.race([
    delivered.promise,
    ...children.map((child) =>
      child.exited.then((code) => {
        throw new Error(`Monitoring process exited with status ${code}`);
      }),
    ),
    new Promise<never>((_, reject) => {
      timeout = setTimeout(
        () => reject(new Error("Did not receive firing alert in 30 seconds")),
        30_000,
      );
    }),
  ]);
  if (receipt.status !== "firing" || receipt.commonLabels?.severity !== "critical") {
    throw new Error("Receiver did not receive a firing critical settlement alert");
  }
  console.log(
    JSON.stringify({
      delivered: true,
      alert: receipt.commonLabels.alertname,
      samples,
      elapsed_ms: Number((performance.now() - started).toFixed(2)),
    }),
  );
} finally {
  clearTimeout(timeout);
  for (const child of children) if (child.exitCode === null) child.kill();
  await Promise.allSettled(children.map((child) => child.exited));
  await endpoint.stop(true);
  await rm(root, { recursive: true, force: true });
}
