import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

async function docker(args: string[]): Promise<string> {
  const child = Bun.spawn(["docker", ...args], { stdout: "pipe", stderr: "pipe" });
  const [status, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  if (status !== 0) throw new Error(`docker ${args[0]} failed: ${stderr.trim()}`);
  return stdout.trim();
}

const upstreamBody = "gateway-ingress-fixture";
const directory = await mkdtemp(join(tmpdir(), "stg-gateway-ingress-"));
const name = `stg-gateway-ingress-${crypto.randomUUID()}`;
let created = false;
try {
  const template = await Bun.file(new URL("../infra/Caddyfile", import.meta.url)).text();
  const configPath = join(directory, "Caddyfile");
  const listener = template.replace(/^:443\s*\{/m, "http://:18080 {");
  if (listener === template) throw new Error("Gateway ingress listener is missing");
  await Bun.write(
    configPath,
    `{\n\tadmin off\n}\n${listener}\nhttp://:8080, http://:8082 {\n\tbind 127.0.0.1\n\trespond "${upstreamBody}" 200\n}\n`,
  );
  await docker([
    "create",
    "--pull=never",
    "--name",
    name,
    "-p",
    "127.0.0.1::18080",
    "--mount",
    `type=bind,source=${configPath},target=/etc/caddy/Caddyfile,readonly`,
    "caddy:2-alpine",
  ]);
  created = true;
  await docker(["start", name]);
  const base = `http://${await docker(["port", name, "18080/tcp"])}`;
  let ready = false;
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline && !ready) {
    try {
      const response = await fetch(`${base}/v1/models`, { signal: AbortSignal.timeout(1000) });
      ready = response.ok && (await response.text()) === upstreamBody;
      if (!response.ok) await response.body?.cancel();
    } catch {}
    if (!ready) await Bun.sleep(200);
  }
  if (!ready) throw new Error("Gateway ingress did not forward requests to the fixture");

  const videoTask = "/v1/videos/vid_00000000000000000000000000000001";
  const openapi = (await Bun.file(new URL("../docs/openapi.json", import.meta.url)).json()) as {
    paths: Record<string, Record<string, Record<string, unknown>>>;
  };
  const documented = Object.entries(openapi.paths).flatMap(([path, operations]) =>
    Object.entries(operations)
      .filter(
        ([, schema]) =>
          schema["x-stg-listener"] === "public" &&
          schema["x-stg-production-listener"] !== "internal",
      )
      .map(([method]) => ({
        path: path
          .replace("{action}", "fixture-model:generateContent")
          .replace("/v1/videos/{taskId}", videoTask),
        method: method.toUpperCase(),
        status: 200,
      })),
  );
  const deniedRoutes: [method: string, path: string][] = [
    ["GET", "/metrics"],
    ["GET", "/internal/v1/health"],
    ["GET", "/admin/v1/users"],
    ["POST", "/internal/outbox/events"],
    ["GET", "/v1/music/generations"],
    ["GET", "/v1/videos"],
    ["POST", videoTask],
    ["GET", `${videoTask}/cancel`],
    ["POST", `${videoTask}/extra/cancel`],
  ];
  const denied = deniedRoutes.map(([method, path]) => ({ method, path, status: 404 }));
  const results = await Promise.all(
    [...documented, ...denied].map(async ({ method, path, status }) => {
      const response = await fetch(base + path, {
        method,
        redirect: "manual",
        signal: AbortSignal.timeout(3000),
      });
      const body = await response.text();
      return {
        method,
        path,
        expected: status,
        actual: response.status,
        ok: response.status === status && (body === upstreamBody) === (status === 200),
      };
    }),
  );
  const failures = results.filter(({ ok }) => !ok);
  console.log(
    JSON.stringify({ checks: results.length, passed: results.length - failures.length, failures }),
  );
  if (failures.length > 0) process.exitCode = 1;
} finally {
  try {
    if (created) await docker(["rm", "--force", name]);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}
