import { fileURLToPath } from "node:url";
import { expect, test } from "bun:test";
import { createGatewayRuntime } from "../app.ts";
import { loadConfigFromEnv } from "../config.ts";
import { createBodyBufferBudget, createPublicIngress } from "../public-ingress.ts";
import { createCgroupPressureSampler, createPressureMonitor } from "../resource-pressure.ts";

const baseEnv = {
  GATEWAY_MODE: "gateway",
  DATABASE_URL: "postgres://fixture@127.0.0.1:1/fixture",
  INTERNAL_AUTH_FILE: fileURLToPath(
    new URL("../../../../tests/fixtures/internal-auth.json", import.meta.url),
  ),
  KEY_PEPPER: "fixture-key-pepper",
  ENCRYPTION_KEY: "00".repeat(32),
};

const ipEnv = {
  REDIS_URL: "redis://127.0.0.1:1",
  INGRESS_IP_MODE: "enforce",
  INGRESS_IP_WINDOW_MS: "10000",
  INGRESS_IP_MAX_REQUESTS: "2",
  INGRESS_IP_NODE_MAX_REQUESTS: "10",
  INGRESS_IP_MAX_PENDING: "1",
  INGRESS_IP_TIMEOUT_MS: "8",
};

const pressureEnv = {
  INGRESS_PRESSURE_MODE: "enforce",
  INGRESS_PRESSURE_SAMPLE_MS: "200",
  INGRESS_PRESSURE_STALE_MS: "900",
  INGRESS_PRESSURE_HIGH_SAMPLES: "2",
  INGRESS_PRESSURE_RECOVERY_SAMPLES: "2",
  INGRESS_CPU_HIGH_PERCENT: "80",
  INGRESS_CPU_RECOVERY_PERCENT: "65",
  INGRESS_MEMORY_HIGH_PERCENT: "83",
  INGRESS_MEMORY_RECOVERY_PERCENT: "70",
  INGRESS_MEMORY_CRITICAL_PERCENT: "94",
};

test("Gateway startup requires a role credentials file", () => {
  expect(() => loadConfigFromEnv({ ...baseEnv, INTERNAL_AUTH_FILE: undefined }, [])).toThrow(
    "INTERNAL_AUTH_FILE is required",
  );
});

test("video concurrency reads the billing account environment setting", () => {
  const config = loadConfigFromEnv(
    { ...baseEnv, MAX_ACTIVE_ASYNC_TASKS_PER_BILLING_ACCOUNT: "5" },
    [],
  );
  expect(config.maxActiveAsyncTasksPerBillingAccount).toBe(5);
});

test("ingress enforcement requires explicit limits and ordered pressure thresholds", () => {
  expect(() => loadConfigFromEnv({ ...baseEnv, INGRESS_IP_MODE: "enforce" }, [])).toThrow(
    "Ingress IP quota requires REDIS_URL",
  );
  expect(() =>
    loadConfigFromEnv({ ...baseEnv, ...ipEnv, INGRESS_IP_MAX_PENDING: undefined }, []),
  ).toThrow("INGRESS_IP_MAX_PENDING is required");
  expect(() =>
    loadConfigFromEnv({ ...baseEnv, ...pressureEnv, INGRESS_CPU_RECOVERY_PERCENT: "81" }, []),
  ).toThrow("Ingress pressure thresholds or sampling intervals are out of order");
  expect(() =>
    loadConfigFromEnv({ ...baseEnv, MAX_BODY_BYTES: "64", INGRESS_BODY_BUFFER_BYTES: "63" }, []),
  ).toThrow("INGRESS_BODY_BUFFER_BYTES");
});

test("public JSON and Gemini routes reject request coding or declared size before pulling the body", async () => {
  const runtime = createGatewayRuntime({
    config: loadConfigFromEnv({ ...baseEnv, MAX_BODY_BYTES: "64" }, []),
    wasmExecutor: null,
  });
  let pulled = 0;
  const body = () =>
    new ReadableStream<Uint8Array>(
      {
        pull() {
          pulled++;
        },
      },
      { highWaterMark: 0 },
    );
  try {
    const encoded = await runtime.fetchPublic(
      new Request("http://fixture.test/v1/chat/completions", {
        method: "POST",
        headers: { "content-encoding": "gzip, br" },
        body: body(),
      }),
    );
    expect(encoded.status).toBe(415);
    expect(encoded.headers.get("accept-encoding")).toBe("identity");
    expect(await encoded.json()).toMatchObject({ error: { code: "unsupported_content_encoding" } });
    const oversized = await runtime.fetchPublic(
      new Request("http://fixture.test/v1beta/models/fixture:generateContent", {
        method: "POST",
        headers: { "content-length": "65" },
        body: body(),
      }),
    );
    expect(oversized.status).toBe(413);
    expect(await oversized.json()).toMatchObject({ error: { code: 413 } });
    expect(pulled).toBe(0);
  } finally {
    await runtime.close();
  }
});

test("an in-flight Redis command keeps its pending slot until it settles", async () => {
  const config = loadConfigFromEnv({ ...baseEnv, ...ipEnv }, []);
  const first = Promise.withResolvers<number | null>();
  let calls = 0;
  const ingress = createPublicIngress(config, {
    takeIngressWindow(_ip, _limit, _windowMs, timeoutMs) {
      expect(timeoutMs).toBe(8);
      calls++;
      return calls === 1 ? first.promise : Promise.resolve(2_001);
    },
  });
  const request = new Request("http://fixture.test/v1/models");
  const pending = ingress.admit(request, "chat", "192.0.2.1");
  const full = await ingress.admit(request, "chat", "192.0.2.2");
  expect(await full?.json()).toMatchObject({ error: { code: "ingress_capacity_unavailable" } });
  expect(calls).toBe(1);
  first.resolve(null);
  const expired = await pending;
  expect(await expired?.json()).toMatchObject({ error: { code: "ingress_quota_unavailable" } });
  const blocked = await ingress.admit(request, "chat", "192.0.2.2");
  expect(blocked?.status).toBe(429);
  expect(blocked?.headers.get("retry-after")).toBe("3");
});

test("pressure sheds new requests, uses fresh recovery samples and allows observation mode", async () => {
  const config = loadConfigFromEnv({ ...baseEnv, ...pressureEnv }, []);
  const settings = config.ingressPressure!;
  let time = 0;
  let sample: { cpu?: number; memory?: { working: number; total: number } } = { cpu: 81 };
  const monitor = createPressureMonitor(
    settings,
    async () => sample,
    () => time,
  );
  const cluster = { takeIngressWindow: async () => 0 };
  const ingress = createPublicIngress(config, cluster, monitor);
  const observed = createPublicIngress(
    { ...config, ingressPressure: { ...settings, mode: "observe" } },
    cluster,
    monitor,
  );
  const request = new Request("http://fixture.test/v1/models");
  await monitor.sample();
  expect(await ingress.admit(request, "chat")).toBeUndefined();
  time += 200;
  await monitor.sample();
  const overloaded = await ingress.admit(request, "chat");
  expect(overloaded?.status).toBe(503);
  expect(await observed.admit(request, "chat")).toBeUndefined();
  expect(observed.metrics().join("\n")).toContain('reason="pressure_cpu",mode="observe"');

  time += 900;
  sample = {};
  await monitor.sample();
  expect((await ingress.admit(request, "chat"))?.status).toBe(503);
  sample = { cpu: 64 };
  await monitor.sample();
  expect((await ingress.admit(request, "chat"))?.status).toBe(503);
  time += 200;
  await monitor.sample();
  expect(await ingress.admit(request, "chat")).toBeUndefined();
  sample = { memory: { working: 68, total: 95 } };
  await monitor.sample();
  expect((await ingress.admit(request, "chat"))?.status).toBe(503);
});

test("cgroup v2 sampling uses the visible mount, fractional quota and matching memory accounting", async () => {
  const files = new Map([
    ["/proc/self/cgroup", "0::/gateway/node\n"],
    [
      "/proc/self/mountinfo",
      "1 0 0:1 /unrelated /sys/fs/other ro - cgroup2 cgroup rw\n" +
        "2 0 0:1 /gateway /sys/fs/cgroup ro - cgroup2 cgroup rw\n",
    ],
    ["/sys/fs/cgroup/node/cpu.max", "50000 100000"],
    ["/sys/fs/cgroup/node/cpu.stat", "usage_usec 1000"],
    ["/sys/fs/cgroup/node/cpuset.cpus.effective", "0-1"],
    ["/sys/fs/cgroup/node/memory.current", "300"],
    ["/sys/fs/cgroup/node/memory.max", "1000"],
    ["/sys/fs/cgroup/memory.current", "900"],
    ["/sys/fs/cgroup/memory.max", "1000"],
    ["/sys/fs/cgroup/memory.stat", "inactive_file 250\n"],
  ]);
  let time = 1000;
  const sample = createCgroupPressureSampler(
    async (file) => {
      const value = files.get(file);
      if (value === undefined) throw new Error("Missing fixture");
      return value;
    },
    () => time,
    "linux",
  );
  expect(await sample()).toEqual({ memory: { working: 65, total: 90 } });
  files.set("/sys/fs/cgroup/node/cpu.stat", "usage_usec 401000");
  time += 1000;
  expect(await sample()).toEqual({ cpu: 80, memory: { working: 65, total: 90 } });
});

test("body budget can be reused after one release without increasing its capacity", () => {
  const budget = createBodyBufferBudget(15);
  const release = budget.reserve(10);
  const other = budget.reserve(5);
  expect(() => budget.reserve(1)).toThrow("Request body capacity unavailable");
  release();
  release();
  expect(() => budget.reserve(11)).toThrow("Request body capacity unavailable");
  const next = budget.reserve(10);
  next();
  other();
  expect(budget.metrics().join("\n")).toContain("gateway_ingress_body_buffer_reserved_bytes 0");
});
