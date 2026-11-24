import { readFile } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";
import type { GatewayConfig } from "./config.ts";

type Settings = NonNullable<GatewayConfig["ingressPressure"]>;
type Sample = { cpu?: number; memory?: { working: number; total: number } };
type SensorState = { blocked: boolean; high: number; low: number; sampledAt?: number };
type ReadText = (path: string) => Promise<string>;

const systemRead: ReadText = (path) => readFile(path, "utf8");

function mountPath(value: string): string {
  return value.replace(/\\([0-7]{3})/g, (_, digits: string) =>
    String.fromCharCode(Number.parseInt(digits, 8)),
  );
}

async function findGroups(read: ReadText): Promise<string[]> {
  const [membership, mounts] = await Promise.all([
    read("/proc/self/cgroup"),
    read("/proc/self/mountinfo"),
  ]);
  const group = membership.match(/^0::(\/[^\n]*)$/m)?.[1];
  if (!group) return [];
  const direct: string[][] = [];
  const relativeToNamespace: string[][] = [];
  for (const line of mounts.split("\n")) {
    const [before, after] = line.split(" - ", 2);
    if (!after?.startsWith("cgroup2 ")) continue;
    const fields = before?.split(" ");
    if (!fields?.[3] || !fields[4]) continue;
    const root = mountPath(fields[3]);
    const mount = resolve(mountPath(fields[4]));
    const offset = relative(root, group);
    const outside = offset === ".." || offset.startsWith("../");
    const child = resolve(mount, outside ? `.${group}` : offset);
    if (child !== mount && !child.startsWith(mount + "/")) continue;
    const paths = [child];
    while (paths.at(-1) !== mount) paths.push(dirname(paths.at(-1)!));
    (outside ? relativeToNamespace : direct).push(paths);
  }
  direct.sort((a, b) => b.length - a.length);
  for (const paths of [...direct, ...relativeToNamespace]) {
    const directory = paths[0]!;
    try {
      await Promise.any([
        read(join(directory, "cpu.stat")),
        read(join(directory, "memory.current")),
      ]);
      return paths;
    } catch {
      continue;
    }
  }
  return [];
}

function integer(value?: string): number | undefined {
  if (!value || !/^\d+$/.test(value)) return undefined;
  const amount = Number(value);
  return Number.isSafeInteger(amount) ? amount : undefined;
}

function stat(text: string | undefined, key: string): number | undefined {
  return integer(
    text
      ?.split("\n")
      .find((line) => line.startsWith(key + " "))
      ?.split(" ")[1],
  );
}

function cpuSetSize(text?: string): number | undefined {
  if (!text) return undefined;
  let count = 0;
  for (const range of text.split(",")) {
    const [first, last, extra] = range.split("-");
    const start = integer(first);
    const end = integer(last ?? first);
    if (extra !== undefined || start === undefined || end === undefined || end < start)
      return undefined;
    count += end - start + 1;
  }
  return count || undefined;
}

export function createCgroupPressureSampler(
  read = systemRead,
  now = () => performance.now(),
  platform = process.platform,
) {
  let groups: string[] | undefined;
  let previous = new Map<string, { usage: number; at: number; cores: number }>();
  const readAt = (path: string) =>
    read(path)
      .then((value) => value.trim())
      .catch(() => undefined);
  return async (): Promise<Sample> => {
    if (platform !== "linux") return {};
    groups ??= await findGroups(read).catch(() => []);
    if (groups.length === 0) {
      groups = undefined;
      return {};
    }
    const at = now();
    const next = new Map<string, { usage: number; at: number; cores: number }>();
    const readings = await Promise.all(
      groups.map(async (directory, index) => {
        const [cpuMax, cpuStat, cpuset, memoryMax, memoryCurrent, memoryStat] = await Promise.all(
          [
            "cpu.max",
            "cpu.stat",
            "cpuset.cpus.effective",
            "memory.max",
            "memory.current",
            "memory.stat",
          ].map((name) => readAt(join(directory, name))),
        );
        const [quotaText, periodText] = cpuMax?.split(/\s+/) ?? [];
        const quota = integer(quotaText);
        const period = integer(periodText);
        const visibleCores = cpuSetSize(cpuset);
        const cores =
          quota && period
            ? Math.min(quota / period, visibleCores ?? Infinity)
            : index === 0
              ? visibleCores
              : undefined;
        const usage = stat(cpuStat, "usage_usec");
        const last = previous.get(directory);
        if (usage !== undefined && cores) next.set(directory, { usage, at, cores });
        const cpu =
          usage !== undefined &&
          last &&
          usage >= last.usage &&
          at > last.at &&
          cores &&
          cores === last.cores
            ? ((usage - last.usage) / ((at - last.at) * 1_000 * cores)) * 100
            : undefined;
        const max = integer(memoryMax);
        const current = integer(memoryCurrent);
        const inactive = stat(memoryStat, "inactive_file") ?? 0;
        const memory =
          max && current !== undefined
            ? {
                working: (Math.max(0, current - inactive) / max) * 100,
                total: (current / max) * 100,
              }
            : undefined;
        return { cpu, memory, readable: cpuStat !== undefined || memoryCurrent !== undefined };
      }),
    );
    previous = next;
    if (!readings.some(({ readable }) => readable)) groups = undefined;
    const result: Sample = {};
    for (const { cpu, memory } of readings) {
      if (cpu !== undefined) result.cpu = Math.max(result.cpu ?? 0, cpu);
      if (memory)
        result.memory = {
          working: Math.max(result.memory?.working ?? 0, memory.working),
          total: Math.max(result.memory?.total ?? 0, memory.total),
        };
    }
    return result;
  };
}

export function createPressureMonitor(
  settings: Settings,
  read = createCgroupPressureSampler(),
  now = () => performance.now(),
) {
  const cpu: SensorState = { blocked: false, high: 0, low: 0 };
  const memory: SensorState = { blocked: false, high: 0, low: 0 };
  let timer: ReturnType<typeof setInterval> | undefined;
  let sampling = false;
  let latest: Sample = {};

  function update(
    state: SensorState,
    value: number | undefined,
    high: number,
    low: number,
    critical = false,
  ) {
    if (value === undefined || !Number.isFinite(value)) {
      state.high = state.low = 0;
      return;
    }
    const at = now();
    if (state.sampledAt === undefined || at - state.sampledAt >= settings.staleMs)
      state.high = state.low = 0;
    state.sampledAt = at;
    state.high = value >= high || critical ? state.high + 1 : 0;
    state.low = value <= low && !critical ? state.low + 1 : 0;
    if (critical || state.high >= settings.highSamples) state.blocked = true;
    if (state.low >= settings.recoverySamples) state.blocked = false;
  }

  async function sample() {
    if (sampling) return;
    sampling = true;
    try {
      latest = await read().catch((): Sample => ({}));
      const { cpu: load, memory: usage } = latest;
      update(cpu, load, settings.cpuHigh, settings.cpuRecovery);
      update(
        memory,
        usage
          ? Math.max(usage.working, usage.total >= settings.memoryCritical ? usage.total : 0)
          : undefined,
        settings.memoryHigh,
        settings.memoryRecovery,
        usage !== undefined && usage.total >= settings.memoryCritical,
      );
    } finally {
      sampling = false;
    }
  }

  return {
    sample,
    start() {
      if (timer !== undefined) return;
      void sample();
      timer = setInterval(() => void sample(), settings.sampleMs);
      timer.unref();
    },
    stop() {
      clearInterval(timer);
      timer = undefined;
    },
    reason() {
      return memory.blocked ? "memory" : cpu.blocked ? "cpu" : undefined;
    },
    metrics(): string[] {
      const recent = (state: SensorState) =>
        state.sampledAt !== undefined && now() - state.sampledAt < settings.staleMs;
      return [
        `gateway_ingress_pressure_sample_fresh{resource="cpu"} ${Number(recent(cpu))}`,
        `gateway_ingress_pressure_sample_fresh{resource="memory"} ${Number(recent(memory))}`,
        `gateway_ingress_pressure_active{resource="cpu"} ${Number(cpu.blocked)}`,
        `gateway_ingress_pressure_active{resource="memory"} ${Number(memory.blocked)}`,
        ...(recent(cpu) && latest.cpu !== undefined
          ? [`gateway_ingress_cpu_usage_percent ${latest.cpu}`]
          : []),
        ...(recent(memory) && latest.memory
          ? [
              `gateway_ingress_memory_working_percent ${latest.memory.working}`,
              `gateway_ingress_memory_total_percent ${latest.memory.total}`,
            ]
          : []),
      ];
    },
  };
}
