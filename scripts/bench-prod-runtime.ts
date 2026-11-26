import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { join, resolve } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

export async function findFreePort(): Promise<number> {
  return new Promise((resolvePort, rejectPort) => {
    const srv = createServer();
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address();
      const port = typeof addr === "object" && addr ? addr.port : 0;
      srv.close(() => resolvePort(port));
    });
    srv.on("error", rejectPort);
  });
}

export async function waitForJson(url: string): Promise<Record<string, unknown>> {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(url);
      if (res.ok) return (await res.json()) as Record<string, unknown>;
    } catch {
      void 0;
    }
    await Bun.sleep(100);
  }
  throw new Error(`Timed out waiting for ${url}`);
}

export function runSync(cwd: string, args: string[], env: Record<string, string>): Promise<void> {
  return new Promise((done, fail) => {
    const proc = spawn(process.execPath, args, { cwd, env, stdio: "inherit" });
    proc.on("exit", (code) => {
      if (code === 0) done();
      else fail(new Error(`Exited ${code}: ${args.join(" ")}`));
    });
  });
}

export async function runPgBin(pgBin: string, name: string, args: string[]): Promise<void> {
  const proc = Bun.spawn([join(pgBin, name), ...args], { stdout: "pipe", stderr: "pipe" });
  const [code, stdout, stderr] = await Promise.all([
    proc.exited,
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
  ]);
  if (code !== 0) throw new Error(`${name} failed (${code}): ${stdout} ${stderr}`);
}

export async function stopProcesses(procs: ReturnType<typeof spawn>[]): Promise<void> {
  const running = procs.filter(
    (proc) => proc.pid && proc.exitCode === null && proc.signalCode === null,
  );
  const exits = Promise.all(
    running.map(
      (proc) => new Promise<void>((resolveExit) => proc.once("exit", () => resolveExit())),
    ),
  );
  for (const proc of running) proc.kill("SIGTERM");
  await Promise.race([exits, sleep(12_000, undefined, { ref: false })]);
  for (const proc of running) {
    if (proc.exitCode === null && proc.signalCode === null) proc.kill("SIGKILL");
  }
  await exits;
}

export function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * p));
  return Number(sorted[idx]!.toFixed(2));
}

export const pgBin = process.env.PG_TOOLS_DIRECTORY ?? resolve(".local/toolchain/pg17/bin");
