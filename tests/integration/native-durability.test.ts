import { expect, test } from "bun:test";
import { execFile } from "node:child_process";
import {
  lstat,
  mkdtemp,
  readFile,
  readdir,
  realpath,
  rm,
  stat,
  statfs,
  writeFile,
} from "node:fs/promises";
import { basename, dirname, isAbsolute, join } from "node:path";
import { createServer } from "node:net";
import { SQL } from "bun";
import {
  createNativeFixture,
  nativeChildEnvironment,
  nativeFixtureOptions,
  within,
} from "../support/native-fixture.ts";

const script = new URL("../../scripts/verify-durability.ts", import.meta.url).pathname;

async function empty(fixture: Awaited<ReturnType<typeof createNativeFixture>>) {
  await fixture.close();
  expect(await fixture.resources()).toEqual({
    processes: [],
    directories: [],
    ports: [],
    timers: 0,
  });
}

function exists(pid: number) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}

async function reapProbe(pid: number) {
  const deadline = Date.now() + 2000;
  while (exists(pid) || (await probeProcesses()).some((entry) => entry.group === pid)) {
    if (Date.now() > deadline) throw new Error("Owned version probe group survived cleanup");
    await Bun.sleep(10);
  }
}

test("native preflight rejects absent tools before default durability can invoke Docker or connect a supplied database", async () => {
  const directory = await mkdtemp("/tmp/gateway-native-preflight-");
  const marker = join(directory, "invoked");
  const sentinel = join(directory, "docker");
  let calls = 0;
  const neighbor = createServer((socket) => {
    calls++;
    socket.destroy();
  });
  await new Promise<void>((resolve, reject) => {
    neighbor.once("error", reject);
    neighbor.listen(0, "127.0.0.1", resolve);
  });
  const address = neighbor.address();
  if (!address || typeof address === "string") throw new Error("Neighbor address missing");
  try {
    await writeFile(sentinel, `#!/bin/sh\nprintf invoked >> '${marker}'\nexit 97\n`, {
      mode: 0o700,
    });
    const child = Bun.spawn([process.execPath, "--no-env-file", script], {
      env: nativeChildEnvironment({
        PATH: directory,
        PG_TOOLS_DIRECTORY: "/missing-native-tools",
        DURABILITY_REDIS_SERVER: "/missing-native-redis",
        DATABASE_URL: `postgres://synthetic:synthetic@127.0.0.1:${address.port}/postgres`,
      }),
      stdout: "pipe",
      stderr: "pipe",
    });
    try {
      const code = await within(child.exited, 5000, "CLI preflight");
      const output = await within(new Response(child.stderr).text(), 1000, "CLI error reader");
      expect(code).not.toBe(0);
      expect(await Bun.file(marker).exists()).toBe(false);
      expect(output).toContain("ENOENT");
      expect(calls).toBe(0);
    } finally {
      if (child.exitCode === null) child.kill("SIGKILL");
      await within(child.exited, 2000, "CLI reap");
    }
  } finally {
    await within(
      new Promise<void>((resolve, reject) =>
        neighbor.close((error) => (error ? reject(error) : resolve())),
      ),
      1000,
      "Neighbor close",
    );
    await rm(directory, { recursive: true });
  }
});

interface ProbeProcess {
  pid: number;
  parent: number;
  group: number;
  command: string;
}

async function probeProcesses(): Promise<ProbeProcess[]> {
  return new Promise((resolve, reject) => {
    execFile(
      "/bin/ps",
      ["-axo", "pid=,ppid=,pgid=,command="],
      { env: nativeChildEnvironment(), timeout: 1000, maxBuffer: 1048576 },
      (error, output) => {
        if (error) return reject(error);
        resolve(
          output.split("\n").flatMap((line) => {
            const match = /^\s*(\d+)\s+(\d+)\s+(\d+)\s+(.+)$/.exec(line);
            return match
              ? [
                  {
                    pid: Number(match[1]),
                    parent: Number(match[2]),
                    group: Number(match[3]),
                    command: match[4]!,
                  },
                ]
              : [];
          }),
        );
      },
    );
  });
}

async function probeOutput(stream: ReadableStream<Uint8Array>) {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let text = "";
  let bytes = 0;
  try {
    while (true) {
      const result = await reader.read();
      if (result.done) return { text: text + decoder.decode(), bytes };
      if (bytes < 16384)
        text += decoder.decode(result.value.subarray(0, 16384 - bytes), { stream: true });
      bytes += result.value.length;
    }
  } finally {
    reader.releaseLock();
  }
}

async function businessPreload(directory: string, evaluation: string, temporaryRoot?: string) {
  const preload = join(directory, "business-preload.ts");
  const contents = `${evaluation}export class UsageProducer {}export class UsageConsumer {}`;
  const fixtureLoader = temporaryRoot
    ? `builder.onLoad({filter:new RegExp("/tests/support/native-fixture[.]ts$")},async(args)=>{const source=await Bun.file(args.path).text();const target='const temp = await realpath("/tmp");';if(source.split(target).length!==2)throw new Error("Synthetic fixture root target missing");return {contents:source.replace(target,${JSON.stringify(`const temp = ${JSON.stringify(temporaryRoot)};`)}),loader:"ts"};});`
    : "";
  await writeFile(
    preload,
    `console.error("SYNTHETIC_LOADER_READY");Bun.plugin({name:"durability-business-boundary",setup(builder){builder.onLoad({filter:new RegExp("/packages/runtime/src/index[.]ts$")},()=>({contents:${JSON.stringify(contents)},loader:"ts"}));${fixtureLoader}}});`,
    { mode: 0o600 },
  );
  return preload;
}

async function interruptPreflight(blockBusinessDependencies = false) {
  const directory = await mkdtemp("/tmp/gateway-native-probe-");
  const executable = join(directory, "initdb");
  const source = "#!/bin/sh\n/bin/sleep 30\nexit 0\n";
  const started = Date.now();
  const diagnostic: Record<string, unknown> = { directory, executable, source, started };
  let pid = 0;
  let failure: unknown;
  let child: ReturnType<typeof Bun.spawn<"ignore", "pipe", "pipe">> | undefined;
  let output: ReturnType<typeof probeOutput> | undefined;
  let errors: ReturnType<typeof probeOutput> | undefined;
  try {
    await writeFile(executable, source, { mode: 0o700 });
    const resolved = await realpath(executable);
    Object.assign(diagnostic, {
      resolved,
      mode: (await lstat(executable)).mode & 0o777,
      sha256: new Bun.CryptoHasher("sha256").update(await readFile(executable)).digest("hex"),
      filesystem: await statfs(directory),
    });
    const preload = blockBusinessDependencies
      ? await businessPreload(
          directory,
          'console.error("SYNTHETIC_BUSINESS_EVALUATION",JSON.stringify({term:process.listenerCount("SIGTERM"),int:process.listenerCount("SIGINT")}));await Bun.sleep(10000);',
        )
      : undefined;
    child = Bun.spawn(
      [process.execPath, "--no-env-file", ...(preload ? ["--preload", preload] : []), script],
      {
        env: nativeChildEnvironment({
          PG_TOOLS_DIRECTORY: directory,
          DURABILITY_REDIS_SERVER: nativeFixtureOptions().redisServer,
        }),
        stdin: "ignore",
        stdout: "pipe",
        stderr: "pipe",
      },
    );
    diagnostic.driverPid = child.pid;
    const exited = child.exited.then((code) => {
      diagnostic.exit = { code, at: Date.now() - started };
      return code;
    });
    output = probeOutput(child.stdout);
    errors = probeOutput(child.stderr);
    const command = `/bin/sh ${resolved} --version`;
    const end = Date.now() + 3000;
    while (!pid) {
      const table = await probeProcesses();
      const owned = table.filter((entry) => entry.parent === child!.pid);
      diagnostic.children = owned;
      const probe = owned.find((entry) => entry.group === entry.pid && entry.command === command);
      if (child.exitCode !== null)
        throw new Error("Native driver exited before a live version probe");
      if (probe) {
        pid = probe.pid;
        diagnostic.probe = { ...probe, at: Date.now() - started };
        break;
      }
      if (Date.now() >= end)
        throw new Error("Live owned version probe did not start within 3000 ms");
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([
          exited,
          new Promise<void>((resolve) => {
            timer = setTimeout(resolve, 10);
          }),
        ]);
      } finally {
        clearTimeout(timer);
      }
    }
    const live = (await probeProcesses()).find((entry) => entry.pid === pid);
    if (
      child.exitCode !== null ||
      live?.parent !== child.pid ||
      live.group !== pid ||
      live.command !== command
    )
      throw new Error("Owned version probe stopped before TERM injection");
    diagnostic.signal = { value: "SIGTERM", at: Date.now() - started, probe: live };
    child.kill("SIGTERM");
    expect(await within(exited, 6000, "Interrupted preflight")).not.toBe(0);
    const remaining = (await probeProcesses()).filter((entry) => entry.group === pid);
    diagnostic.remainingAfterDriverExit = remaining;
    expect(exists(pid)).toBe(false);
    expect(remaining).toEqual([]);
  } catch (error) {
    failure = error;
  }
  try {
    if (child) {
      if (child.exitCode === null) {
        child.kill("SIGTERM");
        try {
          await within(child.exited, 3000, "Preflight graceful reap");
        } catch {
          child.kill("SIGKILL");
          diagnostic.forcedDriverKill = true;
        }
      }
      await within(child.exited, 2000, "Preflight reap");
      const readers = await within(Promise.allSettled([output, errors]), 1000, "Preflight readers");
      diagnostic.stdout = readers[0];
      diagnostic.stderr = readers[1];
      if (blockBusinessDependencies && !failure && readers[1].status === "fulfilled")
        expect(readers[1].value?.text).not.toContain("SYNTHETIC_BUSINESS_EVALUATION");
      const rejected = readers.flatMap((result) =>
        result.status === "rejected" ? [result.reason] : [],
      );
      if (rejected.length) throw new AggregateError(rejected, "Preflight readers failed");
      diagnostic.childrenAfterReap = (await probeProcesses()).filter(
        (entry) => entry.parent === child!.pid || (pid && entry.group === pid),
      );
    }
  } catch (error) {
    failure = failure
      ? new AggregateError([failure, error], "Preflight diagnostics failed")
      : error;
  }
  try {
    if (pid && (await probeProcesses()).some((entry) => entry.group === pid)) {
      try {
        process.kill(-pid, "SIGKILL");
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
      }
      await reapProbe(pid);
      if ((await probeProcesses()).some((entry) => entry.group === pid))
        throw new Error("Owned version probe group survived fallback cleanup");
    }
    diagnostic.groupAfterFallback = pid
      ? (await probeProcesses()).filter((entry) => entry.group === pid)
      : [];
    await rm(directory, { recursive: true });
    diagnostic.directoryRemoved = true;
  } catch (error) {
    failure = failure ? new AggregateError([failure, error], "Preflight cleanup failed") : error;
  }
  if (failure) {
    diagnostic.elapsed = Date.now() - started;
    throw new Error(`Native preflight signal regression: ${JSON.stringify(diagnostic)}`, {
      cause: failure,
    });
  }
}

test("TERM preflight diagnoses early driver exit and reaps the confirmed live probe group", async () => {
  await interruptPreflight();
});

test("slow business dependency evaluation cannot prevent native preflight or probe TERM cleanup", async () => {
  await interruptPreflight(true);
});

async function ownedFixtureDirectory(value: unknown) {
  if (typeof value !== "string" || !isAbsolute(value)) return;
  const name = basename(value);
  const parent = dirname(value);
  if (!/^gateway-native-[A-Za-z0-9_-]+$/.test(name) || value !== join(parent, name)) return;
  const root = await realpath("/tmp");
  if ((parent !== root && parent !== "/tmp") || (await realpath(parent)) !== root) return;
  const directory = join(root, name);
  try {
    if (!(await lstat(directory)).isDirectory()) return;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  return directory;
}

async function businessLoadFailure(interrupt: boolean, temporaryRoot?: string) {
  const directory = await mkdtemp("/tmp/gateway-native-load-");
  const marker = join(directory, "business-evaluating");
  let child: ReturnType<typeof Bun.spawn<"ignore", "pipe", "pipe">> | undefined;
  let output: ReturnType<typeof probeOutput> | undefined;
  let errors: ReturnType<typeof probeOutput> | undefined;
  let fixtureDirectory: string | undefined;
  let failure: unknown;
  const diagnostic: Record<string, unknown> = { directory };
  try {
    for (const [name, version] of [
      ["initdb", "initdb (PostgreSQL) 17.9"],
      ["postgres", "postgres (PostgreSQL) 17.9"],
      ["redis-server", "Redis server v=7.4.12"],
    ])
      await writeFile(
        join(directory, name!),
        `#!/bin/sh\nif [ "$1" = "--version" ]; then\n  printf '%s\\n' '${version}'\n  exit 0\nfi\nprintf SYNTHETIC_SERVICE_START >&2\nexit 91\n`,
        { mode: 0o700 },
      );
    const preload = await businessPreload(
      directory,
      `import {writeFileSync} from "node:fs";writeFileSync(${JSON.stringify(marker)},"evaluating");${interrupt ? "await Bun.sleep(10000);" : ""}throw new Error("SYNTHETIC_BUSINESS_LOAD_FAILED");`,
      temporaryRoot,
    );
    child = Bun.spawn([process.execPath, "--no-env-file", "--preload", preload, script], {
      env: nativeChildEnvironment({
        PG_TOOLS_DIRECTORY: directory,
        DURABILITY_REDIS_SERVER: join(directory, "redis-server"),
      }),
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
    });
    output = probeOutput(child.stdout);
    errors = probeOutput(child.stderr);
    if (interrupt) {
      const end = Date.now() + 3000;
      while (!(await Bun.file(marker).exists())) {
        if (child.exitCode !== null)
          throw new Error("Driver exited before business dependency evaluation");
        if (Date.now() >= end) throw new Error("Business dependency evaluation timed out");
        await Bun.sleep(10);
      }
      child.kill("SIGTERM");
    }
    expect(await within(child.exited, 6000, "Business dependency failure")).not.toBe(0);
    const [stdout, stderr] = await within(Promise.all([output, errors]), 1000, "Business readers");
    expect(stdout.text + stderr.text).not.toContain("SYNTHETIC_SERVICE_START");
    const events = [...stdout.text.split("\n"), ...stderr.text.split("\n")].flatMap((line) => {
      try {
        return [JSON.parse(line) as Record<string, unknown>];
      } catch {
        return [];
      }
    });
    fixtureDirectory = events.find((event) => event.durabilityFixture === "started")?.directory as
      string | undefined;
    expect(await ownedFixtureDirectory(fixtureDirectory)).toBeDefined();
    expect(events.find((event) => event.durability === "fail")?.ownedResources).toEqual({
      processes: [],
      directories: [],
      ports: [],
      timers: 0,
    });
    expect(stderr.text).toContain(
      interrupt ? "Durability interrupted" : "SYNTHETIC_BUSINESS_LOAD_FAILED",
    );
    expect(exists(child.pid)).toBe(false);
    expect((await probeProcesses()).filter((entry) => entry.parent === child!.pid)).toEqual([]);
    await expect(lstat(fixtureDirectory!)).rejects.toThrow("ENOENT");
  } catch (error) {
    failure = error;
  }
  try {
    if (child) {
      diagnostic.driverPid = child.pid;
      if (child.exitCode === null) {
        child.kill("SIGKILL");
        diagnostic.forcedDriverKill = true;
      }
      diagnostic.exitCode = await within(child.exited, 2000, "Business driver reap");
      const readers = await within(
        Promise.allSettled([output, errors]),
        1000,
        "Business reader cleanup",
      );
      diagnostic.stdout = readers[0];
      diagnostic.stderr = readers[1];
    }
    const ownedDirectory = await ownedFixtureDirectory(fixtureDirectory);
    diagnostic.fixtureDirectory = fixtureDirectory;
    diagnostic.ownedDirectory = ownedDirectory;
    if (ownedDirectory) await rm(ownedDirectory, { recursive: true, force: true });
  } catch (error) {
    failure = failure ? new AggregateError([failure, error], "Business cleanup failed") : error;
  } finally {
    await rm(directory, { recursive: true });
  }
  if (failure)
    throw new Error(`Native business load regression: ${JSON.stringify(diagnostic)}`, {
      cause: failure,
    });
}

test("business dependency load failure closes the preflight fixture before any service starts", async () => {
  await businessLoadFailure(false);
});

test("TERM during business dependency loading closes the fixture and prevents later services", async () => {
  await businessLoadFailure(true);
});

test("business load failure accepts a physical temporary root alias after the CLI removes its directory", async () => {
  await businessLoadFailure(false, "/tmp");
});

test("native preflight rejects relative executables and the obsolete native argument", async () => {
  await expect(
    createNativeFixture({ ...nativeFixtureOptions(), redisServer: "redis-server" }),
  ).rejects.toThrow("absolute");
  await expect(
    createNativeFixture({ ...nativeFixtureOptions(), stopTimeoutMs: Infinity }),
  ).rejects.toThrow("finite");
  const child = Bun.spawn([process.execPath, "--no-env-file", script, "--native"], {
    env: nativeChildEnvironment(),
    stdout: "pipe",
    stderr: "pipe",
  });
  expect(await within(child.exited, 3000, "Obsolete argument")).not.toBe(0);
  expect(await new Response(child.stderr).text()).toContain("accepts no arguments");
});

test("native PostgreSQL uses a private SCRAM cluster with durable settings and rejects a wrong password", async () => {
  const fixture = await createNativeFixture();
  let db: SQL | undefined;
  let rejected: SQL | undefined;
  try {
    const pg = await fixture.startPostgres();
    db = new SQL(pg.url, { max: 1 });
    const row = (
      await db`SELECT current_setting('fsync') AS fsync, current_setting('synchronous_commit') AS commit, current_setting('full_page_writes') AS pages, current_setting('data_directory') AS directory`
    )[0];
    expect(row).toEqual({ fsync: "on", commit: "on", pages: "on", directory: pg.directory });
    expect((await stat(fixture.directory)).mode & 0o777).toBe(0o700);
    expect((await stat(join(pg.directory, "postgresql.conf"))).mode & 0o777).toBe(0o600);
    expect(await readFile(join(pg.directory, "pg_hba.conf"), "utf8")).not.toMatch(
      /\s+trust\s*(?:\n|$)/,
    );
    const wrong = new URL(pg.url);
    wrong.password = "wrong-synthetic-password";
    rejected = new SQL(wrong.toString(), { max: 1, connectionTimeout: 1 });
    await expect(rejected`SELECT 1`).rejects.toThrow("password authentication failed");
  } finally {
    await Promise.allSettled([db?.close({ timeout: 1 }), rejected?.close({ timeout: 1 })]);
    await empty(fixture);
  }
});

test("native child HTTP readiness excludes ambient secrets and automatic dotenv loading", async () => {
  const fixture = await createNativeFixture();
  const previous = process.env.T14_AMBIENT_SECRET;
  process.env.T14_AMBIENT_SECRET = "synthetic-parent-only";
  try {
    const file = await fixture.write(
      "environment.ts",
      `const server = Bun.serve({hostname:"127.0.0.1",port:0,fetch:()=>Response.json({model:process.env.MINIMAX_API_KEY,ambient:process.env.T14_AMBIENT_SECRET??null,database:process.env.DATABASE_URL??null,explicit:process.env.T14_EXPLICIT})});console.log(server.port);`,
    );
    const child = await fixture.startBun("environment", file, { T14_EXPLICIT: "public-synthetic" });
    await fixture.waitFor(async () => /^\d+\s*$/.test(child.output));
    const response = await fetch(`http://127.0.0.1:${child.output.trim()}`, {
      signal: AbortSignal.timeout(1000),
    });
    expect(await response.json()).toEqual({
      model: "",
      ambient: null,
      database: null,
      explicit: "public-synthetic",
    });
  } finally {
    if (previous === undefined) delete process.env.T14_AMBIENT_SECRET;
    else process.env.T14_AMBIENT_SECRET = previous;
    await empty(fixture);
  }
});

test("not-ready children that ignore TERM and their descendants are reaped within a finite deadline", async () => {
  const fixture = await createNativeFixture({ ...nativeFixtureOptions(), stopTimeoutMs: 100 });
  let descendant = 0;
  try {
    const file = await fixture.write(
      "stubborn.ts",
      `const child = Bun.spawn([process.execPath,"--no-env-file","-e",'process.on("SIGTERM",()=>{});setInterval(()=>{},1000)'],{stdout:"ignore",stderr:"ignore"});process.on("SIGTERM",()=>{});console.log(child.pid);setInterval(()=>{},1000);`,
    );
    const child = await fixture.startBun("stubborn", file);
    await fixture.waitFor(async () => /^\d+\s*$/.test(child.output));
    descendant = Number(child.output.trim());
    expect(exists(descendant)).toBe(true);
    await expect(fixture.waitFor(async () => false, 100)).rejects.toThrow("timed out");
    const before = Date.now();
    await empty(fixture);
    expect(Date.now() - before).toBeLessThan(6000);
    expect(exists(child.pid)).toBe(false);
    expect(exists(descendant)).toBe(false);
  } finally {
    await fixture.close();
  }
});

test("a child bind failure and partial native start preserve a foreign neighbor listener and directory", async () => {
  const fixture = await createNativeFixture();
  const neighborDirectory = await mkdtemp("/tmp/gateway-native-neighbor-");
  const file = join(neighborDirectory, "keep");
  const neighbor = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: () => new Response("foreign-neighbor"),
  });
  try {
    await writeFile(file, "neighbor-content", { mode: 0o600 });
    const redis = await fixture.startRedis("primary");
    expect((await stat(join(fixture.directory, "primary.conf"))).mode & 0o777).toBe(0o600);
    for (const file of await readdir(join(redis.directory, "appendonlydir")))
      expect((await stat(join(redis.directory, "appendonlydir", file))).mode & 0o777).toBe(0o600);
    const program = await fixture.write(
      "collision.ts",
      `Bun.serve({hostname:"127.0.0.1",port:${neighbor.port},fetch:()=>new Response("bad")});`,
    );
    const child = await fixture.startBun("collision", program);
    expect(await within(child.exited, 3000, "Failed bind")).not.toBe(0);
    await expect(fixture.startRedis("primary")).rejects.toThrow();
    await empty(fixture);
    expect(exists(redis.pid)).toBe(false);
    expect(await readFile(file, "utf8")).toBe("neighbor-content");
    expect(
      await (
        await fetch(`http://127.0.0.1:${neighbor.port}`, { signal: AbortSignal.timeout(1000) })
      ).text(),
    ).toBe("foreign-neighbor");
  } finally {
    await fixture.close();
    await neighbor.stop(true);
    await rm(neighborDirectory, { recursive: true });
  }
});

test("excess child output fails readiness and releases the other independently owned native service", async () => {
  const fixture = await createNativeFixture();
  try {
    const redis = await fixture.startRedis("primary");
    const file = await fixture.write(
      "noisy.ts",
      `setInterval(()=>process.stdout.write("x".repeat(32768)),1);`,
    );
    const child = await fixture.startBun("noisy", file);
    await expect(fixture.waitFor(async () => false, 3000)).rejects.toThrow("output exceeded");
    await empty(fixture);
    expect(child.output.length).toBeLessThanOrEqual(262144);
    expect(exists(redis.pid)).toBe(false);
  } finally {
    await fixture.close();
  }
});

test("expected PostgreSQL query errors do not echo statement payloads or exhaust native fixture output", async () => {
  const fixture = await createNativeFixture();
  let db: SQL | undefined;
  try {
    const postgres = await fixture.startPostgres();
    db = new SQL(postgres.url, { max: 1 });
    const payload = "PRIVATE_QUERY_PAYLOAD_" + "x".repeat(180000);
    for (let n = 0; n < 4; n++) {
      await expect(db.unsafe(`SELECT 1/0, '${payload}'`)).rejects.toThrow("division by zero");
      await Bun.sleep(25);
      expect(postgres.output).not.toContain("PRIVATE_QUERY_PAYLOAD_");
      fixture.assertHealthy();
    }
    expect((await db`SELECT 42 AS answer`)[0].answer).toBe(42);
  } finally {
    await db?.close({ timeout: 1 });
    await empty(fixture);
  }
});

test("transient file removal during native disk inspection does not interrupt an owned service", async () => {
  const fixture = await createNativeFixture();
  try {
    const file = await fixture.write(
      "churn.ts",
      `import {writeFile,unlink} from "node:fs/promises";import {join} from "node:path";const root=process.env.T14_DIRECTORY;const names=Array.from({length:400},(_,i)=>join(root,"churn-"+i));const end=Date.now()+1800;while(Date.now()<end){await Promise.all(names.map(n=>writeFile(n,"x")));await Promise.all(names.map(n=>unlink(n)));}console.log("churn complete");`,
    );
    const child = await fixture.startBun("file-churn", file, { T14_DIRECTORY: fixture.directory });
    await fixture.waitFor(async () => child.output.includes("churn complete"), 5000);
    fixture.assertHealthy();
    expect(await within(child.exited, 1000, "Churn exit")).toBe(0);
  } finally {
    await empty(fixture);
  }
});

test("unexpected symlink disk inspection failures still stop owned services and preserve their neighbor target", async () => {
  const fixture = await createNativeFixture();
  const directory = await mkdtemp("/tmp/gateway-native-symlink-neighbor-");
  const target = join(directory, "keep");
  try {
    await writeFile(target, "foreign target", { mode: 0o600 });
    const redis = await fixture.startRedis("primary");
    const file = await fixture.write(
      "symlink.ts",
      `import {symlink} from "node:fs/promises";await symlink(process.env.T14_TARGET,process.env.T14_LINK);setInterval(()=>{},1000);`,
    );
    await fixture.startBun("symlink", file, {
      T14_TARGET: target,
      T14_LINK: join(fixture.directory, "unexpected-link"),
    });
    await expect(fixture.waitFor(async () => false, 3000)).rejects.toThrow(
      "disk inspection failed",
    );
    expect(() => fixture.assertHealthy()).toThrow("disk inspection failed");
    await empty(fixture);
    expect(exists(redis.pid)).toBe(false);
    expect(await readFile(target, "utf8")).toBe("foreign target");
  } finally {
    await fixture.close();
    await rm(directory, { recursive: true });
  }
});

for (const signal of ["SIGTERM", "SIGINT"] as const) {
  test(`durability ${signal} during initialized native services fails visibly after owned resources reach zero`, async () => {
    const options = nativeFixtureOptions();
    const child = Bun.spawn([process.execPath, "--no-env-file", script], {
      env: nativeChildEnvironment({
        PG_TOOLS_DIRECTORY: options.postgresToolsDirectory,
        DURABILITY_REDIS_SERVER: options.redisServer,
      }),
      stdout: "pipe",
      stderr: "pipe",
    });
    let output = "";
    const reader = child.stdout.getReader();
    const reading = (async () => {
      while (true) {
        const next = await reader.read();
        if (next.done) break;
        output += new TextDecoder().decode(next.value);
        if (output.length > 131072) throw new Error("CLI output limit");
      }
    })();
    const errors = new Response(child.stderr).text();
    try {
      const end = Date.now() + 15000;
      while (!output.includes('"durabilityFixture":"ready"')) {
        if (child.exitCode !== null || Date.now() > end)
          throw new Error("CLI did not initialize native services");
        await Bun.sleep(20);
      }
      const directory = (JSON.parse(output.split("\n")[0]!) as { directory: string }).directory;
      child.kill(signal);
      expect(await within(child.exited, 10000, "Interrupted durability")).not.toBe(0);
      await within(reading, 1000, "Interrupted output reader");
      const stderr = await within(errors, 1000, "Interrupted error reader");
      expect(stderr).toContain(
        '"ownedResources":{"processes":[],"directories":[],"ports":[],"timers":0}',
      );
      expect(await Bun.file(directory).exists()).toBe(false);
    } finally {
      if (child.exitCode === null) child.kill("SIGKILL");
      await within(child.exited, 2000, "Interrupted child reap");
      await within(Promise.allSettled([reading, errors]), 1000, "Interrupted readers");
      reader.releaseLock();
    }
  }, 30000);
}
