import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import {
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  stat,
  symlink,
  unlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { publishPluginExecutable, resolvePluginExecutable } from "../executable.ts";
import { createPluginAttempts } from "../attempts.ts";
import { createPluginSupervisor } from "../supervisor.ts";
import { syntheticChatPackage } from "../../../../tests/support/plugin-package.ts";

test("helper publication captures one immutable binary even when the mutable source is replaced", async () => {
  const directory = await mkdtemp(resolve(tmpdir(), "gateway-executable-"));
  try {
    const source = resolve(directory, "source");
    const original = Buffer.from("original-helper");
    await writeFile(source, original);
    const pinned = await publishPluginExecutable(source);
    expect(pinned.digest).toBe(createHash("sha256").update(original).digest("hex"));
    expect((await stat(pinned.executable)).mode & 0o222).toBe(0);
    await writeFile(source, "replacement-helper");
    expect(await readFile(pinned.executable)).toEqual(original);
    expect(resolvePluginExecutable(pinned.executable)).toEqual(pinned);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("concurrent helper publishers preserve one valid immutable file and no staging files", async () => {
  const directory = await mkdtemp(resolve(tmpdir(), "gateway-executable-concurrent-"));
  try {
    for (let round = 0; round < 32; round++) {
      const parent = resolve(directory, String(round)),
        source = resolve(parent, "source");
      await mkdir(parent);
      await writeFile(source, "shared-helper");
      const completed = await Promise.allSettled(
        Array.from({ length: 8 }, () => publishPluginExecutable(source)),
      );
      const failures = completed.filter((value) => value.status === "rejected");
      expect(failures).toEqual([]);
      const values = completed.map((value) => {
        if (value.status === "rejected") throw value.reason;
        return value.value;
      });
      expect(new Set(values.map((value) => value.executable)).size).toBe(1);
      expect(await readFile(values[0]!.executable, "utf8")).toBe("shared-helper");
      expect(await readdir(dirname(values[0]!.executable))).toEqual(["plugin-host"]);
      expect(resolvePluginExecutable(source)).toEqual(values[0]!);
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("a declared immutable helper path cannot be reinterpreted as a different executable after corruption", async () => {
  const directory = await mkdtemp(resolve(tmpdir(), "gateway-executable-corrupt-"));
  try {
    const source = resolve(directory, "source");
    await writeFile(source, "original-helper");
    const pinned = await publishPluginExecutable(source);
    await chmod(pinned.executable, 0o755);
    await writeFile(pinned.executable, "replacement-helper");
    await chmod(pinned.executable, 0o555);
    const error = await publishPluginExecutable(pinned.executable).catch(
      (failure: unknown) => failure,
    );
    expect(error).toBeInstanceOf(Error);
    expect(() => resolvePluginExecutable(pinned.executable)).toThrow("PLUGIN_EXECUTABLE_CHANGED");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("a declared immutable helper replaced by a symlink cannot publish its target under a new identity", async () => {
  const directory = await mkdtemp(resolve(tmpdir(), "gateway-executable-symlink-"));
  try {
    const source = resolve(directory, "source");
    await writeFile(source, "original-helper");
    const pinned = await publishPluginExecutable(source);
    await writeFile(source, "replacement-helper");
    await unlink(pinned.executable);
    await symlink(source, pinned.executable);
    const publication = await publishPluginExecutable(pinned.executable).catch(
      (error: unknown) => error,
    );
    expect(publication).toBeInstanceOf(Error);
    expect(publication).toMatchObject({ message: "PLUGIN_EXECUTABLE_CHANGED" });
    expect(() => resolvePluginExecutable(pinned.executable)).toThrow("PLUGIN_EXECUTABLE_CHANGED");
    expect(await readdir(dirname(dirname(pinned.executable)))).toEqual([pinned.digest]);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("helper restart and new module workers execute the pinned binary after its mutable build source is replaced", async () => {
  const directory = await mkdtemp(resolve(tmpdir(), "gateway-executable-restart-"));
  const source = resolve(directory, "source");
  await writeFile(source, await readFile(resolve(".local/plugin-host")));
  const pinned = await publishPluginExecutable(source);
  const supervisor = createPluginSupervisor({
    executable: pinned.executable,
    expectedExecutableDigest: pinned.digest,
  });
  const host = createPluginAttempts(supervisor);
  try {
    const { pkg } = await syntheticChatPackage();
    const first = await supervisor.load(pkg.module);
    await writeFile(source, "not-a-helper");
    const disconnected = Promise.withResolvers<void>();
    const unsubscribe = supervisor.onDisconnect(() => disconnected.resolve());
    const timer = setTimeout(
      () => disconnected.reject(new Error("OWNED_HELPER_DID_NOT_EXIT")),
      2000,
    );
    try {
      process.kill(supervisor.snapshot().pid!, "SIGKILL");
      await disconnected.promise;
    } finally {
      clearTimeout(timer);
      unsubscribe();
    }
    const next = await supervisor.load(pkg.module);
    expect(next.connection).not.toBe(first.connection);
    expect(next.hash).toBe(first.hash);
    const attempt = host.create(pkg, {
      tenant: "pinned-runtime",
      attempt: crypto.randomUUID(),
      incarnation: crypto.randomUUID(),
      deadlineAt: Date.now() + 10000,
      config: {
        model_map: { logical: "upstream" },
        max_output_tokens: 100,
        max_input_tokens: 10000,
        auth: "bearer",
      },
    });
    try {
      expect(await attempt.invoke("describe")).toMatchObject({
        kind: "description",
        capabilities: { operations: expect.arrayContaining(["chat"]) },
      });
    } finally {
      attempt.dispose();
    }
  } finally {
    await host.close();
    await rm(directory, { recursive: true, force: true });
  }
  expect(supervisor.snapshot()).toMatchObject({ dead: true, pending: 0, timers: 0 });
});

test("a helper with an unexpected digest cannot start or consume the restart budget", async () => {
  const supervisor = createPluginSupervisor({
    executable: resolve(".local/plugin-host"),
    expectedExecutableDigest: "0".repeat(64),
  });
  try {
    const { pkg } = await syntheticChatPackage();
    const error = await supervisor.load(pkg.module).catch((failure: unknown) => failure);
    expect(error).toMatchObject({ code: "EXECUTABLE_CHANGED" });
    expect(supervisor.snapshot()).toMatchObject({ starts: 0, pending: 0, timers: 0 });
  } finally {
    await supervisor.close();
  }
});

test("a helper FIFO cannot block synchronous executable resolution or publication", async () => {
  const directory = await mkdtemp(resolve(tmpdir(), "gateway-executable-fifo-"));
  let child: Bun.Subprocess | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const path = resolve(directory, "helper");
    execFileSync("mkfifo", [path]);
    const script = `import {resolvePluginExecutable,publishPluginExecutable} from ${JSON.stringify(resolve("packages/plugin-host/src/executable.ts"))}; for(const call of [()=>resolvePluginExecutable(process.env.OWNED_FIFO_PATH),()=>publishPluginExecutable(process.env.OWNED_FIFO_PATH)]) { try { await call(); process.exit(2); } catch(error) { if(error.message !== 'PLUGIN_EXECUTABLE_INVALID') throw error; } }`;
    const running = Bun.spawn([process.execPath, "-e", script], {
      env: { ...process.env, OWNED_FIFO_PATH: path },
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
    });
    child = running;
    timer = setTimeout(() => child!.kill("SIGKILL"), 500);
    const [code, stderr] = await Promise.all([running.exited, new Response(running.stderr).text()]);
    expect({ code, stderr }).toEqual({ code: 0, stderr: "" });
  } finally {
    clearTimeout(timer);
    if (child && child.exitCode === null) child.kill("SIGKILL");
    await child?.exited;
    await rm(directory, { recursive: true, force: true });
  }
});
