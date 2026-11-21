import { mkdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const go = process.env.PLUGIN_GO ?? "go";
const args = process.argv.slice(2);
const action = args[0];
if (!["build", "test", "bench"].includes(action ?? "")) throw new Error("V3_ENGINE_COMMAND");
const env = {
  PATH: process.env.PATH ?? "/usr/bin:/bin",
  TMPDIR: process.env.TMPDIR ?? "/tmp",
  GOENV: "off",
  GOTOOLCHAIN: "local",
  GOCACHE: `${root}.local/go-cache`,
  GOMODCACHE: process.env.PLUGIN_GOMODCACHE ?? `${root}.local/go-mod`,
  GOPROXY: "off",
  GOSUMDB: "off",
};
await mkdir(`${root}.local`, { recursive: true });
const dir = `${root}apps/v3-engine`;
if (action === "build") {
  for (const [bin, pkg] of [
    ["v3-engine", "./cmd/v3-engine"],
    ["v3-bench", "./cmd/v3-bench"],
  ] as const) {
    const child = Bun.spawn(
      [go, "-C", dir, "build", "-trimpath", "-buildvcs=false", "-o", `${root}.local/${bin}`, pkg],
      { env, stdout: "inherit", stderr: "inherit" },
    );
    if ((await child.exited) !== 0) throw new Error(`V3_ENGINE_BUILD_${bin}`);
  }
} else if (action === "test") {
  const race = args.includes("--race") ? ["-race"] : [];
  for (const cmd of [
    ["vet", "./...", "./__test__"],
    ["test", ...race, "./__test__", "-count=1"],
  ]) {
    const child = Bun.spawn([go, "-C", dir, ...cmd], {
      env,
      stdout: "inherit",
      stderr: "inherit",
    });
    if ((await child.exited) !== 0) throw new Error("V3_ENGINE_TEST");
  }
} else {
  const child = Bun.spawn([`${root}.local/v3-bench`, ...args.slice(1)], {
    env,
    stdout: "inherit",
    stderr: "inherit",
  });
  if ((await child.exited) !== 0) throw new Error("V3_ENGINE_BENCH");
}
