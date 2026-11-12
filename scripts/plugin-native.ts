import { mkdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";
const root = fileURLToPath(new URL("../", import.meta.url));
const go = process.env.PLUGIN_GO ?? "go";
const args = process.argv.slice(2);
if (
  !["build", "test"].includes(args[0] ?? "") ||
  args.slice(1).some((arg) => arg !== "--race") ||
  (args[0] === "build" && args.length !== 1)
)
  throw new Error("PLUGIN_NATIVE_COMMAND");
const env = {
  PATH: process.env.PATH ?? "/usr/bin:/bin",
  TMPDIR: process.env.TMPDIR ?? "/tmp",
  GOENV: "off",
  GOTOOLCHAIN: "local",
  GOCACHE: `${root}.local/go-cache`,
  GOMODCACHE: process.env.PLUGIN_GOMODCACHE ?? `${root}.local/go-mod`,
  ...(args[0] === "test" ? { GOPROXY: "off", GOSUMDB: "off" } : {}),
};
const version = Bun.spawn([go, "version"], { env, stdout: "pipe", stderr: "pipe" });
if (
  !(await new Response(version.stdout).text()).includes("go1.27.4 ") ||
  (await version.exited) !== 0
)
  throw new Error("PLUGIN_GO_REQUIRES_1_27_4");
await mkdir(`${root}.local`, { recursive: true });
const commands =
  args[0] === "build"
    ? [
        [
          "build",
          "-trimpath",
          "-buildvcs=false",
          "-o",
          `${root}.local/plugin-host`,
          "./cmd/plugin-host",
        ],
      ]
    : [
        ["vet", "./...", "./__test__"],
        ["test", "-json", ...(args.includes("--race") ? ["-race"] : []), "./__test__", "-count=1"],
      ];
if (args[0] !== "build") {
  const goroot = Bun.spawn([go, "env", "GOROOT"], { env, stdout: "pipe", stderr: "pipe" });
  const toolroot = (await new Response(goroot.stdout).text()).trim();
  if ((await goroot.exited) !== 0 || !toolroot.startsWith("/")) throw new Error("PLUGIN_GO_ROOT");
  const files = await Array.fromAsync(
    new Bun.Glob("**/*.go").scan(`${root}packages/plugin-host/native`),
  );
  const source = Bun.spawn(
    [
      `${toolroot}/bin/gofmt`,
      "-l",
      ...files.map((path) => `${root}packages/plugin-host/native/${path}`),
    ],
    { env, stdout: "pipe", stderr: "pipe" },
  );
  const changed = await new Response(source.stdout).text();
  if ((await source.exited) !== 0 || changed.trim()) throw new Error("PLUGIN_GO_FORMAT");
}
for (const cmd of commands) {
  const child = Bun.spawn([go, "-C", `${root}packages/plugin-host/native`, ...cmd], {
    env,
    stdout: "inherit",
    stderr: "inherit",
  });
  if ((await child.exited) !== 0) throw new Error("PLUGIN_NATIVE_CHECK");
}
