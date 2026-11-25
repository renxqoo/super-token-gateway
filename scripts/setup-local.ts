import { localConsoleTokenEnvironment, writeLocalInternalAuth } from "./local-internal-auth.ts";
import { chmod, mkdir } from "node:fs/promises";
import { fileURLToPath } from "node:url";
if (Bun.version !== "1.4.5") throw new Error("Use Bun 1.4.5");
const file = new URL("../.env.local", import.meta.url);
const localDir = new URL("../.local/", import.meta.url);
const authFile = new URL("internal-auth.json", localDir);
await mkdir(localDir, { recursive: true });
const random = () => Buffer.from(crypto.getRandomValues(new Uint8Array(32))).toString("hex");
if (!(await Bun.file(file).exists())) {
  const password = random();
  const token = random();
  await writeLocalInternalAuth(authFile, token);
  await Bun.write(
    file,
    [
      `DATABASE_URL=postgres://gateway:${password}@127.0.0.1:55442/gateway_current`,
      "REDIS_URL=redis://127.0.0.1:56382",
      `PG_TOOLS_DIRECTORY=${process.env.PG_TOOLS_DIRECTORY ?? fileURLToPath(new URL("../.local/toolchain/pg17/bin", import.meta.url))}`,
      `DURABILITY_REDIS_SERVER=${process.env.DURABILITY_REDIS_SERVER ?? fileURLToPath(new URL("../.local/toolchain/redis74/bin/redis-server", import.meta.url))}`,
      "INTERNAL_AUTH_FILE=" + fileURLToPath(authFile),
      ...Object.entries(localConsoleTokenEnvironment(token)).map(
        ([name, value]) => name + "=" + value,
      ),
      `ENCRYPTION_KEY=${random()}`,
      `KEY_PEPPER=${random()}`,
      `MOCK_CONTROL_TOKEN=${random()}`,
      "NODE_ENV=development",
      "ALLOW_PRIVATE_UPSTREAMS=true",
      "PRIVATE_UPSTREAM_CIDRS=127.0.0.0/8",
      "GATEWAY_HOST=127.0.0.1",
      "GATEWAY_PORT=8088",
      "MOCK_PORT=9088",
      "",
    ].join("\n"),
  );
} else {
  const content = await Bun.file(file).text();
  const required = [
    "DATABASE_URL",
    "REDIS_URL",
    "INTERNAL_AUTH_FILE",
    "KEY_PEPPER",
    "ENCRYPTION_KEY",
    "MOCK_CONTROL_TOKEN",
    "PG_TOOLS_DIRECTORY",
    "DURABILITY_REDIS_SERVER",
  ];
  const missing = required.filter((key) => !new RegExp(`^${key}=`, "m").test(content));
  if (missing.length)
    throw new Error(
      `Current local configuration is incomplete: ${missing.join(", ")}. Configure the dedicated services before continuing.`,
    );
}
await chmod(file, 0o600);
console.log("Private local configuration is ready; existing credentials were preserved");
