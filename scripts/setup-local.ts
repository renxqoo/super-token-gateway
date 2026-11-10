import { chmod, mkdir } from "node:fs/promises";
if (Bun.version !== "1.4.4") throw new Error("Use Bun 1.4.4");
const file = new URL("../.env.local", import.meta.url);
const random = () => Buffer.from(crypto.getRandomValues(new Uint8Array(32))).toString("hex");
if (!(await Bun.file(file).exists())) {
  const password = random();
  await Bun.write(
    file,
    [
      `DATABASE_URL=postgres://gateway:${password}@127.0.0.1:55442/gateway_current`,
      "REDIS_URL=redis://127.0.0.1:56382",
      "QUEUE_REDIS_URL=redis://127.0.0.1:56383",
      "QUEUE_REQUIRED_REPLICAS=0",
      `POSTGRES_PASSWORD=${password}`,
      `ADMIN_KEY=${random()}`,
      `ENCRYPTION_KEY=${random()}`,
      `KEY_PEPPER=${random()}`,
      `MOCK_CONTROL_TOKEN=${random()}`,
      "NODE_ENV=development",
      "ALLOW_PRIVATE_UPSTREAM=true",
      "PORT=8088",
      "MOCK_PORT=9088",
      "",
    ].join("\n"),
  );
} else {
  const content = await Bun.file(file).text();
  const required = ["DATABASE_URL", "REDIS_URL", "QUEUE_REDIS_URL", "MOCK_CONTROL_TOKEN"];
  const missing = required.filter((key) => !new RegExp(`^${key}=`, "m").test(content));
  if (missing.length)
    throw new Error(
      `Current local configuration is incomplete: ${missing.join(", ")}. Configure the dedicated services before continuing.`,
    );
}
await chmod(file, 0o600);
await mkdir(new URL("../.local/", import.meta.url), { recursive: true });
console.log("Private local configuration is ready; existing credentials were preserved");
