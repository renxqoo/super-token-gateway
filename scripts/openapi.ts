import { openapi } from "../apps/gateway/src/openapi.ts";
await Bun.write(
  new URL("../docs/openapi.json", import.meta.url),
  JSON.stringify(openapi(), null, 2) + "\n",
);
const formatted = Bun.spawnSync(
  ["bun", "fmt", "--no-script", new URL("../docs/openapi.json", import.meta.url).pathname],
  { stdout: "ignore", stderr: "pipe" },
);
if (formatted.exitCode !== 0) throw new Error("OpenAPI formatter failed");
