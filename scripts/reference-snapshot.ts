import { mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";

const args = Bun.argv.slice(2);
const option = (name: string, fallback: string) => {
  const at = args.indexOf(name);
  return at < 0 ? fallback : (args[at + 1] ?? fallback);
};
async function git(root: string, ...args: string[]) {
  const process = Bun.spawn(["git", "-C", root, ...args], { stdout: "pipe", stderr: "pipe" });
  const [output, error, code] = await Promise.all([
    new Response(process.stdout).text(),
    new Response(process.stderr).text(),
    process.exited,
  ]);
  if (code !== 0) throw new Error(`Cannot inspect reference ${root}: ${error.slice(0, 300)}`);
  return output.trim();
}
const sources = [
  {
    name: "Tillgate",
    root: option("--tillgate", new URL("../../Tillgate/", import.meta.url).pathname),
    files: [
      "apps/gateway/src/http/contracts/inference-endpoints.ts",
      "packages/billing/src/domain/wallet/exposure.ts",
      "packages/billing/src/application/wallet/debit-floor.ts",
      "packages/billing/src/adapters/postgres/wallet-store.ts",
    ],
  },
  {
    name: "new-api",
    root: option("--new-api", new URL("../../new-api/", import.meta.url).pathname),
    files: ["router/relay-router.go", "relay/request_billing.go", "service/billing_session.go"],
  },
];
const references = [];
for (const source of sources) {
  const root = resolve(source.root);
  const files: Record<string, string> = {};
  for (const path of source.files) {
    const file = Bun.file(resolve(root, path));
    if (!(await file.exists()))
      throw new Error(`Reference evidence missing: ${source.name}/${path}`);
    files[path] = new Bun.CryptoHasher("sha256").update(await file.arrayBuffer()).digest("hex");
  }
  references.push({
    name: source.name,
    root,
    head: await git(root, "rev-parse", "HEAD"),
    status: await git(root, "status", "--porcelain=v1"),
    files,
  });
}
const output = option("--output", ".local/reference-snapshot.json");
await mkdir(dirname(output), { recursive: true });
await Bun.write(
  output,
  JSON.stringify(
    {
      observed_at: new Date().toISOString(),
      gateway_head: await git(process.cwd(), "rev-parse", "HEAD"),
      gateway_status: await git(process.cwd(), "status", "--porcelain=v1"),
      performance_comparison: "not_run",
      references,
    },
    null,
    2,
  ) + "\n",
);
console.log(`Recorded reference versions and source evidence in ${output}; no performance ranking`);
