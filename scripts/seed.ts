import { mkdir, chmod } from "node:fs/promises";
import { createDatabase, migrate, Store, FundsStore } from "@gateway/storage";
import { createKey, encrypt, operations, verifier } from "@gateway/kernel";
import { loadConfig } from "../apps/gateway/src/config.ts";

const config = loadConfig();
if (config.NODE_ENV === "production")
  throw new Error("The development mock seed is forbidden in production");
const db = createDatabase(config.DATABASE_URL);
const store = new Store(db);
try {
  await migrate(db);
  const existing = await db`SELECT id FROM projects WHERE external_ref='local-development'`;
  if (existing.length) {
    console.log("Development configuration already exists; credential is in .local/client.env");
  } else {
    const projectId = await store.createProject({
      name: "Local development",
      external_ref: "local-development",
      rpm: 1_000_000,
      tpm: 1_000_000_000,
      concurrency: 10000,
    });
    const key = createKey();
    await store.createCredential(
      projectId,
      { name: "local-client", models: [], operations: [...operations] },
      verifier(key, config.KEY_PEPPER),
      key.slice(0, 12),
    );
    const mockOrigin =
      process.env.MOCK_ORIGIN ?? `http://127.0.0.1:${process.env.MOCK_PORT ?? 9088}`;
    for (const kind of ["openai", "anthropic", "gemini"] as const) {
      const providerId = crypto.randomUUID();
      await store.createProvider(
        providerId,
        {
          name: `local-${kind}`,
          kind,
          base_url: `${mockOrigin}/${kind}/${kind === "gemini" ? "v1beta" : "v1"}`,
          concurrency: 10000,
          rpm: 1_000_000,
        },
        encrypt("local-mock-upstream-key", config.ENCRYPTION_KEY, providerId),
      );
      const name = kind === "openai" ? "demo" : `demo-${kind}`;
      const caps =
        kind === "openai"
          ? [...operations]
          : kind === "gemini"
            ? (["chat", "responses", "messages", "gemini", "videos"] as const)
            : (["chat", "responses", "messages", "gemini"] as const);
      await store.createModel({ name, operations: [...caps], max_output_tokens: 4096 });
      await store.createRoute(name, {
        provider_id: providerId,
        upstream_model: "demo",
        operations: [...caps],
        priority: 10,
        weight: 1,
        pricing: {
          currency: "USD",
          rates: {
            input_tokens: { numerator: "150000000000", denominator: "1" },
            output_tokens: { numerator: "600000000000", denominator: "1" },
            cached_input_tokens: { numerator: "75000000000", denominator: "1" },
          },
        },
      });
    }
    await mkdir(new URL("../.local/", import.meta.url), { recursive: true });
    const path = new URL("../.local/client.env", import.meta.url);
    await Bun.write(
      path,
      `GATEWAY_URL=http://127.0.0.1:${config.PORT}\nGATEWAY_API_KEY=${key}\nGATEWAY_PROJECT_ID=${projectId}\n`,
    );
    await chmod(path, 0o600);
    console.log(
      "Development model configuration created; private credential saved to .local/client.env",
    );
  }
  const alias = await db`SELECT name FROM models WHERE name='gpt-4o-mini'`;
  if (!alias.length) {
    const provider = await db`SELECT id FROM providers WHERE name='local-openai'`;
    if (provider[0]) {
      await store.createModel({
        name: "gpt-4o-mini",
        operations: ["chat"],
        max_output_tokens: 4096,
      });
      await store.createRoute("gpt-4o-mini", {
        provider_id: provider[0].id,
        upstream_model: "gpt-4o-mini",
        operations: ["chat"],
        priority: 10,
        weight: 1,
        pricing: {
          currency: "USD",
          rates: {
            input_tokens: { numerator: "150000000000", denominator: "1" },
            output_tokens: { numerator: "600000000000", denominator: "1" },
            cached_input_tokens: { numerator: "75000000000", denominator: "1" },
          },
        },
      });
    }
  }
  const funds = new FundsStore(db);
  const development = await db`SELECT id FROM projects WHERE external_ref='local-development'`;
  if (development[0])
    await funds.credit(development[0].id, {
      currency: "USD",
      external_ref: "local-mock-bootstrap-v1",
      amount_atoms: "1000000000000000000000000000000",
      kind: "CASH",
      priority: 100,
      models: [],
      operations: [],
    });
  const models = await db<
    {
      name: string;
      operations: (typeof operations)[number][];
      rates: Record<string, { numerator: string; denominator: string }>;
    }[]
  >`SELECT DISTINCT ON(m.name) m.name,m.operations,v.rates FROM models m JOIN routes r ON r.model_name=m.name JOIN cost_versions v ON v.id=r.pricing_id ORDER BY m.name,r.priority DESC`;
  for (const model of models)
    for (const operation of model.operations) {
      const exists =
        await db`SELECT id FROM billing_tariffs WHERE model=${model.name} AND operation=${operation} AND currency='USD' AND provider_id IS NULL`;
      if (exists.length) continue;
      if (
        [
          "chat",
          "responses",
          "messages",
          "gemini",
          "completions",
          "embeddings",
          "batches",
        ].includes(operation)
      ) {
        const rates =
          operation === "embeddings" ? { input_tokens: model.rates.input_tokens! } : model.rates;
        const ceilings = Object.fromEntries(
          Object.keys(rates).map((key) => [
            key,
            operation === "batches" ? "1000000000" : "1000000",
          ]),
        );
        await funds.setTariff({
          model: model.name,
          operation,
          pricing: { currency: "USD", rates },
          ceilings,
          basis: "PROVIDER_USAGE",
        });
      } else
        await funds.setTariff({
          model: model.name,
          operation,
          pricing: {
            currency: "USD",
            rates: { requests: { numerator: "1000000000000", denominator: "1" } },
          },
          ceilings: { requests: "1" },
          basis: "REQUEST_COMPLETION",
        });
    }
} finally {
  await db.close();
}
