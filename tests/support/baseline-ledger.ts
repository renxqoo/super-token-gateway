import type { SQL } from "bun";
import { createCatalogStore, recordRoute } from "@gateway/storage";

export async function baselineChatFixture(db: SQL, project: string) {
  const key = crypto.randomUUID(),
    provider = crypto.randomUUID();
  await db`INSERT INTO api_keys(id,project_id,name,verifier,prefix,models,operations) VALUES(${key},${project},'fixture',${crypto.randomUUID()},'fixture',${[]},${["chat"]})`;
  const catalog = createCatalogStore(db);
  await catalog.createProvider(
    provider,
    {
      name: "fixture",
      kind: "openai",
      base_url: "http://fixture.invalid/v1",
      concurrency: 100,
      rpm: 1000,
    },
    "encrypted-fixture",
  );
  await catalog.createModel({ name: "fixture", operations: ["chat"], max_output_tokens: 100 });
  const route = await db.begin((tx) =>
    recordRoute(tx, "fixture", {
      provider_id: provider,
      upstream_model: "fixture",
      operations: ["chat"],
      priority: 1,
      weight: 1,
      pricing: { currency: "USD", rates: { requests: { numerator: "1", denominator: "1" } } },
      ceilings: { requests: "1" },
    }),
  );
  const [{ pricing_id }] = await db`SELECT pricing_id FROM routes WHERE id=${route}`;
  return { key, provider, route, routePricing: String(pricing_id) };
}

export async function baselineAdmission(
  db: SQL,
  input: {
    project: string;
    key: string;
    route: string;
    routePricing: string;
    provider: string;
    tariffPricing: string;
    digest: string | null;
    fingerprint: string;
  },
): Promise<{ request: string; attempt: string; created: boolean }> {
  const payload = {
    id: crypto.randomUUID(),
    key_id: input.key,
    project_id: input.project,
    operation: "chat",
    model: "fixture",
    digest: input.digest,
    fingerprint: input.fingerprint,
    route_id: input.route,
    cost_pricing_id: input.routePricing,
    quote: {
      currency: "USD",
      amount_atoms: "10",
      input_characters: "0",
      prices: { [input.provider]: input.tariffPricing },
    },
  };
  const [{ result }] = await db`SELECT admit_gateway_batch(${[payload]}::jsonb) AS result`;
  const admitted = result["0"] as {
    error?: string;
    created: boolean;
    record: { id: string };
    attempt_id: string;
  };
  if (admitted.error) throw new Error(`Baseline admission: ${admitted.error}`);
  return { request: admitted.record.id, attempt: admitted.attempt_id, created: admitted.created };
}

export async function baselineEvidence(
  db: SQL,
  attempt: string,
  outcome: "SUCCEEDED" | "UNKNOWN",
): Promise<void> {
  const event =
    outcome === "SUCCEEDED"
      ? {
          attemptId: attempt,
          source: "execution",
          state: "SUCCEEDED",
          finality: "FINAL",
          requestState: "SUCCEEDED",
          usage: { requests: "1" },
        }
      : {
          attemptId: attempt,
          source: "execution",
          state: "UNKNOWN",
          finality: "UNKNOWN",
          requestState: "UNKNOWN",
          usage: {},
        };
  const [{ result }] = await db`SELECT persist_usage_batch(${[event]}::jsonb) AS result`;
  const persisted = result["0"] as { status: string; error?: string };
  if (persisted.status === "ERROR") throw new Error(`Baseline evidence: ${persisted.error}`);
}
