import type { SQL } from "bun";
import { canonical, DomainError } from "@gateway/kernel";
import { catalogDigest } from "./catalog-epoch.ts";
import { createConfigReleaseStore } from "./config-releases.ts";

type Deployment = {
  id: string;
  channel_id: string;
  upstream_model: string;
  cost_version_id: string;
  priority: number;
  weight: number;
};

type Onboarding = {
  id: string;
  expected_epoch: number;
  model: {
    name: string;
    operations: readonly string[];
    routing_strategy: string;
    max_output_tokens: number;
  };
  billing_price_id: string;
  deployments: readonly Deployment[];
};

type ActivatedModel = {
  customer_price_id: string;
  model: Pick<Onboarding["model"], "operations" | "routing_strategy" | "max_output_tokens">;
  deployments: readonly Deployment[];
};

function conflict(message: string): never {
  throw new DomainError("model_onboarding_conflict", message);
}

function invalid(message: string): never {
  throw new DomainError("invalid_model_onboarding", message);
}

async function prepare(tx: SQL, input: Onboarding) {
  const [epoch] = await tx<{ version: number; lineage_token: string }[]>`
    SELECT version, lineage_token FROM public.config_epoch WHERE id = 1
  `;
  if (!epoch || Number(epoch.version) !== input.expected_epoch)
    conflict("Configuration epoch changed");
  const [model] = await tx`SELECT 1 FROM public.models WHERE name = ${input.model.name}`;
  if (model) conflict("Model already exists");

  const routeIds = input.deployments.map(({ id }) => id);
  const [existing] = await tx`
    SELECT 1 FROM public.routes WHERE id = ANY(${tx.array(routeIds, "UUID")}) LIMIT 1
  `;
  if (existing) conflict("Deployment identifier already exists");

  const channels = [...new Set(input.deployments.map(({ channel_id }) => channel_id))];
  const usableChannels = await tx<{ account_id: string }[]>`
    SELECT c.account_id FROM public.channels c
    JOIN public.plugins p ON p.id = c.provider_plugin_id
    WHERE c.id = ANY(${tx.array(channels, "UUID")}) AND c.enabled AND p.enabled AND p.capability = 'PROVIDER'
  `;
  if (usableChannels.length !== channels.length)
    invalid("Every channel must have an enabled provider");

  const accounts = [...new Set(usableChannels.map(({ account_id }) => account_id))];
  const usableAccounts = await tx<{ state: string }[]>`
    SELECT state FROM public.upstream_accounts
    WHERE id = ANY(${tx.array(accounts, "UUID")}) ORDER BY id FOR SHARE
  `;
  if (
    usableAccounts.length !== accounts.length ||
    usableAccounts.some(({ state }) => state !== "ACTIVE")
  ) {
    invalid("Every upstream account must be active");
  }

  const supplierIds = [...new Set(input.deployments.map(({ cost_version_id }) => cost_version_id))];
  const prices = await tx<{ id: string; side: string }[]>`
    SELECT id, side FROM public.price_versions
    WHERE id = ANY(${tx.array([input.billing_price_id, ...supplierIds], "UUID")})
  `;
  const customer = prices.find(
    ({ id, side }) => id === input.billing_price_id && side === "CUSTOMER",
  );
  if (!customer) invalid("An existing customer price version is required");
  if (prices.filter(({ side }) => side === "SUPPLIER").length !== supplierIds.length) {
    invalid("Every deployment requires an existing supplier price version");
  }
  return epoch.lineage_token;
}

export function createModelOnboardingRepository(db: SQL, modelName: string) {
  const store = createConfigReleaseStore<ActivatedModel>(
    db,
    { kind: "MODEL", model_name: modelName },
    "model_onboarding_conflict",
  );

  return {
    get: store.get,
    confirmations: store.confirmations,
    async observedRevisions() {
      const [row] = await db<
        {
          epoch: number;
          lineage: string;
          model: number | null;
          routes: Record<string, number>;
        }[]
      >`
        SELECT e.version::float8 AS epoch, e.lineage_token AS lineage, m.source_revision AS model,
          COALESCE(jsonb_object_agg(r.id, r.source_revision)
            FILTER (WHERE r.id IS NOT NULL), '{}'::jsonb) AS routes
        FROM public.config_epoch e
        LEFT JOIN public.models m ON m.name = ${modelName}
        LEFT JOIN public.routes r ON r.model_name = m.name AND r.enabled
        WHERE e.id = 1 GROUP BY e.version, e.lineage_token, m.source_revision
      `;
      return row!;
    },
    validate(input: Onboarding) {
      if (input.model.name !== modelName) conflict("Model does not match this release scope");
      return db.begin((tx) => prepare(tx, input));
    },
    publish(intent: Onboarding, actor: string, validateAdapter: (lineage: string) => void) {
      if (intent.model.name !== modelName) conflict("Model does not match this release scope");
      const input = {
        ...intent,
        deployments: [...intent.deployments].sort((a, b) => a.id.localeCompare(b.id)),
      };
      const identity = { id: input.id, actor, request_digest: catalogDigest(canonical(input)) };
      return db.begin(async (tx) => {
        const replay = await store.lockAndReplay(tx, identity);
        if (replay) return replay;
        const lineage = await prepare(tx, input);
        validateAdapter(lineage);
        const model = input.model;
        const operations = tx.array([...model.operations], "TEXT");
        const [created] = await tx`
          INSERT INTO public.models (name, operations, routing_strategy, max_output_tokens, source_revision, managed_by_release)
          VALUES (${model.name}, ${operations}, ${model.routing_strategy}, ${model.max_output_tokens}, 1, true)
          ON CONFLICT (name) DO NOTHING RETURNING name
        `;
        if (!created) conflict("Model already exists");

        const routes = await tx`
          INSERT INTO public.routes (
            id, model_name, channel_id, upstream_model, operations, priority, weight,
            billing_price_id, cost_version_id, source_revision, managed_by_release
          )
          SELECT id, ${model.name}, channel_id, upstream_model, ${operations}, priority, weight,
            ${input.billing_price_id}::uuid, cost_version_id, 1, true
          FROM jsonb_to_recordset(${JSON.stringify(input.deployments)}::text::jsonb) AS deployment(
            id uuid, channel_id uuid, upstream_model text, priority integer, weight double precision, cost_version_id uuid
          )
          ORDER BY id
          ON CONFLICT (id) DO NOTHING RETURNING id
        `;
        if (routes.length !== input.deployments.length)
          conflict("Deployment identifier already exists");

        return store.record(tx, {
          ...identity,
          expected_epoch: input.expected_epoch,
          expected_lineage: lineage,
          changes: [
            {
              customer_price_id: input.billing_price_id,
              model: {
                operations: model.operations,
                routing_strategy: model.routing_strategy,
                max_output_tokens: model.max_output_tokens,
              },
              deployments: input.deployments,
            },
          ],
          rollback_of: null,
        });
      });
    },
  };
}
