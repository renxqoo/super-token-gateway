import type { SQL } from "bun";
import { canonical, DomainError } from "@gateway/kernel";
import { catalogDigest } from "./catalog-epoch.ts";
import { createConfigReleaseStore } from "./config-releases.ts";

type ReleasedRoute = { id: string; priority: number; weight: number; source_revision: number };

type RouteChange = { before: ReleasedRoute; after: ReleasedRoute; definition_digest: string };

type Target = {
  route_id: string;
  expected_revision: number;
  priority: number;
  weight: number;
};

type ReleaseIntent = {
  id: string;
  expected_epoch: number;
} & ({ targets: readonly Target[] } | { rollback_of: string });

type Deployment = ReleasedRoute & {
  channel_id: string;
  upstream_model: string;
  operations: string[];
  billing_price_id: string;
  cost_version_id: string;
  enabled: boolean;
};

type ValidatedRoute = ReleasedRoute & {
  operations: string[];
  billing_price_id: string;
  available: boolean;
  account_id: string;
  definition: Record<string, unknown>;
};

function conflict(message: string): never {
  throw new DomainError("route_release_conflict", message);
}

function invalid(message: string): never {
  throw new DomainError("invalid_route_release", message);
}

async function prepare(
  tx: SQL,
  model: string,
  expectedEpoch: number,
  targets: readonly Target[],
): Promise<RouteChange[]> {
  const [epoch] = await tx`SELECT version FROM public.config_epoch WHERE id = 1`;
  if (Number(epoch?.version) !== expectedEpoch) conflict("Configuration epoch changed");
  const [firstId, secondId] = targets.map(({ route_id }) => route_id);
  if (!firstId || !secondId || firstId === secondId || targets.length !== 2) {
    invalid("Two distinct routes are required");
  }

  const rows = await tx<ValidatedRoute[]>`
    SELECT r.id, r.priority, r.weight, r.source_revision, r.operations, r.billing_price_id,
      c.account_id,
      jsonb_build_object('route', to_jsonb(r) - ARRAY['priority', 'weight', 'source_revision', 'managed_by_release'],
        'channel_revision', c.config_revision) AS definition,
      r.enabled AND m.enabled AND c.enabled AND p.side = 'CUSTOMER' AND provider.enabled
        AND NOT EXISTS (
          SELECT 1 FROM jsonb_array_elements(r.plugin_pipeline) step
          LEFT JOIN public.plugins content ON content.id::text = step->>'plugin_id'
          WHERE step->>'enabled' IS DISTINCT FROM 'false' AND content.enabled IS DISTINCT FROM true
        ) AS available
    FROM public.routes r
    JOIN public.models m ON m.name = r.model_name
    JOIN public.channels c ON c.id = r.channel_id
    JOIN public.plugins provider ON provider.id = c.provider_plugin_id
    JOIN public.price_versions p ON p.id = r.billing_price_id
    WHERE r.model_name = ${model} AND r.id IN (${firstId}, ${secondId})
    ORDER BY r.id FOR UPDATE OF r
  `;
  const [first, second] = rows;
  if (!first || !second) invalid("Both routes must belong to this model");
  if (
    !first.available ||
    !second.available ||
    first.billing_price_id !== second.billing_price_id ||
    canonical([...first.operations].sort()) !== canonical([...second.operations].sort())
  ) {
    invalid("Both routes must be active and share the same customer price and operations");
  }

  const accounts = await tx<{ state: string }[]>`
    SELECT state FROM public.upstream_accounts
    WHERE id IN (${first.account_id}, ${second.account_id}) ORDER BY id FOR SHARE
  `;
  if (accounts.some(({ state }) => state !== "ACTIVE"))
    invalid("Both upstream accounts must be active");

  const [incompatible] = await tx<{ pricing_differs: boolean }[]>`
    WITH competing AS (
      SELECT r.id, r.billing_price_id, ARRAY(
        SELECT step - 'enabled'
        FROM jsonb_array_elements(r.plugin_pipeline) WITH ORDINALITY AS pipeline(step, position)
        WHERE step->>'enabled' IS DISTINCT FROM 'false'
        ORDER BY position
      ) AS active_pipeline
      FROM public.routes r
      WHERE r.model_name = ${model} AND r.enabled
        AND r.operations && ${tx.array(first.operations, "TEXT")}
    )
    SELECT candidate.billing_price_id <> ${first.billing_price_id} AS pricing_differs
    FROM competing candidate JOIN competing reference ON reference.id = ${first.id}
    WHERE candidate.billing_price_id <> ${first.billing_price_id}
      OR candidate.active_pipeline <> reference.active_pipeline
    ORDER BY pricing_differs DESC
    LIMIT 1
  `;
  if (incompatible) {
    invalid(
      incompatible.pricing_differs
        ? "Enabled competing routes must share the customer price version"
        : "Enabled competing routes must share the same content pipeline",
    );
  }

  const [scopedPricing] = await tx`
    SELECT 1
    FROM public.pricing_modifiers p
    JOIN public.routes r ON r.model_name = ${model} AND r.enabled
      AND r.operations && ${tx.array(first.operations, "TEXT")}
    JOIN public.channels c ON c.id = r.channel_id
    WHERE p.enabled AND COALESCE(p.match->>'side', 'CUSTOMER') = 'CUSTOMER'
      AND COALESCE(p.match->>'model', '*') IN ('*', ${model})
      AND (COALESCE(p.match->>'operation', '*') = '*' OR (p.match->>'operation' = ANY(r.operations) AND p.match->>'operation' = ANY(${tx.array(first.operations, "TEXT")})))
      AND (p.match ? 'channel_id' OR p.match ? 'upstream_account_id')
      AND (NOT (p.match ? 'channel_id') OR p.match->>'channel_id' = c.id::text)
      AND (NOT (p.match ? 'upstream_account_id') OR p.match->>'upstream_account_id' = c.account_id::text)
    LIMIT 1
  `;
  if (scopedPricing) invalid("Customer pricing is scoped to a channel or upstream account");

  return rows.map(({ id, priority, weight, source_revision, definition }) => {
    const target = targets.find(({ route_id }) => route_id === id)!;
    if (source_revision !== target.expected_revision) conflict("Route version changed");
    if (source_revision >= 2_147_483_647) conflict("Route version cannot be advanced");
    return {
      definition_digest: catalogDigest(canonical(definition)),
      before: { id, priority, weight, source_revision },
      after: {
        id,
        priority: target.priority,
        weight: target.weight,
        source_revision: source_revision + 1,
      },
    };
  });
}

export function createRouteReleaseRepository(db: SQL, model: string) {
  const store = createConfigReleaseStore<RouteChange>(
    db,
    { kind: "ROUTE", model_name: model },
    "route_release_conflict",
  );

  return {
    get: store.get,
    confirmations: store.confirmations,

    async view() {
      const [row] = await db<{ config_epoch: number; routes: Deployment[] }[]>`
        SELECT e.version::float8 AS config_epoch,
          COALESCE(jsonb_agg(jsonb_build_object(
            'id', r.id, 'channel_id', r.channel_id, 'upstream_model', r.upstream_model,
            'operations', r.operations, 'billing_price_id', r.billing_price_id,
            'cost_version_id', r.cost_version_id, 'enabled', r.enabled,
            'source_revision', r.source_revision, 'priority', r.priority, 'weight', r.weight
          ) ORDER BY r.priority DESC, r.id) FILTER (WHERE r.id IS NOT NULL), '[]'::jsonb) AS routes
        FROM public.config_epoch e
        JOIN public.models m ON m.name = ${model}
        LEFT JOIN public.routes r ON r.model_name = m.name
        WHERE e.id = 1 GROUP BY e.version
      `;
      if (!row) throw new DomainError("not_found", "Model not found");
      return { model_name: model, ...row };
    },

    validate(expectedEpoch: number, targets: readonly Target[]) {
      return db.begin((tx) => prepare(tx, model, expectedEpoch, targets));
    },

    publish(intent: ReleaseIntent, actor: string) {
      const input =
        "targets" in intent
          ? {
              ...intent,
              targets: [...intent.targets].sort((a, b) => a.route_id.localeCompare(b.route_id)),
            }
          : intent;
      const identity = {
        id: input.id,
        actor,
        request_digest: catalogDigest(canonical({ model, ...input })),
      };
      return db.begin(async (tx) => {
        const replay = await store.lockAndReplay(tx, identity);
        if (replay) return replay;
        const rollbackOf = "rollback_of" in input ? input.rollback_of : null;
        const original = rollbackOf ? await store.get(rollbackOf, tx) : null;
        if (rollbackOf && !original) throw new DomainError("not_found", "Route release not found");
        const targets =
          "targets" in input
            ? input.targets
            : original!.changes.map(({ before, after }) => ({
                route_id: after.id,
                expected_revision: after.source_revision,
                priority: before.priority,
                weight: before.weight,
              }));
        const changes = await prepare(tx, model, intent.expected_epoch, targets);
        if (
          original &&
          canonical(
            changes.map(({ before, definition_digest }) => ({ ...before, definition_digest })),
          ) !==
            canonical(
              original.changes.map(({ after, definition_digest }) => ({
                ...after,
                definition_digest,
              })),
            )
        ) {
          conflict("A route has changed since the original release");
        }
        for (const { after } of changes) {
          await tx`
            UPDATE public.routes
            SET priority = ${after.priority}, weight = ${after.weight},
              source_revision = ${after.source_revision}, managed_by_release = true
            WHERE id = ${after.id}
          `;
        }
        return store.record(tx, {
          ...identity,
          expected_epoch: intent.expected_epoch,
          changes,
          rollback_of: rollbackOf,
        });
      });
    },
  };
}
