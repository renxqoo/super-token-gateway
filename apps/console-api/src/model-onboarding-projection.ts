import { and, eq, inArray, sql } from "drizzle-orm";
import {
  GatewayError,
  type GatewayModelOnboardingResult,
  type ModelOnboardingInput,
} from "@gateway/contracts";
import {
  channels,
  consoleAuditLogs,
  modelCatalog,
  routes,
  routeReleaseRequests,
  type ConsoleDrizzle,
} from "./db/schema.ts";

import type { GatewayClient } from "./gateway-client.ts";
import { LOCK_NS } from "./sync-state.ts";

type Tx = Parameters<Parameters<ConsoleDrizzle["transaction"]>[0]>[0];
type RequestRow = typeof routeReleaseRequests.$inferSelect;

export function onboardingConflict(message: string): never {
  throw new GatewayError(409, "model_onboarding_conflict", message);
}

export async function reserveModelOnboarding(tx: Tx, input: ModelOnboardingInput) {
  await tx.execute(
    sql`SELECT pg_advisory_xact_lock(${LOCK_NS.model}::int, hashtext(${input.model.name}))`,
  );
  const channelIds = [...new Set(input.deployments.map(({ channel_id }) => channel_id))];
  const upstream = await tx
    .select({
      enabled: channels.enabled,
      syncStatus: channels.syncStatus,
      synced: channels.syncedRevision,
      desired: channels.desiredRevision,
      secretPending: channels.secretSyncPending,
    })
    .from(channels)
    .where(inArray(channels.id, channelIds));
  if (
    upstream.length !== channelIds.length ||
    upstream.some(
      ({ enabled, syncStatus, synced, desired, secretPending }) =>
        !enabled || syncStatus !== "SYNCED" || synced !== desired || secretPending,
    )
  ) {
    onboardingConflict("Every deployment needs an enabled, synced Console channel");
  }

  const model = input.model;
  const [draft] = await tx
    .insert(modelCatalog)
    .values({
      name: model.name,
      displayName: model.display_name,
      providerBrand: model.provider_brand,
      description: model.description,
      category: model.category,
      contextLength: model.context_length,
      maxOutputTokens: model.max_output_tokens,
      operations: model.operations,
      routingStrategy: model.routing_strategy,
      tags: model.tags,
      isPublic: model.is_public,
      pendingReleaseId: input.id,
    })
    .onConflictDoNothing()
    .returning({ name: modelCatalog.name });
  if (!draft) onboardingConflict("Model already exists or belongs to a pending release");

  const deployments = await tx
    .insert(routes)
    .values(
      [...input.deployments]
        .sort((a, b) => a.id.localeCompare(b.id))
        .map((item) => ({
          id: item.id,
          modelName: model.name,
          channelId: item.channel_id,
          upstreamModel: item.upstream_model,
          operations: model.operations,
          priority: item.priority,
          weight: item.weight,
          billingPriceId: input.billing_price_id,
          costVersionId: item.cost_version_id,
          pendingReleaseId: input.id,
        })),
    )
    .onConflictDoNothing()
    .returning({ id: routes.id });
  if (deployments.length !== input.deployments.length) {
    onboardingConflict("Deployment identifier already exists or belongs to a pending release");
  }
}

function assertMatch(
  row: RequestRow,
  input: ModelOnboardingInput,
  result: GatewayModelOnboardingResult,
) {
  if (
    row.digest !== result.request_digest ||
    row.id !== result.id ||
    row.modelName !== result.model_name ||
    input.billing_price_id !== result.customer_price_id
  ) {
    onboardingConflict("Gateway release does not match the Console request");
  }
}

function readyToProject(result: GatewayModelOnboardingResult) {
  if (
    result.current_configuration !== "ACTIVE" ||
    result.local_applied_epoch < result.config_epoch
  ) {
    return false;
  }
  const statuses = new Map(result.nodes.map((node) => [node.node_id, node.status]));
  return result.expected_nodes.every((id) => statuses.has(id) && statuses.get(id) !== "PENDING");
}

export async function promoteModelOnboarding(
  orm: ConsoleDrizzle,
  row: RequestRow,
  input: ModelOnboardingInput,
  initial: GatewayModelOnboardingResult,
  getPriceVersion: GatewayClient["getPriceVersion"],
  readStatus: () => Promise<GatewayModelOnboardingResult>,
) {
  assertMatch(row, input, initial);
  if (row.state === "COMMITTED") return { projected: true, result: initial };
  if (row.state === "REJECTED") onboardingConflict("Onboarding was rejected. Use a new identifier");
  const price = readyToProject(initial) ? await getPriceVersion(initial.customer_price_id) : null;
  const result = price ? await readStatus() : initial;
  assertMatch(row, input, result);
  if (!price || !readyToProject(result)) {
    const [latest] = await orm
      .select({ state: routeReleaseRequests.state })
      .from(routeReleaseRequests)
      .where(eq(routeReleaseRequests.id, row.id));
    return { projected: latest?.state === "COMMITTED", result };
  }

  const revisions = result.observed_revisions;
  if (
    !revisions ||
    revisions.model === null ||
    input.deployments.some(({ id }) => !Number.isInteger(revisions.routes[id]))
  ) {
    onboardingConflict("Gateway did not confirm all release revisions");
  }
  const modelRevision = revisions.model;
  const routeRevisions = sql<number>`(${JSON.stringify(revisions.routes)}::text::jsonb ->> ${routes.id}::text)::integer`;
  if (price.side !== "CUSTOMER" || price.id !== result.customer_price_id) {
    onboardingConflict("Gateway did not confirm the customer price version");
  }

  await orm.transaction(async (tx) => {
    const [pending] = await tx
      .update(routeReleaseRequests)
      .set({ state: "COMMITTED" })
      .where(and(eq(routeReleaseRequests.id, row.id), eq(routeReleaseRequests.state, "PENDING")))
      .returning({ id: routeReleaseRequests.id });
    if (!pending) {
      const [latest] = await tx
        .select({ state: routeReleaseRequests.state })
        .from(routeReleaseRequests)
        .where(eq(routeReleaseRequests.id, row.id));
      if (latest?.state !== "COMMITTED") onboardingConflict("Onboarding is no longer pending");
      return;
    }

    const [model] = await tx
      .update(modelCatalog)
      .set({
        listPriceVersionId: price.id,
        listRates: price.rates,
        currency: price.currency,
        syncedRevision: modelRevision,
        desiredRevision: modelRevision,
        syncStatus: "SYNCED",
        pendingReleaseId: null,
        updatedAt: sql`now()`,
      })
      .where(and(eq(modelCatalog.name, row.modelName), eq(modelCatalog.pendingReleaseId, row.id)))
      .returning({ name: modelCatalog.name });
    if (!model) onboardingConflict("The model draft is no longer reserved for this release");

    const deployments = await tx
      .update(routes)
      .set({
        syncedRevision: routeRevisions,
        desiredRevision: routeRevisions,
        syncStatus: "SYNCED",
        pendingReleaseId: null,
        updatedAt: sql`now()`,
      })
      .where(and(eq(routes.modelName, row.modelName), eq(routes.pendingReleaseId, row.id)))
      .returning({ id: routes.id });
    if (deployments.length !== input.deployments.length) {
      onboardingConflict("The deployment drafts are no longer reserved for this release");
    }

    await tx.insert(consoleAuditLogs).values({
      id: crypto.randomUUID(),
      actorUserId: row.actorUserId,
      actorEmail: row.actorEmail,
      action: "admin.model_onboarding.publish",
      targetType: "model_onboarding",
      targetId: row.id,
      metadata: { model_name: row.modelName, config_epoch: result.config_epoch },
    });
  });
  return { projected: true, result };
}

export async function discardModelOnboarding(tx: Tx, row: RequestRow) {
  await tx
    .delete(modelCatalog)
    .where(and(eq(modelCatalog.name, row.modelName), eq(modelCatalog.pendingReleaseId, row.id)));
}
