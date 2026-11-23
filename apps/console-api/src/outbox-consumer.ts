import type { SQL } from "bun";
import { and, eq, inArray, sql } from "drizzle-orm";
import {
  alertDeliveries,
  alertRules,
  projects,
  usageDailyRollups,
  type ConsoleDrizzle,
} from "./db/schema.ts";
import type { GatewayClient } from "./gateway-client.ts";

export type IncomingOutboxEvent = {
  seq?: string | number | null;
  event_id: string;
  kind: string;
  request_id?: string | null;
  project_id?: string | null;
  payload?: Record<string, unknown>;
  created_at?: string;
};

type RollupAgg = {
  bucket: string;
  projectId: string;
  apiKeyId: string;
  model: string;
  operation: string;
  currency: string;
  requestCount: number;
  customerAtoms: bigint;
  supplierCostAtoms: bigint;
  inputTokens: number;
  outputTokens: number;
  cachedInputTokens: number;
  reasoningTokens: number;
};

function parseBigIntSafe(value: unknown): bigint {
  if (typeof value === "bigint") return value;
  if (typeof value === "number" && Number.isFinite(value)) return BigInt(Math.trunc(value));
  if (typeof value === "string" && /^-?\d+$/.test(value.trim())) return BigInt(value.trim());
  return 0n;
}

function parseIntSafe(value: unknown): number {
  const n = Number(parseBigIntSafe(value));
  return Number.isSafeInteger(n) && n >= 0 ? n : 0;
}

function asNullableUuid(value: unknown): string | null {
  if (typeof value !== "string") return null;
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)
    ? value
    : null;
}

function compareRollupKey(a: RollupAgg, b: RollupAgg): number {
  return (
    a.bucket.localeCompare(b.bucket) ||
    a.projectId.localeCompare(b.projectId) ||
    a.apiKeyId.localeCompare(b.apiKeyId) ||
    a.model.localeCompare(b.model) ||
    a.operation.localeCompare(b.operation) ||
    a.currency.localeCompare(b.currency)
  );
}

function addRollupDelta(
  map: Map<string, RollupAgg>,
  bucket: string,
  delta: Omit<RollupAgg, "bucket">,
): void {
  const key = `${bucket}\0${delta.projectId}\0${delta.apiKeyId}\0${delta.model}\0${delta.operation}\0${delta.currency}`;
  const existing = map.get(key);
  if (existing) {
    existing.requestCount += delta.requestCount;
    existing.customerAtoms += delta.customerAtoms;
    existing.supplierCostAtoms += delta.supplierCostAtoms;
    existing.inputTokens += delta.inputTokens;
    existing.outputTokens += delta.outputTokens;
    existing.cachedInputTokens += delta.cachedInputTokens;
    existing.reasoningTokens += delta.reasoningTokens;
  } else {
    map.set(key, { bucket, ...delta });
  }
}

export async function ingestOutboxEvents(
  db: SQL,
  orm: ConsoleDrizzle,
  gateway: GatewayClient,
  events: readonly IncomingOutboxEvent[],
): Promise<{ received: number; ingested: number; alertsTriggered: number }> {
  if (events.length === 0) return { received: 0, ingested: 0, alertsTriggered: 0 };

  const projectIds = [
    ...new Set(
      events
        .map((e) => asNullableUuid(e.project_id ?? e.payload?.project_id))
        .filter((id): id is string => id !== null),
    ),
  ];
  const projOrgMap = new Map<string, { id: string; orgId: string; currency: string }>();
  if (projectIds.length > 0) {
    const projRows = await orm
      .select({
        id: projects.id,
        gatewayProjectId: projects.gatewayProjectId,
        orgId: projects.orgId,
        currency: projects.currency,
      })
      .from(projects)
      .where(inArray(projects.gatewayProjectId, projectIds));
    for (const r of projRows) {
      projOrgMap.set(r.gatewayProjectId, { id: r.id, orgId: r.orgId, currency: r.currency });
    }
  }

  let ingested = 0;
  const touchedProjects = new Set<string>();
  const hourlyMap = new Map<string, RollupAgg>();
  const dailyMap = new Map<string, RollupAgg>();

  await db.begin(async (tx) => {
    for (const ev of events) {
      const eventId = asNullableUuid(ev.event_id);
      if (!eventId) continue;
      const payload = (ev.payload ?? {}) as Record<string, unknown>;
      const usage = (payload.usage ?? {}) as Record<string, unknown>;
      const gwProjId = asNullableUuid(ev.project_id ?? payload.project_id);
      const projMeta = gwProjId ? projOrgMap.get(gwProjId) : undefined;
      const requestId = asNullableUuid(ev.request_id ?? payload.request_id);
      const chargeId = asNullableUuid(payload.charge_id);
      const apiKeyId = asNullableUuid(payload.api_key_id);
      const channelId = asNullableUuid(payload.channel_id);
      const pricingOverrideId = asNullableUuid(payload.pricing_override_id);
      const model = typeof payload.model === "string" ? payload.model : null;
      const operation = typeof payload.operation === "string" ? payload.operation : null;
      const outcome = typeof payload.outcome === "string" ? payload.outcome : null;
      const currency =
        typeof payload.currency === "string" ? payload.currency : (projMeta?.currency ?? "USD");

      let customerAtomsBig = 0n;
      let supplierAtomsBig = 0n;
      let reqCountDelta = 0;
      let inputTokens = 0;
      let outputTokens = 0;
      let cachedInputTokens = 0;
      let reasoningTokens = 0;

      if (ev.kind === "billing.refunded") {
        customerAtomsBig =
          payload.delta_atoms !== undefined
            ? parseBigIntSafe(payload.delta_atoms)
            : -parseBigIntSafe(payload.refunded_atoms);
      } else if (ev.kind === "billing.adjusted") {
        customerAtomsBig = parseBigIntSafe(payload.delta_atoms);
      } else {
        reqCountDelta = 1;
        customerAtomsBig = parseBigIntSafe(payload.customer_atoms);
        supplierAtomsBig = parseBigIntSafe(payload.supplier_cost_atoms);
        inputTokens = parseIntSafe(usage.input_tokens);
        outputTokens = parseIntSafe(usage.output_tokens);
        cachedInputTokens = parseIntSafe(usage.cached_input_tokens);
        reasoningTokens = parseIntSafe(usage.reasoning_tokens);
      }

      const seqNum = ev.seq !== undefined && ev.seq !== null ? parseIntSafe(ev.seq) : null;
      const occurredAt =
        ev.created_at && !Number.isNaN(Date.parse(ev.created_at))
          ? new Date(ev.created_at).toISOString()
          : new Date().toISOString();

      const inserted = await tx`
        INSERT INTO public.usage_events (
          event_id, seq, kind, request_id, charge_id, project_id, org_id, api_key_id,
          model, operation, channel_id, outcome, currency, customer_atoms, supplier_cost_atoms,
          input_tokens, output_tokens, cached_input_tokens, reasoning_tokens,
          pricing_override_id, payload, occurred_at
        )
        VALUES (
          ${eventId}::uuid, ${seqNum}, ${ev.kind}, ${requestId}::uuid, ${chargeId}::uuid,
          ${projMeta?.id ?? gwProjId}::uuid, ${projMeta?.orgId ?? null}::uuid, ${apiKeyId}::uuid,
          ${model}, ${operation}, ${channelId}::uuid, ${outcome}, ${currency},
          ${customerAtomsBig.toString()}::numeric, ${supplierAtomsBig.toString()}::numeric,
          ${inputTokens}, ${outputTokens}, ${cachedInputTokens}, ${reasoningTokens},
          ${pricingOverrideId}::uuid, ${payload}::jsonb, ${occurredAt}::timestamptz
        )
        ON CONFLICT (event_id) DO NOTHING
        RETURNING event_id
      `;
      if (inserted.length === 0) continue;
      ingested++;

      const effectiveProjectId = projMeta?.id ?? gwProjId;
      if (!effectiveProjectId || !model || !operation) continue;
      touchedProjects.add(effectiveProjectId);

      const dt = new Date(occurredAt);
      const bucketHour = new Date(
        Date.UTC(dt.getUTCFullYear(), dt.getUTCMonth(), dt.getUTCDate(), dt.getUTCHours()),
      ).toISOString();
      const bucketDay = bucketHour.slice(0, 10);
      const delta = {
        projectId: effectiveProjectId,
        apiKeyId: apiKeyId ?? "",
        model,
        operation,
        currency,
        requestCount: reqCountDelta,
        customerAtoms: customerAtomsBig,
        supplierCostAtoms: supplierAtomsBig,
        inputTokens,
        outputTokens,
        cachedInputTokens,
        reasoningTokens,
      };
      addRollupDelta(hourlyMap, bucketHour, delta);
      addRollupDelta(dailyMap, bucketDay, delta);
    }

    const hourlySorted = [...hourlyMap.values()].sort(compareRollupKey);
    if (hourlySorted.length > 0) {
      await tx`
        INSERT INTO public.usage_hourly_rollups (
          bucket_hour, project_id, api_key_id, model, operation, currency,
          request_count, customer_atoms, supplier_cost_atoms,
          input_tokens, output_tokens, cached_input_tokens, reasoning_tokens
        )
        SELECT * FROM unnest(
          ${hourlySorted.map((r) => r.bucket)}::timestamptz[],
          ${hourlySorted.map((r) => r.projectId)}::uuid[],
          ${hourlySorted.map((r) => r.apiKeyId)}::text[],
          ${hourlySorted.map((r) => r.model)}::text[],
          ${hourlySorted.map((r) => r.operation)}::text[],
          ${hourlySorted.map((r) => r.currency)}::text[],
          ${hourlySorted.map((r) => r.requestCount)}::bigint[],
          ${hourlySorted.map((r) => r.customerAtoms.toString())}::numeric[],
          ${hourlySorted.map((r) => r.supplierCostAtoms.toString())}::numeric[],
          ${hourlySorted.map((r) => r.inputTokens)}::bigint[],
          ${hourlySorted.map((r) => r.outputTokens)}::bigint[],
          ${hourlySorted.map((r) => r.cachedInputTokens)}::bigint[],
          ${hourlySorted.map((r) => r.reasoningTokens)}::bigint[]
        )
        ON CONFLICT (bucket_hour, project_id, api_key_id, model, operation, currency)
        DO UPDATE SET
          request_count = public.usage_hourly_rollups.request_count + EXCLUDED.request_count,
          customer_atoms = public.usage_hourly_rollups.customer_atoms + EXCLUDED.customer_atoms,
          supplier_cost_atoms = public.usage_hourly_rollups.supplier_cost_atoms + EXCLUDED.supplier_cost_atoms,
          input_tokens = public.usage_hourly_rollups.input_tokens + EXCLUDED.input_tokens,
          output_tokens = public.usage_hourly_rollups.output_tokens + EXCLUDED.output_tokens,
          cached_input_tokens = public.usage_hourly_rollups.cached_input_tokens + EXCLUDED.cached_input_tokens,
          reasoning_tokens = public.usage_hourly_rollups.reasoning_tokens + EXCLUDED.reasoning_tokens
      `;
    }

    const dailySorted = [...dailyMap.values()].sort(compareRollupKey);
    if (dailySorted.length > 0) {
      await tx`
        INSERT INTO public.usage_daily_rollups (
          bucket_day, project_id, api_key_id, model, operation, currency,
          request_count, customer_atoms, supplier_cost_atoms,
          input_tokens, output_tokens, cached_input_tokens, reasoning_tokens
        )
        SELECT * FROM unnest(
          ${dailySorted.map((r) => r.bucket)}::date[],
          ${dailySorted.map((r) => r.projectId)}::uuid[],
          ${dailySorted.map((r) => r.apiKeyId)}::text[],
          ${dailySorted.map((r) => r.model)}::text[],
          ${dailySorted.map((r) => r.operation)}::text[],
          ${dailySorted.map((r) => r.currency)}::text[],
          ${dailySorted.map((r) => r.requestCount)}::bigint[],
          ${dailySorted.map((r) => r.customerAtoms.toString())}::numeric[],
          ${dailySorted.map((r) => r.supplierCostAtoms.toString())}::numeric[],
          ${dailySorted.map((r) => r.inputTokens)}::bigint[],
          ${dailySorted.map((r) => r.outputTokens)}::bigint[],
          ${dailySorted.map((r) => r.cachedInputTokens)}::bigint[],
          ${dailySorted.map((r) => r.reasoningTokens)}::bigint[]
        )
        ON CONFLICT (bucket_day, project_id, api_key_id, model, operation, currency)
        DO UPDATE SET
          request_count = public.usage_daily_rollups.request_count + EXCLUDED.request_count,
          customer_atoms = public.usage_daily_rollups.customer_atoms + EXCLUDED.customer_atoms,
          supplier_cost_atoms = public.usage_daily_rollups.supplier_cost_atoms + EXCLUDED.supplier_cost_atoms,
          input_tokens = public.usage_daily_rollups.input_tokens + EXCLUDED.input_tokens,
          output_tokens = public.usage_daily_rollups.output_tokens + EXCLUDED.output_tokens,
          cached_input_tokens = public.usage_daily_rollups.cached_input_tokens + EXCLUDED.cached_input_tokens,
          reasoning_tokens = public.usage_daily_rollups.reasoning_tokens + EXCLUDED.reasoning_tokens
      `;
    }
  });

  let alertsTriggered = 0;
  if (touchedProjects.size > 0) {
    alertsTriggered = await evaluateProjectAlerts(orm, gateway, [...touchedProjects]);
  }

  return { received: events.length, ingested, alertsTriggered };
}

async function claimAlertCooldown(orm: ConsoleDrizzle, ruleId: string): Promise<boolean> {
  const claimed = await orm
    .update(alertRules)
    .set({ lastTriggeredAt: sql`now()`, updatedAt: sql`now()` })
    .where(
      and(
        eq(alertRules.id, ruleId),
        eq(alertRules.enabled, true),
        sql`(${alertRules.lastTriggeredAt} IS NULL OR ${alertRules.lastTriggeredAt} <= now() - interval '1 hour')`,
      ),
    )
    .returning({ id: alertRules.id });
  return claimed.length > 0;
}

async function evaluateProjectAlerts(
  orm: ConsoleDrizzle,
  gateway: GatewayClient,
  projectIds: string[],
): Promise<number> {
  const rules = await orm
    .select()
    .from(alertRules)
    .where(and(inArray(alertRules.projectId, projectIds), eq(alertRules.enabled, true)));
  if (rules.length === 0) return 0;

  const projRows = await orm
    .select({
      id: projects.id,
      gatewayProjectId: projects.gatewayProjectId,
      currency: projects.currency,
    })
    .from(projects)
    .where(inArray(projects.id, projectIds));
  const projById = new Map(projRows.map((p) => [p.id, p]));

  const todayStr = new Date().toISOString().slice(0, 10);
  let triggered = 0;

  for (const rule of rules) {
    const proj = projById.get(rule.projectId);
    if (!proj) continue;
    const threshold = BigInt(rule.thresholdAtoms);

    if (rule.kind === "LOW_BALANCE") {
      const wallet = await gateway
        .getWallet(proj.gatewayProjectId, proj.currency)
        .catch(() => null);
      if (!wallet) continue;
      const available = parseBigIntSafe(wallet.available_atoms);
      if (available <= threshold && (await claimAlertCooldown(orm, rule.id))) {
        triggered++;
        await orm.insert(alertDeliveries).values({
          id: crypto.randomUUID(),
          ruleId: rule.id,
          projectId: rule.projectId,
          status: "RECORDED",
          payload: {
            kind: "LOW_BALANCE",
            available_atoms: available.toString(),
            threshold_atoms: threshold.toString(),
            currency: proj.currency,
          },
        });
      }
    } else if (rule.kind === "DAILY_SPEND") {
      const [row] = await orm
        .select({
          total: sql<string>`COALESCE(sum(${usageDailyRollups.customerAtoms}), 0)::text`,
        })
        .from(usageDailyRollups)
        .where(
          and(
            eq(usageDailyRollups.projectId, rule.projectId),
            eq(usageDailyRollups.bucketDay, todayStr),
            eq(usageDailyRollups.currency, proj.currency),
          ),
        );
      const spentToday = parseBigIntSafe(row?.total);
      if (spentToday >= threshold && (await claimAlertCooldown(orm, rule.id))) {
        triggered++;
        await orm.insert(alertDeliveries).values({
          id: crypto.randomUUID(),
          ruleId: rule.id,
          projectId: rule.projectId,
          status: "RECORDED",
          payload: {
            kind: "DAILY_SPEND",
            spent_atoms: spentToday.toString(),
            threshold_atoms: threshold.toString(),
            currency: proj.currency,
            day: todayStr,
          },
        });
      }
    }
  }
  return triggered;
}

export async function pullOutboxBatch(
  db: SQL,
  orm: ConsoleDrizzle,
  gateway: GatewayClient,
  consumerName: string,
  limit = 200,
): Promise<{ claimed: number; ingested: number; acked: boolean }> {
  const claimed = await gateway.claimOutboxEvents(consumerName, limit);
  if (claimed.events.length === 0) return { claimed: 0, ingested: 0, acked: true };
  const res = await ingestOutboxEvents(db, orm, gateway, claimed.events);
  const lastSeq = claimed.events.at(-1)!.seq;
  const ack = await gateway.ackOutboxEvents(consumerName, claimed.lease_token, lastSeq);
  return { claimed: claimed.events.length, ingested: res.ingested, acked: Boolean(ack.ok) };
}
