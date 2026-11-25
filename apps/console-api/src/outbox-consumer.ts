import type { SQL } from "bun";
import { and, eq, inArray, sql } from "drizzle-orm";
import { fetchPublicWebhook } from "./auth.ts";
import {
  alertDeliveries,
  alertRules,
  billingAccounts,
  usageDailyRollups,
  type ConsoleDrizzle,
} from "./db/schema.ts";
import type { GatewayClient } from "./gateway-client.ts";

export type IncomingOutboxEvent = {
  seq?: string | number | null;
  event_id: string;
  kind: string;
  request_id?: string | null;
  billing_account_id?: string | null;
  payload?: Record<string, unknown>;
  created_at?: string;
};

type RollupAgg = {
  bucket: string;
  billingAccountId: string;
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

type ParsedEvent = {
  eventId: string;
  seqNum: number | null;
  kind: string;
  requestId: string | null;
  chargeId: string | null;
  effectiveBillingAccountId: string | null;
  orgId: string | null;
  apiKeyId: string | null;
  model: string | null;
  operation: string | null;
  channelId: string | null;
  outcome: string | null;
  currency: string;
  customerAtoms: bigint;
  supplierCostAtoms: bigint;
  reqCountDelta: number;
  inputTokens: number;
  outputTokens: number;
  cachedInputTokens: number;
  reasoningTokens: number;
  pricingOverrideId: string | null;
  payload: Record<string, unknown>;
  occurredAt: string;
  hasBillingAccountRecord: boolean;
};

function parseBigIntSafe(value: unknown): bigint {
  if (typeof value === "bigint") return value;
  if (typeof value === "number" && Number.isFinite(value)) return BigInt(Math.trunc(value));
  if (typeof value === "string" && /^-?\d+$/.test(value.trim())) return BigInt(value.trim());
  return 0n;
}

function parseNullableBigInt(value: unknown): bigint | null {
  if (typeof value === "bigint") return value;
  if (typeof value === "number" && Number.isFinite(value)) return BigInt(Math.trunc(value));
  if (typeof value === "string" && /^-?\d+$/.test(value.trim())) return BigInt(value.trim());
  return null;
}

function parseIntSafe(value: unknown): number {
  const n = Number(parseBigIntSafe(value));
  return Number.isSafeInteger(n) && n >= 0 ? n : 0;
}

function asNullableUuid(value: unknown): string | null {
  if (typeof value !== "string") return null;
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)
    ? value.toLowerCase()
    : null;
}

function compareRollupKey(a: RollupAgg, b: RollupAgg): number {
  return (
    a.bucket.localeCompare(b.bucket) ||
    a.billingAccountId.localeCompare(b.billingAccountId) ||
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
  const key = `${bucket}\0${delta.billingAccountId}\0${delta.apiKeyId}\0${delta.model}\0${delta.operation}\0${delta.currency}`;
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
  events: readonly IncomingOutboxEvent[],
): Promise<{ received: number; ingested: number }> {
  if (events.length === 0) return { received: 0, ingested: 0 };

  const billingAccountIds = [
    ...new Set(
      events
        .map((e) => asNullableUuid(e.billing_account_id))
        .filter((id): id is string => id !== null),
    ),
  ];
  const billingAccountByGatewayId = new Map<
    string,
    { id: string; orgId: string; currency: string }
  >();
  if (billingAccountIds.length > 0) {
    const accountRows = await orm
      .select({
        id: billingAccounts.id,
        gatewayBillingAccountId: billingAccounts.gatewayBillingAccountId,
        orgId: billingAccounts.orgId,
        currency: billingAccounts.currency,
      })
      .from(billingAccounts)
      .where(inArray(billingAccounts.gatewayBillingAccountId, billingAccountIds));
    for (const r of accountRows) {
      billingAccountByGatewayId.set(r.gatewayBillingAccountId, {
        id: r.id,
        orgId: r.orgId,
        currency: r.currency,
      });
    }
  }

  const parsedEvents: ParsedEvent[] = [];
  const seenEventIds = new Set<string>();
  for (const ev of events) {
    const eventId = asNullableUuid(ev.event_id);
    if (!eventId || seenEventIds.has(eventId)) continue;
    seenEventIds.add(eventId);
    const payload = (ev.payload ?? {}) as Record<string, unknown>;
    const usage = (payload.usage ?? {}) as Record<string, unknown>;
    const gatewayAccountId = asNullableUuid(ev.billing_account_id);
    const accountMetadata = gatewayAccountId
      ? billingAccountByGatewayId.get(gatewayAccountId)
      : undefined;
    const currency =
      typeof payload.currency === "string"
        ? payload.currency
        : (accountMetadata?.currency ?? "USD");

    let customerAtoms = 0n;
    let supplierCostAtoms = 0n;
    let reqCountDelta = 0;
    let inputTokens = 0;
    let outputTokens = 0;
    let cachedInputTokens = 0;
    let reasoningTokens = 0;

    if (ev.kind === "billing.refunded" || ev.kind === "billing.adjusted") {
      customerAtoms = parseBigIntSafe(payload.delta_atoms);
    } else if (ev.kind === "billing.settled") {
      reqCountDelta = 1;
      customerAtoms = parseBigIntSafe(payload.customer_atoms);
      supplierCostAtoms = parseBigIntSafe(payload.supplier_cost_atoms);
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

    parsedEvents.push({
      eventId,
      seqNum,
      kind: ev.kind,
      requestId: asNullableUuid(ev.request_id),
      chargeId: asNullableUuid(payload.charge_id),
      effectiveBillingAccountId: accountMetadata?.id ?? gatewayAccountId,
      orgId: accountMetadata?.orgId ?? null,
      apiKeyId: asNullableUuid(payload.api_key_id),
      model: typeof payload.model === "string" ? payload.model : null,
      operation: typeof payload.operation === "string" ? payload.operation : null,
      channelId: asNullableUuid(payload.channel_id),
      outcome: typeof payload.outcome === "string" ? payload.outcome : null,
      currency,
      customerAtoms,
      supplierCostAtoms,
      reqCountDelta,
      inputTokens,
      outputTokens,
      cachedInputTokens,
      reasoningTokens,
      pricingOverrideId: asNullableUuid(payload.pricing_override_id),
      payload,
      occurredAt,
      hasBillingAccountRecord: Boolean(accountMetadata),
    });
  }

  if (parsedEvents.length === 0) return { received: events.length, ingested: 0 };

  let ingested = 0;

  await db.begin(async (tx) => {
    const insertedRows = await tx<{ event_id: string }[]>`
      INSERT INTO public.usage_events (
        event_id, seq, kind, request_id, charge_id, billing_account_id, org_id, api_key_id,
        model, operation, channel_id, outcome, currency, customer_atoms, supplier_cost_atoms,
        input_tokens, output_tokens, cached_input_tokens, reasoning_tokens,
        pricing_override_id, payload, occurred_at
      )
      SELECT
        u.event_id, u.seq, u.kind, u.request_id, u.charge_id, u.billing_account_id, u.org_id, u.api_key_id,
        u.model, u.operation, u.channel_id, u.outcome, u.currency, u.customer_atoms, u.supplier_cost_atoms,
        u.input_tokens, u.output_tokens, u.cached_input_tokens, u.reasoning_tokens,
        u.pricing_override_id, u.payload, u.occurred_at
      FROM unnest(
        ${parsedEvents.map((e) => e.eventId)}::uuid[],
        ${parsedEvents.map((e) => e.seqNum)}::bigint[],
        ${parsedEvents.map((e) => e.kind)}::text[],
        ${parsedEvents.map((e) => e.requestId)}::uuid[],
        ${parsedEvents.map((e) => e.chargeId)}::uuid[],
        ${parsedEvents.map((e) => e.effectiveBillingAccountId)}::uuid[],
        ${parsedEvents.map((e) => e.orgId)}::uuid[],
        ${parsedEvents.map((e) => e.apiKeyId)}::uuid[],
        ${parsedEvents.map((e) => e.model)}::text[],
        ${parsedEvents.map((e) => e.operation)}::text[],
        ${parsedEvents.map((e) => e.channelId)}::uuid[],
        ${parsedEvents.map((e) => e.outcome)}::text[],
        ${parsedEvents.map((e) => e.currency)}::text[],
        ${parsedEvents.map((e) => e.customerAtoms.toString())}::numeric[],
        ${parsedEvents.map((e) => e.supplierCostAtoms.toString())}::numeric[],
        ${parsedEvents.map((e) => e.inputTokens)}::bigint[],
        ${parsedEvents.map((e) => e.outputTokens)}::bigint[],
        ${parsedEvents.map((e) => e.cachedInputTokens)}::bigint[],
        ${parsedEvents.map((e) => e.reasoningTokens)}::bigint[],
        ${parsedEvents.map((e) => e.pricingOverrideId)}::uuid[],
        ${parsedEvents.map((e) => JSON.stringify(e.payload))}::jsonb[],
        ${parsedEvents.map((e) => e.occurredAt)}::timestamptz[]
      ) AS u(
        event_id, seq, kind, request_id, charge_id, billing_account_id, org_id, api_key_id,
        model, operation, channel_id, outcome, currency, customer_atoms, supplier_cost_atoms,
        input_tokens, output_tokens, cached_input_tokens, reasoning_tokens,
        pricing_override_id, payload, occurred_at
      )
      ON CONFLICT (event_id) DO NOTHING
      RETURNING event_id::text AS event_id
    `;

    const insertedSet = new Set(insertedRows.map((r) => r.event_id));
    ingested = insertedSet.size;
    if (ingested === 0) return;

    const hourlyMap = new Map<string, RollupAgg>();
    const dailyMap = new Map<string, RollupAgg>();
    const alertBillingAccountIds = new Set<string>();

    for (const ev of parsedEvents) {
      if (!insertedSet.has(ev.eventId)) continue;
      if (
        ev.kind !== "billing.settled" &&
        ev.kind !== "billing.refunded" &&
        ev.kind !== "billing.adjusted"
      ) {
        continue;
      }
      if (ev.effectiveBillingAccountId && ev.hasBillingAccountRecord) {
        alertBillingAccountIds.add(ev.effectiveBillingAccountId);
      }
      if (!ev.effectiveBillingAccountId || !ev.model || !ev.operation) continue;

      const dt = new Date(ev.occurredAt);
      const bucketHour = new Date(
        Date.UTC(dt.getUTCFullYear(), dt.getUTCMonth(), dt.getUTCDate(), dt.getUTCHours()),
      ).toISOString();
      const bucketDay = bucketHour.slice(0, 10);
      const delta = {
        billingAccountId: ev.effectiveBillingAccountId,
        apiKeyId: ev.apiKeyId ?? "",
        model: ev.model,
        operation: ev.operation,
        currency: ev.currency,
        requestCount: ev.reqCountDelta,
        customerAtoms: ev.customerAtoms,
        supplierCostAtoms: ev.supplierCostAtoms,
        inputTokens: ev.inputTokens,
        outputTokens: ev.outputTokens,
        cachedInputTokens: ev.cachedInputTokens,
        reasoningTokens: ev.reasoningTokens,
      };
      addRollupDelta(hourlyMap, bucketHour, delta);
      addRollupDelta(dailyMap, bucketDay, delta);
    }

    const hourlySorted = [...hourlyMap.values()].sort(compareRollupKey);
    if (hourlySorted.length > 0) {
      await tx`
        INSERT INTO public.usage_hourly_rollups (
          bucket_hour, billing_account_id, api_key_id, model, operation, currency,
          request_count, customer_atoms, supplier_cost_atoms,
          input_tokens, output_tokens, cached_input_tokens, reasoning_tokens
        )
        SELECT * FROM unnest(
          ${hourlySorted.map((r) => r.bucket)}::timestamptz[],
          ${hourlySorted.map((r) => r.billingAccountId)}::uuid[],
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
        ON CONFLICT (bucket_hour, billing_account_id, api_key_id, model, operation, currency)
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
          bucket_day, billing_account_id, api_key_id, model, operation, currency,
          request_count, customer_atoms, supplier_cost_atoms,
          input_tokens, output_tokens, cached_input_tokens, reasoning_tokens
        )
        SELECT * FROM unnest(
          ${dailySorted.map((r) => r.bucket)}::date[],
          ${dailySorted.map((r) => r.billingAccountId)}::uuid[],
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
        ON CONFLICT (bucket_day, billing_account_id, api_key_id, model, operation, currency)
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

    const alertEntries = [...alertBillingAccountIds].sort();
    if (alertEntries.length > 0) {
      await tx`
        INSERT INTO public.alert_eval_queue (billing_account_id)
        SELECT unnest(${alertEntries}::uuid[])
        ON CONFLICT (billing_account_id)
        DO UPDATE SET
          dirty_at = EXCLUDED.dirty_at,
          generation = EXCLUDED.generation,
          next_attempt_at = EXCLUDED.next_attempt_at
      `;
    }
  });

  return { received: events.length, ingested };
}

async function claimAndRecordAlert(
  orm: ConsoleDrizzle,
  rule: { id: string; billingAccountId: string; webhookUrl: string | null },
  payload: Record<string, unknown>,
): Promise<boolean> {
  return orm.transaction(async (tx) => {
    const claimed = await tx
      .update(alertRules)
      .set({ lastTriggeredAt: sql`now()`, updatedAt: sql`now()` })
      .where(
        and(
          eq(alertRules.id, rule.id),
          eq(alertRules.enabled, true),
          sql`(${alertRules.lastTriggeredAt} IS NULL OR ${alertRules.lastTriggeredAt} <= now() - interval '1 hour')`,
        ),
      )
      .returning({ id: alertRules.id });
    if (claimed.length === 0) return false;
    await tx.insert(alertDeliveries).values({
      id: crypto.randomUUID(),
      ruleId: rule.id,
      billingAccountId: rule.billingAccountId,
      status: rule.webhookUrl ? "PENDING" : "RECORDED",
      webhookUrl: rule.webhookUrl ?? null,
      attempts: 0,
      nextAttemptAt: new Date(),
      payload,
    });
    return true;
  });
}

export async function processPendingAlertEvaluations(
  db: SQL,
  orm: ConsoleDrizzle,
  gateway: GatewayClient,
  limit = 20,
): Promise<number> {
  const claimed = await db<
    {
      billing_account_id: string;
      generation: string;
    }[]
  >`
    WITH due AS (
      SELECT billing_account_id
      FROM public.alert_eval_queue
      WHERE next_attempt_at <= now()
      ORDER BY next_attempt_at, dirty_at, billing_account_id
      LIMIT ${limit}
      FOR UPDATE SKIP LOCKED
    )
    UPDATE public.alert_eval_queue q
    SET next_attempt_at = now() + interval '60 seconds',
        generation = gen_random_uuid()
    FROM due
    WHERE q.billing_account_id = due.billing_account_id
    RETURNING q.billing_account_id, q.generation
  `;
  if (claimed.length === 0) return 0;

  const billingAccountIds = claimed.map((c) => c.billing_account_id);

  const rules = await orm
    .select()
    .from(alertRules)
    .where(
      and(inArray(alertRules.billingAccountId, billingAccountIds), eq(alertRules.enabled, true)),
    );
  const accountRows = await orm
    .select({
      id: billingAccounts.id,
      gatewayBillingAccountId: billingAccounts.gatewayBillingAccountId,
      currency: billingAccounts.currency,
    })
    .from(billingAccounts)
    .where(inArray(billingAccounts.id, billingAccountIds));
  const accountsById = new Map(accountRows.map((p) => [p.id, p]));

  const todayStr = new Date().toISOString().slice(0, 10);
  const dailyRows = await orm
    .select({
      billingAccountId: usageDailyRollups.billingAccountId,
      currency: usageDailyRollups.currency,
      total: sql<string>`COALESCE(sum(${usageDailyRollups.customerAtoms}), 0)::text`,
    })
    .from(usageDailyRollups)
    .where(
      and(
        inArray(usageDailyRollups.billingAccountId, billingAccountIds),
        eq(usageDailyRollups.bucketDay, todayStr),
      ),
    )
    .groupBy(usageDailyRollups.billingAccountId, usageDailyRollups.currency);
  const dailySpendByAccountCurrency = new Map<string, bigint>();
  for (const r of dailyRows) {
    dailySpendByAccountCurrency.set(
      `${r.billingAccountId}\0${r.currency}`,
      parseBigIntSafe(r.total),
    );
  }

  const walletBillingAccountIds = new Set(
    rules.filter((rule) => rule.kind === "LOW_BALANCE").map((rule) => rule.billingAccountId),
  );
  const fetchedWalletByAccount = new Map(
    await Promise.all(
      accountRows
        .filter((billingAccount) => walletBillingAccountIds.has(billingAccount.id))
        .map(async (billingAccount) => {
          const wallet = await gateway
            .getWallet(billingAccount.gatewayBillingAccountId, billingAccount.currency)
            .catch(() => null);
          return [
            billingAccount.id,
            wallet ? parseNullableBigInt(wallet.available_atoms) : null,
          ] as const;
        }),
    ),
  );
  let triggered = 0;
  const retryBillingAccounts = new Set<string>();
  for (const rule of rules) {
    const billingAccount = accountsById.get(rule.billingAccountId);
    if (!billingAccount) continue;
    const threshold = BigInt(rule.thresholdAtoms);

    if (rule.kind === "LOW_BALANCE") {
      const available = fetchedWalletByAccount.get(billingAccount.id) ?? null;
      if (available === null) {
        retryBillingAccounts.add(billingAccount.id);
        continue;
      }
      if (available <= threshold) {
        const recorded = await claimAndRecordAlert(orm, rule, {
          kind: "LOW_BALANCE",
          billing_account_id: rule.billingAccountId,
          available_atoms: available.toString(),
          threshold_atoms: threshold.toString(),
          currency: billingAccount.currency,
        });
        if (recorded) triggered++;
      }
    } else if (rule.kind === "DAILY_SPEND") {
      const spentToday =
        dailySpendByAccountCurrency.get(`${rule.billingAccountId}\0${billingAccount.currency}`) ??
        0n;
      if (spentToday >= threshold) {
        const recorded = await claimAndRecordAlert(orm, rule, {
          kind: "DAILY_SPEND",
          billing_account_id: rule.billingAccountId,
          spent_atoms: spentToday.toString(),
          threshold_atoms: threshold.toString(),
          currency: billingAccount.currency,
          day: todayStr,
        });
        if (recorded) triggered++;
      }
    }
  }

  await db`
    WITH processed AS (
      SELECT * FROM unnest(
        ${claimed.map((item) => item.billing_account_id)}::uuid[],
        ${claimed.map((item) => item.generation)}::uuid[],
        ${claimed.map((item) => retryBillingAccounts.has(item.billing_account_id))}::boolean[]
      ) AS p(billing_account_id, generation, retry)
    ), locked AS (
      SELECT q.billing_account_id, q.generation, p.retry
      FROM public.alert_eval_queue q
      JOIN processed p USING (billing_account_id, generation)
      ORDER BY q.billing_account_id
      FOR UPDATE OF q
    ), deferred AS (
      UPDATE public.alert_eval_queue q
      SET next_attempt_at = now() + interval '15 seconds'
      FROM locked l
      WHERE q.billing_account_id = l.billing_account_id AND q.generation = l.generation AND l.retry
    )
    DELETE FROM public.alert_eval_queue q
    USING locked l
    WHERE q.billing_account_id = l.billing_account_id AND q.generation = l.generation AND NOT l.retry
  `;
  return triggered;
}

export async function processPendingWebhookDeliveries(
  db: SQL,
  options?: {
    fetchImpl?: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;
    allowPrivateWebhooks?: boolean;
    maxAttempts?: number;
    batchLimit?: number;
    timeoutMs?: number;
    signal?: AbortSignal;
  },
): Promise<{ attempted: number; delivered: number; failed: number }> {
  const fetchImpl = options?.fetchImpl ?? fetch;
  const maxAttempts = options?.maxAttempts ?? 5;
  const batchLimit = options?.batchLimit ?? 50;
  const timeoutMs = options?.timeoutMs ?? 5_000;

  let attempted = 0;
  let delivered = 0;
  let failed = 0;

  while (attempted < batchLimit && !options?.signal?.aborted) {
    const [item] = await db<
      {
        id: string;
        rule_id: string;
        billing_account_id: string;
        webhook_url: string | null;
        attempts: number;
        payload: Record<string, unknown>;
      }[]
    >`
      WITH due AS (
        SELECT id
        FROM public.alert_deliveries
        WHERE status = 'PENDING' AND next_attempt_at <= now()
        ORDER BY next_attempt_at ASC, id
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      )
      UPDATE public.alert_deliveries d
      SET attempts = d.attempts + 1,
          next_attempt_at = now() + interval '60 seconds'
      FROM due
      WHERE d.id = due.id
      RETURNING
        d.id::text AS id,
        d.rule_id::text AS rule_id,
        d.billing_account_id::text AS billing_account_id,
        d.webhook_url,
        d.attempts,
        d.payload
    `;

    if (!item) break;
    attempted++;
    const url = item.webhook_url;
    if (!url) {
      await db`
        UPDATE public.alert_deliveries
        SET status = 'RECORDED'
        WHERE id = ${item.id}::uuid AND attempts = ${item.attempts} AND status = 'PENDING'
      `;
      continue;
    }

    let ok = false;
    let httpStatus: number | null = null;
    let errorMessage: string | null = null;
    let permanentFailure = false;

    try {
      const timeout = AbortSignal.timeout(timeoutMs);
      const init: RequestInit = {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "user-agent": "super-token-gateway-console-alerts/1.0",
          "x-stg-alert-delivery-id": item.id,
        },
        body: JSON.stringify({
          delivery_id: item.id,
          rule_id: item.rule_id,
          ...item.payload,
          billing_account_id: item.billing_account_id,
        }),
        redirect: "manual",
        signal: options?.signal ? AbortSignal.any([options.signal, timeout]) : timeout,
      };
      const res = options?.allowPrivateWebhooks
        ? await fetchImpl(url, init)
        : await fetchPublicWebhook(url, init, fetchImpl);
      httpStatus = res.status;
      await res.body?.cancel().catch(() => undefined);
      if (res.status >= 200 && res.status < 300) {
        ok = true;
      } else {
        errorMessage = `Webhook returned HTTP ${res.status}`;
        if (res.status >= 300 && res.status < 400) {
          permanentFailure = true;
        }
      }
    } catch (err) {
      if (options?.signal?.aborted) {
        await db`
          UPDATE public.alert_deliveries
          SET attempts = attempts - 1,
              next_attempt_at = now()
          WHERE id = ${item.id}::uuid AND attempts = ${item.attempts} AND status = 'PENDING'
        `;
        break;
      }
      errorMessage = err instanceof Error ? err.message.slice(0, 500) : String(err).slice(0, 500);
      if (err instanceof Error && "code" in err && err.code === "unsafe_webhook_url") {
        permanentFailure = true;
      }
    }

    if (ok) {
      const rows = await db`
        UPDATE public.alert_deliveries
        SET status = 'DELIVERED',
            last_http_status = ${httpStatus},
            last_error = NULL,
            delivered_at = now()
        WHERE id = ${item.id}::uuid AND attempts = ${item.attempts} AND status = 'PENDING'
        RETURNING id
      `;
      delivered += rows.length;
    } else if (permanentFailure || item.attempts >= maxAttempts) {
      const rows = await db`
        UPDATE public.alert_deliveries
        SET status = 'FAILED',
            last_http_status = ${httpStatus},
            last_error = ${errorMessage}
        WHERE id = ${item.id}::uuid AND attempts = ${item.attempts} AND status = 'PENDING'
        RETURNING id
      `;
      failed += rows.length;
    } else {
      const backoffSec = Math.min(3600, 15 * 2 ** (item.attempts - 1));
      await db`
        UPDATE public.alert_deliveries
        SET status = 'PENDING',
            last_http_status = ${httpStatus},
            last_error = ${errorMessage},
            next_attempt_at = now() + (${backoffSec}::text || ' seconds')::interval
        WHERE id = ${item.id}::uuid AND attempts = ${item.attempts} AND status = 'PENDING'
      `;
    }
  }

  return { attempted, delivered, failed };
}

export async function pullOutboxBatch(
  db: SQL,
  orm: ConsoleDrizzle,
  gateway: GatewayClient,
  consumerName: string,
  limit = 200,
): Promise<{ claimed: number; ingested: number; acked: boolean }> {
  let totalClaimed = 0;
  let totalIngested = 0;
  let lastAcked = true;
  for (let round = 0; round < 10; round++) {
    const claimed = await gateway.claimOutboxEvents(consumerName, limit);
    if (claimed.events.length === 0) break;
    const res = await ingestOutboxEvents(db, orm, claimed.events);
    const lastSeq = claimed.events.at(-1)!.seq;
    const ack = await gateway.ackOutboxEvents(consumerName, claimed.lease_token, lastSeq);
    totalClaimed += claimed.events.length;
    totalIngested += res.ingested;
    lastAcked = Boolean(ack.ok);
    if (!lastAcked || claimed.events.length < limit) break;
  }
  return { claimed: totalClaimed, ingested: totalIngested, acked: lastAcked };
}
