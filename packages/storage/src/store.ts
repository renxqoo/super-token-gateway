import { Batcher } from "@gateway/runtime/batcher";
import { fundingError } from "./funds.ts";
import type { SQL } from "bun";
import {
  upstreamCostConfigurationSchema,
  opaqueResourceIdSchema,
  batchResourceMetadataSchema,
  GatewayError,
} from "@gateway/contracts";
import { sha256 } from "@gateway/runtime/crypto";
import type { BillingQuote, Operation, Pricing, Finality, Usage } from "@gateway/kernel";
import type {
  BatchResourceMetadata,
  Credential,
  Project,
  Resource,
  Route,
  UsageEvidence,
  RequestPolicy,
} from "@gateway/contracts";

type KeyInput = {
  name: string;
  models: string[];
  operations: Operation[];
  principal_id?: string | undefined;
  role?: string | undefined;
  allowed_cidrs?: string[] | undefined;
  rpm?: number | undefined;
  tpm?: number | undefined;
  concurrency?: number | undefined;
  expires_at?: string | undefined;
};
type ProjectInput = {
  name: string;
  external_ref?: string | undefined;
  billing_currency?: string | undefined;
  organization_id?: string | undefined;
  rpm: number;
  tpm: number;
  concurrency: number;
};
type ProviderInput = {
  name: string;
  kind: string;
  base_url: string;
  concurrency: number;
  rpm: number;
};
type RouteInput = {
  provider_id: string;
  upstream_model: string;
  operations: Operation[];
  priority: number;
  weight: number;
  pricing: Pricing;
  ceilings: Usage;
  request_policy?: RequestPolicy | undefined;
};
interface ModelMetadata {
  name: string;
  operations: Operation[];
  max_output_tokens: number;
  routing_strategy: string;
}
export interface RequestRecord {
  id: string;
  state: string;
  fingerprint: string;
}
export interface UsageRecord {
  id: string;
  seq: string;
  project_id: string;
  request_id: string;
  attempt_id: string;
  source: string;
  semantics: "attempt_snapshot" | "disputed_snapshot";
  snapshot_revision: string | null;
  cost_revision: string | null;
  finality: Finality;
  usage: Usage;
  cost_atoms: string | null;
  currency: string;
  cost_complete: boolean;
}

export interface TokenSummary {
  complete: boolean;
  tokens: number;
}

interface PersistenceResult {
  status: string;
  error?: string;
  billing?: { state: string; replayed?: boolean };
}
export class Store {
  private modelCache = new Map<
    string,
    {
      until: number;
      model: {
        name: string;
        operations: Operation[];
        max_output_tokens: number;
        routing_strategy: string;
      };
    }
  >();
  private routeCache = new Map<string, { until: number; routes: Route[] }>();
  private readonly modelLoads = new Map<string, Promise<ModelMetadata>>();
  private readonly routeLoads = new Map<string, Promise<Route[]>>();
  private readonly authenticationBatch: Batcher<
    string,
    { credential: Credential; project: Project } | undefined
  >;
  private readonly admissionBatch: Batcher<Record<string, unknown>, Record<string, unknown>>;
  private readonly persistenceBatch: Batcher<UsageEvidence[], PersistenceResult[]>;
  constructor(private readonly db: SQL) {
    this.persistenceBatch = new Batcher(
      async (groups) => {
        const events = groups.flat();
        const rows = await this.db<
          { result: Record<string, PersistenceResult> }[]
        >`SELECT persist_usage_batch(${events}::jsonb) AS result`;
        let offset = 0;
        return groups.map((group) =>
          group.map(() => {
            const result = rows[0]?.result[String(offset++)];
            if (!result) throw new Error("Usage commit result cardinality mismatch");
            return result;
          }),
        );
      },
      256,
      5,
      4,
      8192,
      (events) => events.length,
    );
    this.authenticationBatch = new Batcher((hashes) => this.authenticateMany(hashes), 128, 3);
    this.admissionBatch = new Batcher(
      async (inputs) => {
        const rows = await this.db`SELECT admit_gateway_batch(${inputs}::jsonb) AS result`;
        return inputs.map((_, i) => rows[0].result[String(i)]);
      },
      128,
      5,
      1,
    );
  }

  async createOrganization(input: {
    external_ref: string;
    name: string;
    rpm?: number | undefined;
    tpm?: number | undefined;
    concurrency?: number | undefined;
  }): Promise<string> {
    const id = crypto.randomUUID();
    await this
      .db`INSERT INTO organizations(id,external_ref,name,rpm,tpm,concurrency) VALUES(${id},${input.external_ref},${input.name},${input.rpm ?? 100000},${input.tpm ?? 1000000000},${input.concurrency ?? 10000})`;
    return id;
  }
  async updateOrganization(
    id: string,
    input: {
      enabled?: boolean | undefined;
      rpm?: number | undefined;
      tpm?: number | undefined;
      concurrency?: number | undefined;
    },
  ): Promise<void> {
    const rows = await this
      .db`UPDATE organizations SET enabled=coalesce(${input.enabled ?? null},enabled),rpm=coalesce(${input.rpm ?? null},rpm),tpm=coalesce(${input.tpm ?? null},tpm),concurrency=coalesce(${input.concurrency ?? null},concurrency) WHERE id=${id} RETURNING id`;
    if (!rows.length) throw new GatewayError(404, "not_found", "Organization not found");
  }
  async createPrincipal(
    organizationId: string,
    input: {
      external_ref: string;
      role: string;
      rpm?: number | undefined;
      tpm?: number | undefined;
      concurrency?: number | undefined;
    },
  ): Promise<string> {
    const id = crypto.randomUUID();
    await this
      .db`INSERT INTO principals(id,organization_id,external_ref,role,rpm,tpm,concurrency) VALUES(${id},${organizationId},${input.external_ref},${input.role},${input.rpm ?? 10000},${input.tpm ?? 100000000},${input.concurrency ?? 1000})`;
    return id;
  }
  async updatePrincipal(
    id: string,
    input: {
      enabled?: boolean | undefined;
      role?: string | undefined;
      rpm?: number | undefined;
      tpm?: number | undefined;
      concurrency?: number | undefined;
    },
  ): Promise<void> {
    const rows = await this
      .db`UPDATE principals SET enabled=coalesce(${input.enabled ?? null},enabled),role=coalesce(${input.role ?? null},role),rpm=coalesce(${input.rpm ?? null},rpm),tpm=coalesce(${input.tpm ?? null},tpm),concurrency=coalesce(${input.concurrency ?? null},concurrency) WHERE id=${id} RETURNING id`;
    if (!rows.length) throw new GatewayError(404, "not_found", "Principal not found");
  }
  async revokeProjectKey(id: string, projectId: string): Promise<void> {
    const rows = await this
      .db`UPDATE api_keys SET enabled=false WHERE id=${id} AND project_id=${projectId} RETURNING id`;
    if (!rows.length) throw new GatewayError(404, "not_found", "Key not found");
  }
  async createProject(input: ProjectInput): Promise<string> {
    const id = crypto.randomUUID();
    await this
      .db`INSERT INTO projects(id,name,external_ref,rpm,tpm,concurrency,billing_currency,organization_id)
      VALUES(${id},${input.name},${input.external_ref ?? null},${input.rpm},${input.tpm},${input.concurrency},${input.billing_currency ?? "USD"},${input.organization_id ?? null})`;
    return id;
  }
  async projects(): Promise<Project[]> {
    return this.db<Project[]>`SELECT * FROM projects ORDER BY created_at DESC LIMIT 200`;
  }
  async setProject(id: string, enabled: boolean): Promise<void> {
    const rows = await this.db`UPDATE projects SET enabled=${enabled} WHERE id=${id} RETURNING id`;
    if (!rows.length) throw new GatewayError(404, "not_found", "Project not found");
  }
  async createCredential(
    projectId: string,
    input: KeyInput,
    hash: string,
    prefix: string,
    connection: SQL = this.db,
  ): Promise<string> {
    const id = crypto.randomUUID();
    await connection`INSERT INTO api_keys(id,project_id,name,verifier,prefix,models,operations,expires_at,principal_id,role,allowed_cidrs,rpm,tpm,concurrency)
      VALUES(${id},${projectId},${input.name},${hash},${prefix},${input.models},${input.operations},${input.expires_at ?? null},${input.principal_id ?? null},${input.role ?? "INVOKER"},${input.allowed_cidrs ?? []},${input.rpm ?? null},${input.tpm ?? null},${input.concurrency ?? null})`;
    return id;
  }

  async delegateCredential(
    parentId: string,
    requested: KeyInput,
    explicitOperations: boolean,
    hash: string,
    prefix: string,
  ): Promise<{ id: string; input: KeyInput }> {
    return this.db.begin(async (tx) => {
      const rows = await tx<
        (Credential & { expires_at: string | null })[]
      >`SELECT k.id,k.project_id,k.principal_id,k.models,k.operations,k.allowed_cidrs,k.rpm,k.tpm::float8,k.concurrency,k.expires_at::text
        FROM api_keys k JOIN projects p ON p.id=k.project_id
        LEFT JOIN organizations o ON o.id=p.organization_id LEFT JOIN principals u ON u.id=k.principal_id
        WHERE k.id=${parentId} AND k.enabled AND k.role='ADMIN' AND p.enabled
        AND (k.expires_at IS NULL OR k.expires_at>clock_timestamp())
        AND (o.id IS NULL OR o.enabled) AND (u.id IS NULL OR u.enabled AND u.role IN ('OWNER','ADMIN'))
        FOR SHARE OF k`;
      const parent = rows[0];
      const denied = () => {
        throw new GatewayError(
          403,
          "delegation_denied",
          "Delegated credentials must remain within the issuing credential's authority",
        );
      };
      if (!parent) denied();
      const authority = parent!;
      const input = { ...requested, allowed_cidrs: requested.allowed_cidrs ?? [] };
      if (authority.principal_id) {
        if (input.principal_id && input.principal_id !== authority.principal_id) denied();
        input.principal_id = authority.principal_id;
      }
      if (authority.models.length) {
        if (input.models.some((model) => !authority.models.includes(model))) denied();
        if (!input.models.length) input.models = [...authority.models];
      }
      if (!explicitOperations) input.operations = [...authority.operations];
      if (input.operations.some((operation) => !authority.operations.includes(operation))) denied();
      if (authority.allowed_cidrs?.length) {
        if (!input.allowed_cidrs.length) input.allowed_cidrs = [...authority.allowed_cidrs];
        const ranges = await tx`SELECT NOT EXISTS(
          SELECT 1 FROM unnest(${input.allowed_cidrs}::text[]) child(cidr) WHERE NOT EXISTS(
            SELECT 1 FROM unnest(${authority.allowed_cidrs}::text[]) source(cidr) WHERE child.cidr::inet <<= source.cidr::inet
          )) AS permitted`;
        if (!ranges[0]?.permitted) denied();
      }
      if (authority.expires_at) {
        const deadline = new Date(authority.expires_at).getTime();
        if (input.expires_at && new Date(input.expires_at).getTime() > deadline) denied();
        input.expires_at ??= new Date(deadline).toISOString();
      }
      for (const field of ["rpm", "tpm", "concurrency"] as const) {
        const ceiling = authority[field];
        if (ceiling !== null && ceiling !== undefined) {
          if (input[field] !== undefined && input[field]! > ceiling) denied();
          input[field] ??= ceiling;
        }
      }
      const id = await this.createCredential(
        authority.project_id,
        input,
        hash,
        prefix,
        tx as unknown as SQL,
      );
      return { id, input };
    });
  }
  async keys(projectId: string): Promise<unknown[]> {
    return this
      .db`SELECT id,project_id,name,prefix,models,operations,enabled,expires_at,created_at,principal_id,role,allowed_cidrs,rpm,tpm,concurrency FROM api_keys WHERE project_id=${projectId} ORDER BY created_at DESC LIMIT 200`;
  }
  async revokeKey(id: string): Promise<void> {
    await this.db`UPDATE api_keys SET enabled=false WHERE id=${id}`;
  }
  async authenticate(
    hash: string,
  ): Promise<{ credential: Credential; project: Project } | undefined> {
    return this.authenticationBatch.add(hash);
  }
  private async authenticateMany(
    hashes: string[],
  ): Promise<({ credential: Credential; project: Project } | undefined)[]> {
    const rows = await this.db<
      (Project &
        Credential & {
          verifier: string;
          key_id: string;
          key_rpm: number | null;
          key_tpm: number | null;
          key_concurrency: number | null;
        })[]
    >`SELECT k.verifier,k.id AS key_id,k.project_id,k.models,k.operations,k.principal_id,k.role,k.allowed_cidrs,k.rpm AS key_rpm,k.tpm::float8 AS key_tpm,k.concurrency AS key_concurrency,u.rpm AS principal_rpm,u.tpm::float8 AS principal_tpm,u.concurrency AS principal_concurrency,
    p.id,p.name,p.enabled,p.rpm,p.tpm,p.concurrency,p.billing_currency,p.organization_id,o.rpm AS organization_rpm,o.tpm::float8 AS organization_tpm,o.concurrency AS organization_concurrency
    FROM api_keys k JOIN projects p ON p.id=k.project_id LEFT JOIN organizations o ON o.id=p.organization_id LEFT JOIN principals u ON u.id=k.principal_id
    WHERE k.verifier=ANY(${[...new Set(hashes)]}::text[]) AND k.enabled AND p.enabled AND (k.expires_at IS NULL OR k.expires_at>now()) AND (o.id IS NULL OR o.enabled) AND (u.id IS NULL OR u.enabled AND (k.role<>'ADMIN' OR u.role IN ('OWNER','ADMIN')) AND (k.role<>'INVOKER' OR u.role IN ('OWNER','ADMIN','MEMBER')) AND (k.role<>'BILLING' OR u.role IN ('OWNER','ADMIN','BILLING')))`;
    const byHash = new Map(rows.map((row) => [row.verifier, row]));
    return hashes.map((hash) => {
      const row = byHash.get(hash);
      return row
        ? {
            credential: {
              id: row.key_id,
              project_id: row.project_id,
              models: row.models,
              operations: row.operations,
              principal_id: row.principal_id ?? null,
              role: row.role ?? "INVOKER",
              allowed_cidrs: row.allowed_cidrs ?? [],
              rpm: row.key_rpm,
              tpm: row.key_tpm,
              concurrency: row.key_concurrency,
              principal_rpm: row.principal_rpm ?? 0,
              principal_tpm: row.principal_tpm ?? 0,
              principal_concurrency: row.principal_concurrency ?? 0,
            },
            project: row,
          }
        : undefined;
    });
  }
  async createProvider(id: string, input: ProviderInput, secret: string) {
    return this.db.begin((tx) => recordProvider(tx, id, input, secret));
  }
  async providers(): Promise<unknown[]> {
    return this
      .db`SELECT id,name,kind,base_url,enabled,concurrency,rpm,brand_id,account_id,binding_id,config_revision::text FROM channels ORDER BY name LIMIT 500`;
  }
  async provider(id: string): Promise<Route> {
    const rows = await this.db<Route[]>`SELECT *,id AS provider_id FROM channels WHERE id=${id}`;
    if (!rows[0])
      throw new GatewayError(503, "provider_unavailable", "The original provider is unavailable");
    return rows[0];
  }
  async setProvider(id: string, enabled: boolean): Promise<void> {
    await this.db`UPDATE channels SET enabled=${enabled} WHERE id=${id}`;
    this.routeCache.clear();
    this.routeLoads.clear();
  }
  async createModel(input: {
    name: string;
    operations: Operation[];
    max_output_tokens: number;
    routing_strategy?: string;
  }): Promise<void> {
    await this
      .db`INSERT INTO models(name,operations,max_output_tokens,routing_strategy) VALUES(${input.name},${input.operations},${input.max_output_tokens},${input.routing_strategy ?? "WEIGHTED"})`;
  }
  async models(
    allowed: string[] = [],
  ): Promise<
    { name: string; operations: Operation[]; max_output_tokens: number; routing_strategy: string }[]
  > {
    return this
      .db`SELECT name,operations,max_output_tokens,routing_strategy FROM models WHERE enabled AND (${allowed.length === 0} OR name=ANY(${allowed})) ORDER BY name LIMIT 2000`;
  }
  async model(name: string): Promise<ModelMetadata> {
    const cached = this.modelCache.get(name);
    if (cached && cached.until > Date.now()) return cached.model;
    const pending = this.modelLoads.get(name);
    if (pending) return pending;
    let load!: Promise<ModelMetadata>;
    load = (async () => {
      const rows = await this.db<
        ModelMetadata[]
      >`SELECT name,operations,max_output_tokens,routing_strategy FROM models WHERE name=${name} AND enabled`;
      if (!rows[0]) throw new GatewayError(404, "model_not_found", "Model not found");
      if (this.modelCache.size > 2000) this.modelCache.clear();
      if (this.modelLoads.get(name) === load)
        this.modelCache.set(name, { until: Date.now() + 1000, model: rows[0] });
      return rows[0];
    })();
    this.modelLoads.set(name, load);
    try {
      return await load;
    } finally {
      if (this.modelLoads.get(name) === load) this.modelLoads.delete(name);
    }
  }
  async createRoute(model: string, input: RouteInput): Promise<string> {
    const id = await this.db.begin((tx) => recordRoute(tx, model, input));
    this.routeCache.clear();
    this.routeLoads.clear();
    return id;
  }
  async routes(model: string, operation: Operation): Promise<Route[]> {
    const key = `${model}:${operation}`;
    const cached = this.routeCache.get(key);
    if (cached && cached.until > Date.now()) return cached.routes;
    const pending = this.routeLoads.get(key);
    if (pending) return pending;
    let load!: Promise<Route[]>;
    load = (async () => {
      const rows = await this.db<
        Route[]
      >`SELECT r.*,p.kind,p.base_url,p.secret,p.concurrency,p.rpm,jsonb_build_object('currency',v.currency,'rates',v.rates) AS pricing,v.ceilings FROM routes r JOIN channels p ON p.id=r.provider_id JOIN models m ON m.name=r.model_name JOIN cost_versions v ON v.id=r.pricing_id WHERE r.model_name=${model} AND r.enabled AND p.enabled AND m.enabled AND ${operation}=ANY(r.operations) AND ${operation}=ANY(m.operations) ORDER BY r.priority DESC`;
      if (this.routeCache.size > 10000) this.routeCache.clear();
      if (this.routeLoads.get(key) === load)
        this.routeCache.set(key, { until: Date.now() + 1000, routes: rows });
      return rows;
    })();
    this.routeLoads.set(key, load);
    try {
      return await load;
    } finally {
      if (this.routeLoads.get(key) === load) this.routeLoads.delete(key);
    }
  }
  async admitInitial(
    id: string,
    credential: Credential,
    operation: Operation,
    model: string,
    digest: string | null,
    fingerprint: string,
    route: Route,
    quote: BillingQuote,
    signal?: AbortSignal,
  ): Promise<{ record: RequestRecord; created: boolean; attempt_id?: string }> {
    const payload = {
      id,
      key_id: credential.id,
      project_id: credential.project_id,
      operation,
      model,
      digest,
      fingerprint,
      route_id: route.id,
      cost_pricing_id: route.pricing_id,
      quote,
    };
    try {
      signal?.throwIfAborted();
      const deadline = signal ? Infinity : Date.now() + 2000;
      let backoff = 2;
      let result = await this.admissionBatch.add(payload);
      while (result.error === "55P03" && Date.now() < deadline) {
        signal?.throwIfAborted();
        await Bun.sleep(backoff + Math.random() * 5);
        backoff = Math.min(50, backoff * 2);
        signal?.throwIfAborted();
        result = await this.admissionBatch.add(payload);
      }
      if (result.error === "55P03")
        throw new GatewayError(503, "wallet_busy", "Wallet admission deadline reached");
      if (result.error) throw { errno: String(result.error) };
      return result as unknown as { record: RequestRecord; created: boolean; attempt_id?: string };
    } catch (error) {
      if ((error as { errno?: string }).errno === "P0409")
        throw new GatewayError(
          409,
          "idempotency_conflict",
          "Idempotency key has a different payload",
        );
      fundingError(error);
    }
  }
  async request(
    id: string,
    projectId: string,
    models: string[] = [],
    operations: Operation[] = [],
  ): Promise<unknown> {
    const rows = await this
      .db`SELECT id,operation,model,state,created_at,updated_at FROM gateway_requests WHERE id=${id} AND project_id=${projectId}
      AND (${models.length === 0} OR model=ANY(${models})) AND (${operations.length === 0} OR operation=ANY(${operations}))`;
    if (!rows[0]) throw new GatewayError(404, "not_found", "Request not found");
    const attempts = await this
      .db`SELECT id,ordinal,state,error_code,started_at,finished_at FROM attempts WHERE request_id=${id} ORDER BY ordinal`;
    const usage = await this
      .db`SELECT attempt_id,source,CASE record_kind WHEN 'DISPUTED' THEN 'disputed_snapshot' ELSE 'attempt_snapshot' END AS semantics,snapshot_revision::text,(SELECT c.revision::text FROM upstream_costs c WHERE c.evidence_id=usage_events.id) AS cost_revision,finality,usage,cost_atoms::text,currency,cost_complete FROM usage_events WHERE request_id=${id} ORDER BY seq`;
    return { ...rows[0], attempts, usage };
  }
  async startAttempt(requestId: string, ordinal: number, route: Route): Promise<string> {
    const id = crypto.randomUUID();
    const rows = await this
      .db`WITH admitted AS (INSERT INTO attempts(id,request_id,ordinal,provider_id,upstream_model,pricing_id,state,billing_pricing_id)
      SELECT ${id},q.id,${ordinal},r.provider_id,r.upstream_model,r.pricing_id,'DISPATCHING',(q.billing_quote->'prices'->>r.provider_id::text)::uuid
      FROM gateway_requests q JOIN api_keys k ON k.id=q.key_id JOIN projects p ON p.id=q.project_id
      LEFT JOIN organizations o ON o.id=p.organization_id LEFT JOIN principals u ON u.id=k.principal_id
      JOIN routes r ON r.id=${route.id} AND r.model_name=q.model JOIN models m ON m.name=q.model JOIN channels pr ON pr.id=r.provider_id
      WHERE q.id=${requestId} AND k.enabled AND p.enabled AND (k.expires_at IS NULL OR k.expires_at>clock_timestamp())
      AND k.role IN ('ADMIN','INVOKER') AND q.operation=ANY(k.operations) AND (cardinality(k.models)=0 OR q.model=ANY(k.models))
      AND (o.id IS NULL OR o.enabled) AND (u.id IS NULL OR u.enabled AND u.role IN ('OWNER','ADMIN','MEMBER'))
      AND r.enabled AND m.enabled AND pr.enabled AND q.operation=ANY(r.operations) AND q.operation=ANY(m.operations)
      AND r.provider_id=${route.provider_id} AND r.upstream_model=${route.upstream_model} AND r.pricing_id=${route.pricing_id}
      AND pr.kind=${route.kind} AND pr.base_url=${route.base_url} AND pr.secret=${route.secret} RETURNING id,request_id)
      UPDATE gateway_requests q SET state='RUNNING',updated_at=now() FROM admitted a WHERE q.id=a.request_id RETURNING a.id`;
    if (!rows.length)
      throw new GatewayError(
        403,
        "dispatch_denied",
        "Credential or upstream configuration changed before retry dispatch",
      );
    return id;
  }
  async persistUsage(events: UsageEvidence[]): Promise<Record<string, number>> {
    if (!events.length || events.length > 128)
      throw new RangeError("Usage batch requires 1 to 128 events");
    const counts: Record<string, number> = {};
    let pending = events;
    let failed = false;
    const deadline = Date.now() + 2000;
    let backoff = 2;
    do {
      const outcomes = await this.persistenceBatch.add(pending);
      const retry: UsageEvidence[] = [];
      for (const [index, result] of outcomes.entries()) {
        if (result.status === "ERROR" && result.error === "55P03") {
          retry.push(pending[Number(index)]!);
          continue;
        }
        if (result.status === "ERROR") failed = true;
        const state = result.billing?.replayed
          ? "REPLAYED"
          : (result.billing?.state ?? result.status);
        counts[state] = (counts[state] ?? 0) + 1;
      }
      pending = retry;
      if (pending.length && Date.now() < deadline) {
        await Bun.sleep(backoff + Math.random() * 5);
        backoff = Math.min(50, backoff * 2);
      } else break;
    } while (pending.length);
    if (failed || pending.length)
      throw new GatewayError(
        503,
        "usage_commit_failed",
        "One or more usage or financial effects require retry; the durable job must be retained",
      );
    return counts;
  }
  async tokenSummary(requestId: string): Promise<TokenSummary> {
    const rows = await this.db<
      { result: { complete: boolean; tokens: string } }[]
    >`SELECT gateway_token_summary(${requestId}::uuid) AS result`;
    const row = rows[0]?.result;
    const tokens = Number(row?.tokens ?? 0);
    return { complete: !!row?.complete && Number.isSafeInteger(tokens) && tokens >= 0, tokens };
  }
  async usageDimensions(attemptId: string): Promise<(keyof Usage)[]> {
    const rows = await this.db<{ dimension: keyof Usage }[]>`
      SELECT jsonb_object_keys(v.rates) AS dimension FROM attempts a JOIN cost_versions v ON v.id=a.pricing_id WHERE a.id=${attemptId}
      UNION SELECT jsonb_object_keys(p.rates) FROM attempts a JOIN billing_prices p ON p.id=a.billing_pricing_id WHERE a.id=${attemptId} AND p.basis='PROVIDER_USAGE'`;
    return rows.map((row) => row.dimension).filter((dimension) => dimension !== "requests");
  }
  async usage(
    projectId: string | null,
    limit = 100,
    before?: string,
    models: string[] = [],
    operations: Operation[] = [],
  ): Promise<UsageRecord[]> {
    if (
      before !== undefined &&
      (!/^[1-9][0-9]{0,18}$/.test(before) || BigInt(before) > 9223372036854775807n)
    )
      throw new GatewayError(400, "invalid_cursor", "Usage cursor must be a positive bigint");
    return this.db<
      UsageRecord[]
    >`SELECT id,seq::text,project_id,request_id,attempt_id,source,CASE record_kind WHEN 'DISPUTED' THEN 'disputed_snapshot' ELSE 'attempt_snapshot' END AS semantics,snapshot_revision::text,(SELECT c.revision::text FROM upstream_costs c WHERE c.evidence_id=usage_events.id) AS cost_revision,finality,usage,cost_atoms::text,currency,cost_complete
      FROM usage_events WHERE
      EXISTS(SELECT 1 FROM gateway_requests q WHERE q.id=usage_events.request_id AND (${models.length === 0} OR q.model=ANY(${models})) AND (${operations.length === 0} OR q.operation=ANY(${operations}))) AND (${projectId}::uuid IS NULL OR project_id=${projectId})
      AND (${before ?? null}::bigint IS NULL OR seq<${before ?? null}::bigint) ORDER BY seq DESC LIMIT ${limit}`;
  }
  async updateProject(
    id: string,
    patch: { [K in keyof ProjectInput]?: ProjectInput[K] | undefined } & {
      enabled?: boolean | undefined;
    },
  ): Promise<void> {
    const rows = await this
      .db`UPDATE projects SET name=coalesce(${patch.name ?? null},name),enabled=coalesce(${patch.enabled ?? null},enabled),rpm=coalesce(${patch.rpm ?? null},rpm),tpm=coalesce(${patch.tpm ?? null},tpm),concurrency=coalesce(${patch.concurrency ?? null},concurrency),billing_currency=coalesce(${patch.billing_currency ?? null},billing_currency) WHERE id=${id} RETURNING id`;
    if (!rows.length) throw new GatewayError(404, "not_found", "Project not found");
  }
  async updateProvider(
    id: string,
    patch: {
      enabled?: boolean | undefined;
      rpm?: number | undefined;
      concurrency?: number | undefined;
    },
    secret?: string,
  ): Promise<void> {
    const rows = await this
      .db`UPDATE channels SET enabled=coalesce(${patch.enabled ?? null},enabled),rpm=coalesce(${patch.rpm ?? null},rpm),concurrency=coalesce(${patch.concurrency ?? null},concurrency),secret=coalesce(${secret ?? null},secret) WHERE id=${id} RETURNING id`;
    if (!rows.length) throw new GatewayError(404, "not_found", "Provider not found");
    this.routeCache.clear();
    this.routeLoads.clear();
  }
  async updateModel(
    name: string,
    patch: {
      enabled?: boolean | undefined;
      operations?: Operation[] | undefined;
      max_output_tokens?: number | undefined;
      routing_strategy?: string | undefined;
    },
  ): Promise<void> {
    const rows = await this
      .db`UPDATE models SET enabled=coalesce(${patch.enabled ?? null},enabled),operations=coalesce(${patch.operations ?? null}::text[],operations),max_output_tokens=coalesce(${patch.max_output_tokens ?? null},max_output_tokens),routing_strategy=coalesce(${patch.routing_strategy ?? null},routing_strategy) WHERE name=${name} RETURNING name`;
    if (!rows.length) throw new GatewayError(404, "not_found", "Model not found");
    this.modelCache.delete(name);
    this.modelLoads.delete(name);
    this.routeCache.clear();
    this.routeLoads.clear();
  }
  async adminModels(): Promise<unknown[]> {
    return this
      .db`SELECT name,enabled,operations,max_output_tokens,routing_strategy FROM models ORDER BY name LIMIT 2000`;
  }
  async routeDefinitions(model: string): Promise<unknown[]> {
    return this
      .db`SELECT r.*,jsonb_build_object('currency',v.currency,'rates',v.rates) AS pricing,v.ceilings FROM routes r JOIN cost_versions v ON v.id=r.pricing_id WHERE model_name=${model} ORDER BY priority DESC,id LIMIT 2000`;
  }
  async updateRoute(
    id: string,
    patch: {
      enabled?: boolean | undefined;
      priority?: number | undefined;
      weight?: number | undefined;
      pricing?: Pricing | undefined;
      ceilings?: Usage | undefined;
      request_policy?: RequestPolicy | undefined;
    },
  ): Promise<void> {
    await this.db.begin((tx) => recordRouteUpdate(tx, id, patch));
    this.routeCache.clear();
    this.routeLoads.clear();
  }
  async updateCredential(
    id: string,
    patch: {
      models?: string[] | undefined;
      operations?: Operation[] | undefined;
      expires_at?: string | undefined;
      allowed_cidrs?: string[] | undefined;
      role?: string | undefined;
      rpm?: number | undefined;
      tpm?: number | undefined;
      concurrency?: number | undefined;
    },
  ): Promise<void> {
    const rows = await this
      .db`UPDATE api_keys SET models=coalesce(${patch.models ?? null}::text[],models),operations=coalesce(${patch.operations ?? null}::text[],operations),expires_at=coalesce(${patch.expires_at ?? null}::timestamptz,expires_at),allowed_cidrs=coalesce(${patch.allowed_cidrs ?? null}::text[],allowed_cidrs),role=coalesce(${patch.role ?? null},role),rpm=coalesce(${patch.rpm ?? null},rpm),tpm=coalesce(${patch.tpm ?? null},tpm),concurrency=coalesce(${patch.concurrency ?? null},concurrency) WHERE id=${id} AND enabled RETURNING id`;
    if (!rows.length) throw new GatewayError(404, "not_found", "Active credential not found");
  }
  async createConsumer(name: string, replay: boolean): Promise<string> {
    const id = crypto.randomUUID();
    await this.db.begin(async (tx) => {
      await tx`SELECT pg_advisory_xact_lock(81442592)`;
      await tx`INSERT INTO event_consumers(id,name,replay_until) VALUES(${id},${name},${replay}::boolean::integer*(SELECT coalesce(max(seq),0) FROM usage_events))`;
    });
    return id;
  }
  async backfillConsumer(consumerId: string): Promise<void> {
    const cursor = await this.db<{ complete: boolean }[]>`
      SELECT replay_cursor>=replay_until AS complete FROM event_consumers WHERE id=${consumerId}`;
    if (!cursor[0] || cursor[0].complete) return;
    await this
      .db`WITH owned AS (SELECT id,replay_cursor,replay_until FROM event_consumers WHERE id=${consumerId} AND replay_cursor<replay_until FOR UPDATE SKIP LOCKED),
    page AS (SELECT e.id,e.seq FROM usage_events e,owned o WHERE e.seq>o.replay_cursor AND e.seq<=o.replay_until ORDER BY e.seq LIMIT 1000),
    inserted AS (INSERT INTO event_deliveries(consumer_id,event_id) SELECT ${consumerId},id FROM page ON CONFLICT DO NOTHING RETURNING event_id)
    UPDATE event_consumers c SET replay_cursor=coalesce((SELECT max(seq) FROM page),o.replay_until) FROM owned o WHERE c.id=o.id`;
  }
  async claimEvents(
    consumerId: string,
    limit: number,
  ): Promise<{ lease_token: string; events: UsageRecord[] }> {
    await this.backfillConsumer(consumerId);
    const token = crypto.randomUUID();
    const events = await this.db<UsageRecord[]>`
      WITH selected AS (SELECT consumer_id,event_id FROM event_deliveries
        WHERE consumer_id=${consumerId} AND state<>'ACKED' AND (lease_until IS NULL OR lease_until<now())
        ORDER BY event_id LIMIT ${limit} FOR UPDATE SKIP LOCKED), claimed AS (
        UPDATE event_deliveries d SET state='LEASED',lease_token=${token},lease_until=now()+interval '30 seconds',attempts=d.attempts+1
        FROM selected s WHERE d.consumer_id=s.consumer_id AND d.event_id=s.event_id RETURNING d.event_id)
      SELECT e.id,e.seq::text,e.project_id,e.request_id,e.attempt_id,e.source,CASE e.record_kind WHEN 'DISPUTED' THEN 'disputed_snapshot' ELSE 'attempt_snapshot' END AS semantics,e.snapshot_revision::text,(SELECT c.revision::text FROM upstream_costs c WHERE c.evidence_id=e.id) AS cost_revision,e.finality,e.usage,e.cost_atoms::text,e.currency,e.cost_complete
      FROM usage_events e JOIN claimed c ON e.id=c.event_id`;
    return { lease_token: token, events };
  }
  async ackEvents(consumerId: string, token: string, ids: string[]): Promise<number> {
    const rows = await this
      .db`UPDATE event_deliveries SET state='ACKED',lease_until=NULL WHERE consumer_id=${consumerId}
      AND lease_token=${token} AND event_id=ANY(${ids}) AND state='LEASED' AND lease_until>now() RETURNING event_id`;
    return rows.length;
  }
  async saveResource(
    projectId: string,
    providerId: string,
    remoteId: string,
    kind: string,
    model: string | null,
    attemptId: string | null = null,
    preferredId?: string,
    batch?: BatchResourceMetadata,
  ): Promise<string> {
    if (
      !opaqueResourceIdSchema.safeParse(remoteId).success ||
      (batch !== undefined && !batchResourceMetadataSchema.safeParse(batch).success)
    )
      throw new GatewayError(
        502,
        "invalid_upstream",
        "Invalid provider resource identity or metadata",
      );
    const id = preferredId ?? `${kind}_${crypto.randomUUID().replaceAll("-", "")}`;
    const rows = await this.db<
      { id: string }[]
    >`INSERT INTO resources(id,project_id,provider_id,remote_id,kind,model,attempt_id,batch_endpoint,batch_upstream_model,batch_manifest)
      VALUES(${id},${projectId},${providerId},${remoteId},${kind},${model},${attemptId},${batch?.endpoint ?? null},${batch?.upstreamModel ?? null},${batch?.manifest ?? null}::jsonb)
      ON CONFLICT(project_id,provider_id,kind,remote_id) DO UPDATE SET updated_at=resources.updated_at RETURNING id`;
    const row = rows[0];
    if (!row) throw new Error("Resource insert failed");
    return row.id;
  }
  async resource(id: string, projectId: string, kind?: string): Promise<Resource> {
    const rows = await this.db<
      Resource[]
    >`SELECT * FROM resources WHERE id=${id} AND project_id=${projectId}
      AND state<>'DELETED' AND (${kind ?? null}::text IS NULL OR kind=${kind ?? null})`;
    if (!rows[0]) throw new GatewayError(404, "not_found", "Resource not found");
    return rows[0];
  }
  async resources(
    projectId: string,
    kind: string,
    models: string[] = [],
    limit = 100,
    after?: string,
  ): Promise<Resource[]> {
    if (after) {
      const cursor = await this
        .db`SELECT id FROM resources WHERE id=${after} AND project_id=${projectId} AND kind=${kind} AND (${models.length === 0} OR model=ANY(${models}))`;
      if (!cursor.length) throw new GatewayError(404, "not_found", "Resource cursor not found");
    }
    return this.db<
      Resource[]
    >`SELECT * FROM resources r WHERE project_id=${projectId} AND kind=${kind} AND state<>'DELETED'
      AND (${models.length === 0} OR model=ANY(${models}))
      AND (${after ?? null}::text IS NULL OR (created_at,id)<(SELECT created_at,id FROM resources WHERE id=${after ?? null}))
      ORDER BY created_at DESC,id DESC LIMIT ${limit + 1}`;
  }
  async updateResource(id: string, state: string, upstreamState?: string): Promise<void> {
    await this
      .db`UPDATE resources SET state=${state},upstream_state=coalesce(${upstreamState ?? null},upstream_state),updated_at=now()
      WHERE id=${id} AND state<>'DELETED' AND (kind<>'batch' OR state NOT IN ('COMPLETED','CANCELLED','FAILED') OR ${state}='DELETED')`;
  }
  async audit(action: string, resourceId: string): Promise<void> {
    await this
      .db`INSERT INTO audit_events(id,action,resource_id) VALUES(${crypto.randomUUID()},${action},${sha256(resourceId)})`;
  }
  async staleAttempts(ageSeconds: number): Promise<{ id: string }[]> {
    return this.db<
      { id: string }[]
    >`SELECT id FROM attempts WHERE state='DISPATCHING' AND started_at<now()-${ageSeconds}*interval '1 second' LIMIT 100`;
  }
  async claimResources(limit: number): Promise<{ token: string; resources: Resource[] }> {
    const token = crypto.randomUUID();
    const resources = await this.db<
      Resource[]
    >`WITH due AS (SELECT id FROM resources WHERE state IN ('ACTIVE','PENDING_USAGE')
      AND kind IN ('batch','video','response','operation') AND next_poll_at<=now() AND (poll_until IS NULL OR poll_until<now())
      AND (attempt_id IS NULL OR EXISTS(SELECT 1 FROM attempts a WHERE a.id=resources.attempt_id AND a.state<>'DISPATCHING'))
      ORDER BY next_poll_at LIMIT ${limit} FOR UPDATE SKIP LOCKED)
      UPDATE resources r SET poll_token=${token},poll_until=now()+interval '30 seconds'
      FROM due WHERE r.id=due.id RETURNING r.*`;
    return { token, resources };
  }
  async completePoll(id: string, token: string, failed: boolean): Promise<void> {
    await this
      .db`UPDATE resources SET poll_until=NULL,poll_count=poll_count+1,next_poll_at=now()+least(3600,greatest(${failed ? 30 : 5},power(2,least(poll_count,11))::integer))*interval '1 second'
      WHERE id=${id} AND poll_token=${token}`;
  }
}

export async function recordProvider(
  tx: SQL,
  id: string,
  input: ProviderInput,
  secret: string,
  existing?: { account_id: string; brand_id: string },
) {
  if (existing) {
    const brand = (
      await tx`SELECT id,name,enabled FROM supplier_brands WHERE id=${existing.brand_id} FOR SHARE`
    )[0];
    const account = (
      await tx`SELECT id,brand_id,mode FROM upstream_accounts WHERE id=${existing.account_id} FOR SHARE`
    )[0];
    if (
      !brand ||
      !account ||
      !brand.enabled ||
      brand.name !== input.kind ||
      account.brand_id !== brand.id ||
      account.mode !== "PREPAID"
    )
      throw new GatewayError(403, "provider_account_mismatch", "Provider account binding denied");
    await tx`INSERT INTO channels(id,name,kind,base_url,secret,concurrency,rpm,brand_id,account_id) VALUES(${id},${input.name},${input.kind},${input.base_url},${secret},${input.concurrency},${input.rpm},${brand.id},${account.id})`;
    return {
      id,
      brand_id: brand.id as string,
      account_id: account.id as string,
      config_revision: "0",
      binding_id: null,
      funded: false,
    };
  }
  const brandId = crypto.randomUUID();
  const accountId = crypto.randomUUID();
  const brands =
    await tx`INSERT INTO supplier_brands(id,name) VALUES(${brandId},${input.kind}) ON CONFLICT(name) DO UPDATE SET name=excluded.name RETURNING id`;
  await tx`INSERT INTO upstream_accounts(id,brand_id,name) VALUES(${accountId},${brands[0].id},${input.name})`;
  await tx`INSERT INTO channels(id,name,kind,base_url,secret,concurrency,rpm,brand_id,account_id)
        VALUES(${id},${input.name},${input.kind},${input.base_url},${secret},${input.concurrency},${input.rpm},${brands[0].id},${accountId})`;
  return {
    id,
    brand_id: brands[0].id as string,
    account_id: accountId,
    config_revision: "0",
    binding_id: null,
    funded: false,
  };
}

export async function recordRoute(tx: SQL, model: string, input: RouteInput) {
  upstreamCostConfigurationSchema.parse({ pricing: input.pricing, ceilings: input.ceilings });
  const id = crypto.randomUUID(),
    pricingId = crypto.randomUUID();
  await tx`INSERT INTO cost_versions(id,currency,rates,ceilings) VALUES(${pricingId},${input.pricing.currency},${input.pricing.rates},${input.ceilings})`;
  await tx`INSERT INTO routes(id,model_name,provider_id,upstream_model,operations,priority,weight,pricing_id)
        VALUES(${id},${model},${input.provider_id},${input.upstream_model},${[...new Set(input.operations)].sort()},${input.priority},${input.weight},${pricingId})`;
  if (input.request_policy)
    await tx`UPDATE routes SET request_policy=${input.request_policy} WHERE id=${id}`;

  return id;
}

export async function recordRouteUpdate(
  tx: SQL,
  id: string,
  patch: Parameters<Store["updateRoute"]>[1],
): Promise<void> {
  if (patch.pricing !== undefined || patch.ceilings !== undefined)
    upstreamCostConfigurationSchema.parse({ pricing: patch.pricing, ceilings: patch.ceilings });

  const pricingId = patch.pricing ? crypto.randomUUID() : null;
  if (pricingId && patch.pricing)
    await tx`INSERT INTO cost_versions(id,currency,rates,ceilings) VALUES(${pricingId},${patch.pricing.currency},${patch.pricing.rates},${patch.ceilings})`;
  const rows =
    await tx`UPDATE routes SET enabled=coalesce(${patch.enabled ?? null},enabled),priority=coalesce(${patch.priority ?? null},priority),weight=coalesce(${patch.weight ?? null},weight),pricing_id=coalesce(${pricingId}::uuid,pricing_id),request_policy=coalesce(${patch.request_policy ?? null}::jsonb,request_policy) WHERE id=${id} RETURNING id`;
  if (!rows.length) throw new GatewayError(404, "not_found", "Route not found");
}
