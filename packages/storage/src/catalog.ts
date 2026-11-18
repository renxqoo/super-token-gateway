import type { SQL } from "bun";
import { createEventStore, type UsageRecord } from "./events.ts";
import { upstreamCostConfigurationSchema, GatewayError } from "@gateway/contracts";
import type { Operation, Pricing, Usage } from "@gateway/kernel";
import type { Credential, Project, RequestPolicy } from "@gateway/contracts";
import { runDatabaseQuery, withDatabaseTransaction } from "./transaction.ts";

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
type RoutePatch = {
  enabled?: boolean | undefined;
  priority?: number | undefined;
  weight?: number | undefined;
  pricing?: Pricing | undefined;
  ceilings?: Usage | undefined;
  request_policy?: RequestPolicy | undefined;
};

export function createCatalogStore(db: SQL) {
  async function createCredential(
    projectId: string,
    input: KeyInput,
    hash: string,
    prefix: string,
    connection: SQL = db,
  ): Promise<string> {
    const id = crypto.randomUUID();
    await connection`INSERT INTO api_keys(id,project_id,name,verifier,prefix,models,operations,expires_at,principal_id,role,allowed_cidrs,rpm,tpm,concurrency)
      VALUES(${id},${projectId},${input.name},${hash},${prefix},${input.models},${input.operations},${input.expires_at ?? null},${input.principal_id ?? null},${input.role ?? "INVOKER"},${input.allowed_cidrs ?? []},${input.rpm ?? null},${input.tpm ?? null},${input.concurrency ?? null})`;
    return id;
  }
  return {
    async createOrganization(input: {
      external_ref: string;
      name: string;
      rpm?: number | undefined;
      tpm?: number | undefined;
      concurrency?: number | undefined;
    }): Promise<string> {
      const id = crypto.randomUUID();
      await db`INSERT INTO organizations(id,external_ref,name,rpm,tpm,concurrency) VALUES(${id},${input.external_ref},${input.name},${input.rpm ?? 100000},${input.tpm ?? 1000000000},${input.concurrency ?? 10000})`;
      return id;
    },
    async updateOrganization(
      id: string,
      input: {
        enabled?: boolean | undefined;
        rpm?: number | undefined;
        tpm?: number | undefined;
        concurrency?: number | undefined;
      },
    ): Promise<void> {
      const rows =
        await db`UPDATE organizations SET enabled=coalesce(${input.enabled ?? null},enabled),rpm=coalesce(${input.rpm ?? null},rpm),tpm=coalesce(${input.tpm ?? null},tpm),concurrency=coalesce(${input.concurrency ?? null},concurrency) WHERE id=${id} RETURNING id`;
      if (!rows.length) throw new GatewayError(404, "not_found", "Organization not found");
    },
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
      await db`INSERT INTO principals(id,organization_id,external_ref,role,rpm,tpm,concurrency) VALUES(${id},${organizationId},${input.external_ref},${input.role},${input.rpm ?? 10000},${input.tpm ?? 100000000},${input.concurrency ?? 1000})`;
      return id;
    },
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
      const rows =
        await db`UPDATE principals SET enabled=coalesce(${input.enabled ?? null},enabled),role=coalesce(${input.role ?? null},role),rpm=coalesce(${input.rpm ?? null},rpm),tpm=coalesce(${input.tpm ?? null},tpm),concurrency=coalesce(${input.concurrency ?? null},concurrency) WHERE id=${id} RETURNING id`;
      if (!rows.length) throw new GatewayError(404, "not_found", "Principal not found");
    },
    async revokeProjectKey(id: string, projectId: string): Promise<void> {
      const rows =
        await db`UPDATE api_keys SET enabled=false WHERE id=${id} AND project_id=${projectId} RETURNING id`;
      if (!rows.length) throw new GatewayError(404, "not_found", "Key not found");
    },
    async createProject(input: ProjectInput): Promise<string> {
      const id = crypto.randomUUID();
      await db`INSERT INTO projects(id,name,external_ref,rpm,tpm,concurrency,billing_currency,organization_id)
        VALUES(${id},${input.name},${input.external_ref ?? null},${input.rpm},${input.tpm},${input.concurrency},${input.billing_currency ?? "USD"},${input.organization_id ?? null})`;
      return id;
    },
    async projects(): Promise<Project[]> {
      return db<Project[]>`SELECT * FROM projects ORDER BY created_at DESC LIMIT 200`;
    },
    async updateProject(
      id: string,
      patch: { [K in keyof ProjectInput]?: ProjectInput[K] | undefined } & {
        enabled?: boolean | undefined;
      },
    ): Promise<void> {
      const rows =
        await db`UPDATE projects SET name=coalesce(${patch.name ?? null},name),enabled=coalesce(${patch.enabled ?? null},enabled),rpm=coalesce(${patch.rpm ?? null},rpm),tpm=coalesce(${patch.tpm ?? null},tpm),concurrency=coalesce(${patch.concurrency ?? null},concurrency),billing_currency=coalesce(${patch.billing_currency ?? null},billing_currency) WHERE id=${id} RETURNING id`;
      if (!rows.length) throw new GatewayError(404, "not_found", "Project not found");
    },
    async keys(projectId: string): Promise<unknown[]> {
      return db`SELECT id,project_id,name,prefix,models,operations,enabled,expires_at,created_at,principal_id,role,allowed_cidrs,rpm,tpm,concurrency FROM api_keys WHERE project_id=${projectId} ORDER BY created_at DESC LIMIT 200`;
    },
    async revokeKey(id: string): Promise<void> {
      await db`UPDATE api_keys SET enabled=false WHERE id=${id}`;
    },
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
      const rows =
        await db`UPDATE api_keys SET models=coalesce(${patch.models ?? null}::text[],models),operations=coalesce(${patch.operations ?? null}::text[],operations),expires_at=coalesce(${patch.expires_at ?? null}::timestamptz,expires_at),allowed_cidrs=coalesce(${patch.allowed_cidrs ?? null}::text[],allowed_cidrs),role=coalesce(${patch.role ?? null},role),rpm=coalesce(${patch.rpm ?? null},rpm),tpm=coalesce(${patch.tpm ?? null},tpm),concurrency=coalesce(${patch.concurrency ?? null},concurrency) WHERE id=${id} AND enabled RETURNING id`;
      if (!rows.length) throw new GatewayError(404, "not_found", "Active credential not found");
    },
    async delegateCredential(
      parentId: string,
      requested: KeyInput,
      explicitOperations: boolean,
      hash: string,
      prefix: string,
    ): Promise<{ id: string; input: KeyInput }> {
      return db.begin(async (tx) => {
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
        if (input.operations.some((operation) => !authority.operations.includes(operation)))
          denied();
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
        const id = await createCredential(
          authority.project_id,
          input,
          hash,
          prefix,
          tx as unknown as SQL,
        );
        return { id, input };
      });
    },
    createProvider(id: string, input: ProviderInput, secret: string) {
      return db.begin((tx) => recordProvider(tx, id, input, secret));
    },
    async providers(): Promise<unknown[]> {
      return db`SELECT id,name,kind,base_url,enabled,concurrency,rpm,brand_id,account_id,binding_id,config_revision::text FROM channels ORDER BY name LIMIT 500`;
    },
    async updateProvider(
      id: string,
      patch: {
        enabled?: boolean | undefined;
        rpm?: number | undefined;
        concurrency?: number | undefined;
      },
      secret?: string,
    ): Promise<void> {
      const rows =
        await db`UPDATE channels SET enabled=coalesce(${patch.enabled ?? null},enabled),rpm=coalesce(${patch.rpm ?? null},rpm),concurrency=coalesce(${patch.concurrency ?? null},concurrency),secret=coalesce(${secret ?? null},secret) WHERE id=${id} RETURNING id`;
      if (!rows.length) throw new GatewayError(404, "not_found", "Provider not found");
    },
    async createModel(input: {
      name: string;
      operations: Operation[];
      max_output_tokens: number;
      routing_strategy?: string;
    }): Promise<void> {
      await db`INSERT INTO models(name,operations,max_output_tokens,routing_strategy) VALUES(${input.name},${input.operations},${input.max_output_tokens},${input.routing_strategy ?? "WEIGHTED"})`;
    },
    async models(allowed: string[] = []): Promise<
      {
        name: string;
        operations: Operation[];
        max_output_tokens: number;
        routing_strategy: string;
      }[]
    > {
      return db`SELECT name,operations,max_output_tokens,routing_strategy FROM models WHERE enabled AND (${allowed.length === 0} OR name=ANY(${allowed})) ORDER BY name LIMIT 2000`;
    },
    async adminModels(): Promise<unknown[]> {
      return db`SELECT name,enabled,operations,max_output_tokens,routing_strategy FROM models ORDER BY name LIMIT 2000`;
    },
    async updateModel(
      name: string,
      patch: {
        enabled?: boolean | undefined;
        operations?: Operation[] | undefined;
        max_output_tokens?: number | undefined;
        routing_strategy?: string | undefined;
      },
    ): Promise<void> {
      const rows =
        await db`UPDATE models SET enabled=coalesce(${patch.enabled ?? null},enabled),operations=coalesce(${patch.operations ?? null}::text[],operations),max_output_tokens=coalesce(${patch.max_output_tokens ?? null},max_output_tokens),routing_strategy=coalesce(${patch.routing_strategy ?? null},routing_strategy) WHERE name=${name} RETURNING name`;
      if (!rows.length) throw new GatewayError(404, "not_found", "Model not found");
    },
    createRoute(model: string, input: RouteInput): Promise<string> {
      return withDatabaseTransaction(
        db,
        { deadline: performance.now() + 4000 },
        async (tx, signal) => {
          await runDatabaseQuery(tx`SELECT core_catalog_write_v1()`, signal);
          return recordRoute(tx, model, input);
        },
      );
    },
    async routeDefinitions(model: string): Promise<unknown[]> {
      return db`SELECT r.*,jsonb_build_object('currency',v.currency,'rates',v.rates) AS pricing,v.ceilings FROM routes r JOIN cost_versions v ON v.id=r.pricing_id WHERE model_name=${model} ORDER BY priority DESC,id LIMIT 2000`;
    },
    async request(
      id: string,
      projectId: string,
      models: string[] = [],
      operations: Operation[] = [],
    ): Promise<unknown> {
      const rows =
        await db`SELECT id,operation,model,state,created_at,updated_at FROM gateway_requests WHERE id=${id} AND project_id=${projectId}
        AND (${models.length === 0} OR model=ANY(${models})) AND (${operations.length === 0} OR operation=ANY(${operations}))`;
      if (!rows[0]) throw new GatewayError(404, "not_found", "Request not found");
      const attempts =
        await db`SELECT id,ordinal,state,error_code,started_at,finished_at FROM attempts WHERE request_id=${id} ORDER BY ordinal`;
      const usage =
        await db`SELECT attempt_id,source,CASE record_kind WHEN 'DISPUTED' THEN 'disputed_snapshot' ELSE 'attempt_snapshot' END AS semantics,snapshot_revision::text,(SELECT c.revision::text FROM upstream_costs c WHERE c.evidence_id=usage_events.id) AS cost_revision,finality,usage,cost_atoms::text,currency,cost_complete FROM usage_events WHERE request_id=${id} ORDER BY seq`;
      return { ...rows[0], attempts, usage };
    },
    usage(
      projectId: string | null,
      limit = 100,
      before?: string,
      models: string[] = [],
      operations: Operation[] = [],
    ): Promise<UsageRecord[]> {
      return createEventStore(db).usage({ projectId, limit, before, models, operations });
    },
    createCredential,
  };
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

export async function recordRouteUpdate(tx: SQL, id: string, patch: RoutePatch): Promise<void> {
  if (patch.pricing !== undefined || patch.ceilings !== undefined)
    upstreamCostConfigurationSchema.parse({ pricing: patch.pricing, ceilings: patch.ceilings });

  const pricingId = patch.pricing ? crypto.randomUUID() : null;
  if (pricingId && patch.pricing)
    await tx`INSERT INTO cost_versions(id,currency,rates,ceilings) VALUES(${pricingId},${patch.pricing.currency},${patch.pricing.rates},${patch.ceilings})`;
  const rows =
    await tx`UPDATE routes SET enabled=coalesce(${patch.enabled ?? null},enabled),priority=coalesce(${patch.priority ?? null},priority),weight=coalesce(${patch.weight ?? null},weight),pricing_id=coalesce(${pricingId}::uuid,pricing_id),request_policy=coalesce(${patch.request_policy ?? null}::jsonb,request_policy) WHERE id=${id} RETURNING id`;
  if (!rows.length) throw new GatewayError(404, "not_found", "Route not found");
}
