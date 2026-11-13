import { z } from "zod";
import {
  prepBindingV1,
  dispatchProjectionV1Schema,
  executionContextResponseSchema,
  type ExecutionReceipt,
  type ExecutionReply,
  type UpstreamRechargeReceipt,
} from "@gateway/contracts";
import type { SQL } from "bun";
import { createCoreRpcClient } from "@gateway/core-client";
import { canonical } from "@gateway/kernel";
import { sha256 } from "@gateway/runtime/crypto";
import { withIdentity, identityAudience } from "./internal-identity.ts";
import { prepareCatalog, syntheticAdminKey, syntheticPepper } from "./catalog.ts";
import { createCoreManagementApplication } from "../../apps/core-api/src/platform/management.ts";
import { bundledReleaseId } from "../../apps/core-api/src/plugins/releases.ts";
export const financialKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
export async function withFinancial(
  run: (fixture: Awaited<ReturnType<typeof prepareFinancial>>, db: SQL) => Promise<void>,
  options: {
    entry?: string;
    databasePort?: number;
    limits?: Partial<import("@gateway/runtime/h2").H2Limits>;
    managementFactory?: typeof createCoreManagementApplication;
  } = {},
) {
  const namespace = `financial_${crypto.randomUUID().replaceAll("-", "")}`;
  await withIdentity(
    async (f, db) => {
      const catalog = await prepareCatalog(db);
      let fixture: Awaited<ReturnType<typeof prepareFinancial>> | undefined;
      try {
        const role = String((await catalog.appDb`SELECT current_user AS name`)[0].name);
        await db.unsafe(
          (await Bun.file("infra/core-management-role.sql").text()).replaceAll(
            "core_management_api",
            role,
          ),
        );
        const management = (options.managementFactory ?? createCoreManagementApplication)({
          db: catalog.appDb,
          adminKey: syntheticAdminKey,
          pepper: syntheticPepper,
          encryptionKey: financialKey,
        });
        fixture = await prepareFinancial(f, db, catalog, management, namespace);
        await run(fixture, db);
      } finally {
        try {
          await fixture?.close();
        } finally {
          await catalog.close();
        }
      }
    },
    {
      ...(options.databasePort ? { databasePort: options.databasePort } : {}),
      ...(options.entry ? { entry: options.entry } : {}),
      ...(options.limits ? { limits: options.limits } : {}),
      execution: {
        pepper: syntheticPepper,
        encryptionKey: financialKey,
        redisUrls: [process.env.REDIS_URL!],
        quotaNamespace: namespace,
        quotaGeneration: "v1",
        maxAttempts: 3,
      },
    },
  );
}
async function prepareFinancial(
  f: Parameters<Parameters<typeof withIdentity>[0]>[0],
  db: SQL,
  catalog: Awaited<ReturnType<typeof prepareCatalog>>,
  management: ReturnType<typeof createCoreManagementApplication>,
  namespace: string,
) {
  const clients: ReturnType<typeof createCoreRpcClient>[] = [];
  function client(port = f.a.port, name = "gateway", options: { timeoutMs?: number } = {}) {
    const cert = f.certificates.get(name)!,
      core = f.certificates.get(port === f.a.port ? "coreA" : "coreB")!;
    const c = createCoreRpcClient({
      origin: `https://127.0.0.1:${port}`,
      servername: "localhost",
      key: cert.key,
      cert: cert.cert,
      ca: f.ca,
      expectedPeerSha256: sha256(core.x509.raw),
      expectedSubjectAltname: core.x509.subjectAltName!,
      audience: identityAudience,
      ...options,
    });
    clients.push(c);
    return c;
  }
  async function request(
    path: string,
    body?: unknown,
    token = syntheticAdminKey,
    method = body ? "POST" : "GET",
  ) {
    const response = await management.request(`http://management.test${path}`, {
      method,
      headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
    return { status: response.status, value: (await response.json()) as Record<string, unknown> };
  }
  return {
    identity: f,
    db,
    namespace,
    catalog,
    client,
    request,
    async bootstrap(
      currency = "USD",
      creditedAtoms = "1000",
      modelOutput = 100,
      customerBasis: "REQUEST_COMPLETION" | "PROVIDER_USAGE" = "REQUEST_COMPLETION",
      supplier: {
        rates: Record<string, { numerator: string; denominator: string }>;
        ceilings: Record<string, string>;
      } = {
        rates: {
          input_tokens: { numerator: "1", denominator: "3" },
          output_tokens: { numerator: "1", denominator: "3" },
        },
        ceilings: { input_tokens: "100", output_tokens: "50" },
      },
    ) {
      const uuid = crypto.randomUUID(),
        model = `financial-${uuid}`;
      const providerResponse = await request("/providers", {
        name: model,
        kind: "openai",
        base_url: "https://synthetic.provider.test/v1",
        api_key: "synthetic-upstream-secret",
        concurrency: 10,
        rpm: 100,
      });
      if (providerResponse.status !== 201)
        throw new Error(`Provider creation: ${JSON.stringify(providerResponse)}`);
      const provider = {
        ...providerResponse,
        value: financialProviderSchema.parse(providerResponse.value),
      };
      await request("/models", {
        name: model,
        operations: ["chat"],
        max_output_tokens: modelOutput,
      });
      const route = await request(`/models/${model}/routes`, {
        provider_id: provider.value.id,
        upstream_model: "synthetic",
        operations: ["chat"],
        pricing: { currency, rates: supplier.rates },
        ceilings: supplier.ceilings,
      });
      if (route.status !== 201) throw new Error(`Route creation: ${JSON.stringify(route)}`);
      const tariff = await request("/billing/tariffs", {
        model,
        operation: "chat",
        provider_id: provider.value.id,
        ...(customerBasis === "PROVIDER_USAGE"
          ? {
              pricing: {
                currency: "USD",
                rates: {
                  input_tokens: { numerator: "1", denominator: "1" },
                  output_tokens: { numerator: "2", denominator: "1" },
                },
              },
              ceilings: { input_tokens: "100", output_tokens: "50" },
            }
          : {
              pricing: {
                currency: "USD",
                rates: { requests: { numerator: "100", denominator: "1" } },
              },
              ceilings: { requests: "1" },
            }),
        basis: customerBasis,
      });
      if (tariff.status !== 201) throw new Error(`Tariff creation: ${JSON.stringify(tariff)}`);
      const publication = await catalog.application.installBundled(await catalog.actor(), {
        command_id: crypto.randomUUID(),
        release_id: bundledReleaseId,
        reason: "Synthetic financial prerequisite",
      });
      const binding = await catalog.application.bind(await catalog.actor(), provider.value.id, {
        ...catalog.binding(publication.version_id),
        account_id: provider.value.account_id,
        brand_id: provider.value.brand_id,
        secret_reference: `channel:${provider.value.id}`,
        config: {
          model_map: { [model]: "synthetic" },
          max_output_tokens: 100,
          max_input_tokens: 10000,
          auth: "bearer",
        },
      });
      const projectResponse = await request("/projects", {
        name: model,
        billing_currency: "USD",
        rpm: 1000,
        tpm: 10000000,
        concurrency: 100,
      });
      const project = {
        ...projectResponse,
        value: z.object({ id: z.uuid() }).parse(projectResponse.value),
      };
      const keyResponse = await request(`/projects/${project.value.id}/keys`, {
        name: "synthetic",
        models: [model],
        operations: ["chat"],
      });
      const key = {
        ...keyResponse,
        value: z.object({ id: z.uuid(), key: z.string() }).parse(keyResponse.value),
      };
      const credit = await request(`/projects/${project.value.id}/credits`, {
        external_ref: uuid,
        currency: "USD",
        amount_atoms: "1000",
        kind: "CASH",
      });
      if (credit.status !== 201) throw new Error(`Customer credit: ${JSON.stringify(credit)}`);
      const voucherBytes = Buffer.from("%PDF-synthetic-financial-fixture");
      const voucherResponse = await request("/financial-vouchers", {
        content_base64: voucherBytes.toString("base64"),
        content_type: "application/pdf",
        digest: sha256(voucherBytes),
        expires_at: new Date(Date.now() + 3600000).toISOString(),
      });
      if (voucherResponse.status !== 201)
        throw new Error(`Voucher: ${JSON.stringify(voucherResponse)}`);
      const voucher = {
        ...voucherResponse,
        value: z.object({ id: z.uuid(), digest: z.string() }).parse(voucherResponse.value),
      };
      const rechargeInput = {
        external_ref: uuid,
        payment_currency: "CNY",
        payment_atoms: "700",
        credited_currency: currency,
        credited_atoms: creditedAtoms,
        voucher_id: voucher.value.id,
        voucher_digest: voucher.value.digest,
        completed_at: new Date(Date.now() - 1000).toISOString(),
        reason: "Synthetic completed payment fact",
      };
      const recharge = await request(
        `/upstream-accounts/${provider.value.account_id}/recharges`,
        rechargeInput,
      );
      if (recharge.status !== 201) throw new Error(`Recharge: ${JSON.stringify(recharge)}`);
      return {
        model,
        provider: provider.value,
        route: route.value,
        project: project.value,
        key: key.value,
        publication,
        binding,
        recharge: recharge.value as unknown as UpstreamRechargeReceipt,
        rechargeInput,
      };
    },
    async close() {
      for (const c of clients) await c.close();
      const { RedisClient } = await import("bun");
      const redis = new RedisClient(process.env.REDIS_URL!);
      try {
        await redis.connect();
        const keys = (await redis.send("KEYS", [`${namespace}:*`])) as string[];
        for (const key of keys) await redis.send("DEL", [key]);
      } finally {
        redis.close();
      }
    },
  };
}

export async function prepareFinancialAdmission(
  setup: Awaited<ReturnType<Awaited<ReturnType<typeof prepareFinancial>>["bootstrap"]>>,
  client: Pick<ReturnType<typeof createCoreRpcClient>, "execution">,
  options: {
    deadline?: string;
    intent?: string | null;
    requestId?: string;
    attemptId?: string;
    ceilings?: Record<string, string>;
    quantities?: Record<string, string>;
    inputCharacters?: string;
    images?: string;
    channelId?: string;
    fingerprint?: string;
    resourceIds?: string[];
  } = {},
) {
  const deadline = options.deadline ?? new Date(Date.now() + 25000).toISOString(),
    fingerprint = options.fingerprint ?? sha256("synthetic-input");
  const context = executionContextResponseSchema.parse(
    await client.execution(
      "execution_context_v1",
      {
        customer_token: setup.key.key,
        ip: "127.0.0.1",
        operation: "chat",
        model: setup.model,
        fingerprint,
      },
      { deadlineAt: deadline },
    ),
  );
  const candidate = options.channelId
    ? context.candidates.find((c) => c.ref.channel_id === options.channelId)
    : context.candidates[0];
  if (!candidate) throw new Error("No candidate");
  const manifest = {
    version: 1,
    operation: "chat",
    model: setup.model,
    upstream_model: "synthetic",
    stream: false,
    input_bytes: 100,
    quantities: options.quantities ?? {},
    ...(options.inputCharacters === undefined ? {} : { input_characters: options.inputCharacters }),
    ...(options.images === undefined ? {} : { images: options.images }),
    resource_ids: options.resourceIds ?? [],
    ceilings: options.ceilings ?? { input_tokens: "60", output_tokens: "30" },
  };
  const projection = {
    version: 1,
    proposed_request_id: options.requestId ?? crypto.randomUUID(),
    proposed_attempt_id: options.attemptId ?? crypto.randomUUID(),
    origin: candidate.origin,
    method: "POST",
    operation: "chat",
    request_target_sha256: sha256("/v1/chat/completions"),
    final_headers_digest: sha256("synthetic-headers"),
    final_body_sha256: sha256("synthetic-input"),
    candidate_ref: candidate.ref,
    auth: { type: "bearer" },
    credential_ciphertext_sha256: candidate.credential_ciphertext_sha256,
    billable_manifest_sha256: sha256(canonical(["stg.internal.billable-manifest", "1", manifest])),
    deadline_at: deadline,
  };
  const prepId = crypto.randomUUID();
  const data = {
    context_id: context.context_id,
    dispatch_projection: projection,
    dispatched_digest: sha256(canonical(["stg.internal.dispatch-projection", "1", projection])),
    manifest,
    prep_session_id: prepId,
    prep_binding_digest: sha256(
      canonical([
        "stg.internal.prep-binding",
        "1",
        prepBindingV1({
          owner: context.owner,
          project_id: context.project_id,
          projection: dispatchProjectionV1Schema.parse(projection),
          prep_session_id: prepId,
          host_config_sha256: sha256(canonical(candidate.config)),
        }),
      ]),
    ),
    project_id: setup.project.id,
    key_id: setup.key.id,
    intent_digest: options.intent === undefined ? sha256(crypto.randomUUID()) : options.intent,
    fingerprint,
    customer_currency: "USD",
  };
  const command = crypto.randomUUID();
  return {
    data,
    command,
    deadline,
    requestId: projection.proposed_request_id,
    attemptId: projection.proposed_attempt_id,
    async admit() {
      return client.execution("execution_admit_v1", data, {
        commandId: command,
        deadlineAt: deadline,
      }) as Promise<ExecutionReply>;
    },
    beginData(receipt: ExecutionReceipt) {
      return {
        attempt_id: projection.proposed_attempt_id,
        permit_epoch: "1",
        authorization_plan_digest: receipt.authorization_plan_digest,
        dispatched_digest: data.dispatched_digest,
        prep_session_id: data.prep_session_id,
        prep_binding_digest: data.prep_binding_digest,
      };
    },
  };
}

import { sign } from "node:crypto";
import { executionDataSchemas, commandProofFields, type ExecutionKind } from "@gateway/contracts";
import { registeredIdentity, identityRequest } from "./internal-identity.ts";
export async function financialRpcSession(
  f: Awaited<ReturnType<typeof prepareFinancial>>,
  options: { timeoutMs?: number } = {},
) {
  const a = await f.identity.connection("gateway", f.identity.a.port, options),
    b = await f.identity.connection("gateway", f.identity.b.port, options),
    identity = await registeredIdentity(a, f.identity.a.port);
  async function send(
    kind: ExecutionKind,
    data: unknown,
    requestOptions: { commandId?: string; deadlineAt?: string; traceId?: string } = {},
    second = false,
    raw = false,
  ) {
    const owner = identity.receipt,
      command = requestOptions.commandId ?? crypto.randomUUID(),
      deadline = requestOptions.deadlineAt ?? new Date(Date.now() + 25000).toISOString(),
      trace = requestOptions.traceId ?? crypto.randomUUID();
    const parsed = raw ? data : executionDataSchemas[kind].parse(data),
      path = `/_internal/core/v1/${({ execution_context_v1: "context", execution_admit_v1: "admissions", execution_next_v1: "attempts", execution_begin_v1: "begin", execution_abort_v1: "abort", execution_query_v1: "commands", execution_quota_v1: "quota", execution_quota_recovery_v1: "quota-recovery", execution_abort_recovery_v1: "abort-recovery" } as const)[kind]}`;
    const body = {
      envelope: {
        command_id: command,
        body_digest: sha256(
          canonical({
            kind,
            contract_version: 1,
            owner: {
              service: owner.service,
              instance_id: owner.instance_id,
              session_id: owner.session_id,
              incarnation_id: owner.incarnation_id,
            },
            deadline_at: deadline,
            data: parsed,
          }),
        ),
        caller_incarnation: owner.incarnation_id,
        deadline_at: deadline,
        trace_id: trace,
        contract_version: 1,
      },
      data: parsed,
    };
    const proof = {
      session_id: owner.session_id,
      incarnation_id: owner.incarnation_id,
      command_type: kind,
      command_id: command,
      deadline_at: deadline,
      trace_id: trace,
      contract_version: 1 as const,
      raw_body_sha256: sha256(JSON.stringify(body)),
    };
    const signature = sign(
      null,
      Buffer.from(JSON.stringify(commandProofFields(identityAudience, owner, proof, "POST", path))),
      identity.keys.privateKey,
    ).toString("base64url");
    return identityRequest(
      second ? b : a,
      second ? f.identity.b.port : f.identity.a.port,
      path,
      body,
      Buffer.from(JSON.stringify({ ...proof, signature })).toString("base64url"),
    );
  }
  return {
    identity,
    send,
    execution: async (
      kind: ExecutionKind,
      data: unknown,
      options?: { commandId?: string; deadlineAt?: string; traceId?: string },
    ) => {
      const response = await send(kind, data, options);
      if (response.status >= 400) throw new Error(String(response.value.code));
      return response.value;
    },
  };
}
export async function financialBlockedBy(db: SQL, pid: number) {
  const deadline = Date.now() + 2000;
  while (Date.now() < deadline) {
    const rows =
      await db`SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND ${pid}=ANY(pg_blocking_pids(pid))`;
    if (rows.length) return Number(rows[0].pid);
    await Bun.sleep(5);
  }
  throw new Error("Expected PostgreSQL blocking edge absent");
}
export async function financialGate(db: SQL) {
  let release!: () => void, ready!: (pid: number) => void;
  const key = Math.floor(Math.random() * 100000000) + 100000000,
    gate = new Promise<void>((r) => (release = r)),
    locked = new Promise<number>((r) => (ready = r));
  const transaction = db.begin(async (tx) => {
    const pid = Number((await tx`SELECT pg_backend_pid() pid`)[0].pid);
    await tx`SELECT pg_advisory_xact_lock(${key}::bigint)`;
    ready(pid);
    await gate;
  });
  const pid = await locked;
  return {
    key,
    pid,
    async release() {
      release();
      await transaction;
    },
  };
}

export const financialProviderSchema = z.object({
  id: z.uuid(),
  account_id: z.uuid(),
  brand_id: z.uuid(),
  config_revision: z.string(),
});
export function financialBalances(value: unknown) {
  return z
    .object({
      data: z.array(
        z.object({
          currency: z.string(),
          balance_atoms: z.string(),
          reserved_atoms: z.string(),
          available_atoms: z.string(),
        }),
      ),
    })
    .parse(value).data;
}
