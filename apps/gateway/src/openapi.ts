import { z } from "zod";
import {
  keySchema,
  modelSchema,
  operations,
  pricingSchema,
  projectSchema,
  providerSchema,
  routeSchema,
  requestPolicySchema,
  creditSchema,
  creditPolicySchema,
  tariffSchema,
  refundSchema,
  adjustmentSchema,
  resolutionSchema,
} from "@gateway/kernel";

export const inferencePaths: Record<string, string> = {
  "/v1/chat/completions": "chat",
  "/v1/responses": "responses",
  "/v1/messages": "messages",
  "/v1/embeddings": "embeddings",
  "/v1/images/generations": "images",
  "/v1/images/edits": "image_edits",
  "/v1/audio/speech": "speech",
  "/v1/audio/transcriptions": "transcription",
  "/v1/audio/translations": "translation",
  "/v1/files": "files",
  "/v1/batches": "batches",
  "/v1/videos": "videos",
  "/v1/moderations": "moderations",
  "/v1/completions": "completions",
};
export const resourceActions = {
  files: ["get", "delete", "content"],
  batches: ["get", "cancel"],
  videos: ["get", "delete", "content"],
  responses: ["get", "delete", "cancel", "input_items"],
} as const;
export function openapi(): Record<string, unknown> {
  const paths: Record<string, Record<string, unknown>> = {};
  const schemas: Record<string, unknown> = {
    Project: z.toJSONSchema(projectSchema),
    Credential: z.toJSONSchema(keySchema),
    Provider: z.toJSONSchema(providerSchema),
    Model: z.toJSONSchema(modelSchema),
    Route: z.toJSONSchema(routeSchema),
    Pricing: z.toJSONSchema(pricingSchema),
    Credit: z.toJSONSchema(creditSchema),
    CreditPolicy: z.toJSONSchema(creditPolicySchema),
    Tariff: z.toJSONSchema(tariffSchema),
    Refund: z.toJSONSchema(refundSchema),
    Adjustment: z.toJSONSchema(adjustmentSchema),
    Resolution: z.toJSONSchema(resolutionSchema),
    UsageEvent: {
      type: "object",
      required: [
        "id",
        "attempt_id",
        "source",
        "semantics",
        "finality",
        "usage",
        "cost_atoms",
        "currency",
        "cost_complete",
      ],
      properties: {
        id: { type: "string", format: "uuid" },
        attempt_id: { type: "string", format: "uuid" },
        source: { type: "string" },
        semantics: { const: "attempt_snapshot" },
        finality: { enum: ["FINAL", "PARTIAL", "UNKNOWN", "ESTIMATED"] },
        usage: { type: "object", additionalProperties: { type: "string" } },
        cost_atoms: {
          type: "string",
          pattern: "^[0-9]+$",
          description:
            "Integer atoms, 10^-18 units of the recorded currency. Cumulative per-attempt evidence; never sum revisions.",
        },
        currency: { type: "string" },
        cost_complete: { type: "boolean" },
      },
    },
    Error: {
      type: "object",
      properties: {
        error: {
          type: "object",
          properties: { code: { type: "string" }, message: { type: "string" } },
        },
        request_id: { type: "string" },
      },
    },
  };
  const responses = {
    "200": { description: "Success; output content is never retained by Gateway" },
    "400": { description: "Invalid request" },
    "401": { description: "Invalid credential" },
    "402": { description: "Insufficient eligible funds or frozen wallet" },
    "403": { description: "Scope denied" },
    "404": { description: "Owned resource or model not found" },
    "409": { description: "Configuration/payload conflict or an already accepted intention" },
    "413": { description: "Byte budget exceeded" },
    "422": { description: "Unsupported capability or semantics" },
    "429": { description: "Rate, token or concurrency budget exceeded" },
    "502": { description: "Provider, conversion or usage finalization failed" },
    "503": { description: "Admission or durable storage unavailable" },
    "504": { description: "Deadline reached" },
  };
  const add = (path: string, method: string, id: string, schema?: unknown, admin = false) => {
    const parameters = [...path.matchAll(/\{([^}]+)\}/g)].map((match) => ({
      in: "path",
      name: match[1],
      required: true,
      schema: { type: "string" },
    }));
    const value: Record<string, unknown> = {
      operationId: id,
      security: [{ [admin ? "adminKey" : "projectKey"]: [] }],
      parameters,
      responses,
    };
    if (schema) value.requestBody = { required: true, content: { "application/json": { schema } } };
    (paths[path] ??= {})[method] = value;
    return value;
  };
  for (const [path, operation] of Object.entries(inferencePaths)) {
    const multipart = ["files", "image_edits", "transcription", "translation"].includes(operation);
    const spec = add(path, "post", operation, {
      type: "object",
      additionalProperties: true,
      description:
        "Native provider request. Logical model alias replaces the upstream model. Unsupported cross-protocol semantics return 422.",
    });
    spec.parameters = [
      {
        in: "header",
        name: "x-gateway-currency",
        schema: { type: "string", pattern: "^[A-Z]{3,8}$" },
        description: "Wallet and tariff currency; defaults to project currency. No FX conversion.",
      },
      {
        in: "header",
        name: "Idempotency-Key",
        schema: { type: "string", maxLength: 200 },
        description:
          "Repeated intentions return 409 and their original request ID; Gateway cannot replay response bodies.",
      },
    ];
    if (multipart)
      spec.requestBody = {
        required: true,
        content: {
          "multipart/form-data": {
            schema: {
              type: "object",
              properties: {
                model: { type: "string" },
                file: { type: "string", format: "binary" },
                image: { type: "string", format: "binary" },
                purpose: { type: "string" },
                stream: { type: "boolean" },
              },
              additionalProperties: true,
            },
          },
        },
      };
  }
  for (const [kind, actions] of Object.entries(resourceActions)) {
    const list = add(`/v1/${kind}`, "get", `list_${kind}`);
    list.parameters = [
      {
        in: "query",
        name: "limit",
        schema: { type: "integer", minimum: 1, maximum: 100, default: 100 },
      },
      { in: "query", name: "after", schema: { type: "string" } },
    ];
    for (const action of actions) {
      const path = `/v1/${kind}/{id}${["get", "delete"].includes(action) ? "" : `/${action}`}`;
      add(
        path,
        action === "delete" ? "delete" : action === "cancel" ? "post" : "get",
        `${action}_${kind}`,
      );
    }
  }
  add("/v1/models", "get", "models");
  add("/v1/usage", "get", "project_usage");
  add("/v1/requests/{id}", "get", "request_evidence");
  add("/v1/wallets", "get", "wallets")["x-gateway-roles"] = ["ADMIN", "BILLING"];
  add("/v1/requests/{id}/billing", "get", "billing_receipt")["x-gateway-roles"] = [
    "ADMIN",
    "BILLING",
  ];
  add("/v1/project/keys", "get", "project_keys")["x-gateway-roles"] = ["ADMIN"];
  add("/v1/project/keys", "post", "project_create_key", {
    $ref: "#/components/schemas/Credential",
  })["x-gateway-roles"] = ["ADMIN"];
  add("/v1/project/keys/{id}", "delete", "project_revoke_key")["x-gateway-roles"] = ["ADMIN"];
  add("/v1beta/models", "get", "gemini_models");
  add("/v1beta/models/{model}:generateContent", "post", "generateContent", {
    type: "object",
    additionalProperties: true,
  });
  add("/v1beta/models/{model}:streamGenerateContent", "post", "streamGenerateContent", {
    type: "object",
    additionalProperties: true,
  });
  for (const [path, schema] of [
    ["projects", "Project"],
    ["providers", "Provider"],
    ["models", "Model"],
  ]) {
    add(`/admin/${path}`, "get", `admin_list_${path}`, undefined, true);
    add(
      `/admin/${path}`,
      "post",
      `admin_create_${path}`,
      { $ref: `#/components/schemas/${schema}` },
      true,
    );
    add(
      `/admin/${path}/{${path === "models" ? "model" : "id"}}`,
      "patch",
      `admin_update_${path}`,
      {
        type: "object",
        additionalProperties: false,
        properties:
          path === "projects"
            ? {
                enabled: { type: "boolean" },
                name: { type: "string" },
                billing_currency: { type: "string", pattern: "^[A-Z]{3,8}$" },
                rpm: { type: "integer", minimum: 1 },
                tpm: { type: "integer", minimum: 1 },
                concurrency: { type: "integer", minimum: 1 },
              }
            : path === "providers"
              ? {
                  enabled: { type: "boolean" },
                  api_key: { type: "string" },
                  rpm: { type: "integer", minimum: 1 },
                  concurrency: { type: "integer", minimum: 1 },
                }
              : {
                  enabled: { type: "boolean" },
                  operations: { type: "array", items: { enum: operations } },
                  max_output_tokens: { type: "integer", minimum: 1 },
                  routing_strategy: { enum: ["WEIGHTED", "ROUND_ROBIN", "LEAST_LATENCY"] },
                },
      },
      true,
    );
  }
  add("/admin/projects/{id}/keys", "get", "admin_keys", undefined, true);
  add(
    "/admin/projects/{id}/keys",
    "post",
    "admin_create_key",
    { $ref: "#/components/schemas/Credential" },
    true,
  );
  add("/admin/keys/{id}", "delete", "admin_revoke_key", undefined, true);
  add(
    "/admin/keys/{id}",
    "patch",
    "admin_scope_key",
    {
      type: "object",
      properties: {
        models: { type: "array", items: { type: "string" } },
        operations: { type: "array", items: { enum: operations } },
        expires_at: { type: "string", format: "date-time" },
        role: { enum: ["ADMIN", "INVOKER", "BILLING", "VIEWER"] },
        allowed_cidrs: { type: "array", maxItems: 100, items: { type: "string" } },
        rpm: { type: "integer", minimum: 1 },
        tpm: { type: "integer", minimum: 1 },
        concurrency: { type: "integer", minimum: 1 },
      },
    },
    true,
  );
  add("/admin/models/{model}/routes", "get", "admin_routes", undefined, true);
  add(
    "/admin/models/{model}/routes",
    "post",
    "admin_create_route",
    { $ref: "#/components/schemas/Route" },
    true,
  );
  add(
    "/admin/routes/{id}",
    "patch",
    "admin_update_route",
    {
      type: "object",
      properties: {
        enabled: { type: "boolean" },
        priority: { type: "integer" },
        weight: { type: "number" },
        pricing: { $ref: "#/components/schemas/Pricing" },
        request_policy: z.toJSONSchema(requestPolicySchema),
      },
    },
    true,
  );
  add("/admin/usage", "get", "admin_usage", undefined, true);
  add(
    "/admin/projects/{id}/credits",
    "post",
    "credit_wallet",
    { $ref: "#/components/schemas/Credit" },
    true,
  );
  add("/admin/projects/{id}/wallets", "get", "admin_wallets", undefined, true);
  add(
    "/admin/projects/{id}/wallets/{currency}/credit-policy",
    "post",
    "configure_wallet_credit",
    { $ref: "#/components/schemas/CreditPolicy" },
    true,
  );
  add("/admin/projects/{id}/wallets/{currency}/lots", "get", "wallet_lots", undefined, true);
  add("/admin/projects/{id}/wallets/{currency}/journal", "get", "wallet_journal", undefined, true);
  add(
    "/admin/projects/{id}/wallets/{currency}",
    "patch",
    "configure_wallet",
    {
      type: "object",
      additionalProperties: false,
      properties: {
        policy: { enum: ["GIFT_FIRST", "CASH_FIRST", "EARLIEST_EXPIRY"] },
        frozen: { type: "boolean" },
      },
    },
    true,
  );
  add("/admin/tariffs", "get", "tariffs", undefined, true);
  add("/admin/tariffs", "post", "set_tariff", { $ref: "#/components/schemas/Tariff" }, true);
  add(
    "/admin/charges/{id}/refunds",
    "post",
    "refund_charge",
    { $ref: "#/components/schemas/Refund" },
    true,
  );
  add(
    "/admin/charges/{id}/adjustments",
    "post",
    "adjust_charge",
    { $ref: "#/components/schemas/Adjustment" },
    true,
  );
  add(
    "/admin/requests/{id}/resolutions",
    "post",
    "resolve_billing",
    { $ref: "#/components/schemas/Resolution" },
    true,
  );
  const quotaProperties = {
    rpm: { type: "integer", minimum: 1, maximum: 10000000 },
    tpm: { type: "integer", minimum: 1, maximum: 1000000000 },
    concurrency: { type: "integer", minimum: 1, maximum: 1000000 },
  };
  add(
    "/admin/organizations",
    "post",
    "create_organization",
    {
      type: "object",
      additionalProperties: false,
      required: ["external_ref", "name"],
      properties: {
        external_ref: { type: "string", minLength: 1, maxLength: 200 },
        name: { type: "string", minLength: 1, maxLength: 100 },
        ...quotaProperties,
      },
    },
    true,
  );
  add(
    "/admin/organizations/{id}",
    "patch",
    "update_organization",
    {
      type: "object",
      additionalProperties: false,
      properties: { enabled: { type: "boolean" }, ...quotaProperties },
    },
    true,
  );
  const roles = { enum: ["OWNER", "ADMIN", "MEMBER", "BILLING", "VIEWER"] };
  add(
    "/admin/organizations/{id}/principals",
    "post",
    "create_principal",
    {
      type: "object",
      additionalProperties: false,
      required: ["external_ref", "role"],
      properties: {
        external_ref: { type: "string", minLength: 1, maxLength: 200 },
        role: roles,
        ...quotaProperties,
      },
    },
    true,
  );
  add(
    "/admin/principals/{id}",
    "patch",
    "update_principal",
    {
      type: "object",
      additionalProperties: false,
      properties: { enabled: { type: "boolean" }, role: roles, ...quotaProperties },
    },
    true,
  );
  add(
    "/admin/consumers",
    "post",
    "admin_create_consumer",
    {
      type: "object",
      required: ["name"],
      properties: { name: { type: "string" }, replay: { type: "boolean", default: true } },
    },
    true,
  );
  add(
    "/admin/consumers/{id}/claim",
    "post",
    "admin_claim_events",
    {
      type: "object",
      properties: { limit: { type: "integer", minimum: 1, maximum: 500, default: 100 } },
    },
    true,
  );
  add(
    "/admin/consumers/{id}/ack",
    "post",
    "admin_ack_events",
    {
      type: "object",
      required: ["lease_token", "event_ids"],
      properties: {
        lease_token: { type: "string", format: "uuid" },
        event_ids: { type: "array", maxItems: 500, items: { type: "string", format: "uuid" } },
      },
    },
    true,
  );
  add("/admin/recover", "post", "admin_recover", undefined, true);
  add("/admin/queue", "get", "usage_queue_stats", undefined, true);
  add("/admin/queue/jobs/{id}/retry", "post", "retry_usage_job", undefined, true);
  add("/admin/metrics", "get", "metrics", undefined, true);
  for (const path of ["/livez", "/readyz", "/openapi.json"]) {
    add(path, "get", path.slice(1));
    const entry = paths[path]?.get as Record<string, unknown>;
    entry.security = [];
  }
  return {
    openapi: "3.1.0",
    info: {
      title: "Independent LLM Gateway",
      version: "1.0.0",
      description:
        "Independent technical tenants, routing, metering and prepaid wallet settlement. Commercial identity, payments, subscriptions and marketing remain external. No LLM content persistence.",
    },
    servers: [{ url: "/" }],
    components: {
      securitySchemes: {
        projectKey: { type: "http", scheme: "bearer" },
        adminKey: { type: "http", scheme: "bearer" },
      },
      schemas,
    },
    paths,
  };
}
