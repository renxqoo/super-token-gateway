import { z } from "zod";
import { SCHEMA_VERSION } from "@gateway/ledger";
import { fileURLToPath } from "node:url";
import { createGatewayRuntime } from "../apps/gateway/src/app.ts";
import { loadConfigFromEnv } from "../apps/gateway/src/config.ts";

const { publicApp, internalApp, internalRequestBodies, internalRouteRoles, close } =
  createGatewayRuntime({
    config: loadConfigFromEnv(
      {
        GATEWAY_MODE: "gateway",
        DATABASE_URL: "postgres://openapi@127.0.0.1:1/openapi",
        KEY_PEPPER: "openapi-generation-only",
        ADMIN_TOKEN: "openapi-generation-only",
        ENCRYPTION_KEY: "0".repeat(64),
      },
      [],
    ),
    wasmExecutor: null,
  });

const bearerAuth = { type: "http", scheme: "bearer" };
const jsonObject = { type: "object", additionalProperties: true };
const errorSchema = {
  type: "object",
  required: ["error"],
  properties: {
    error: {
      type: "object",
      required: ["code", "message"],
      properties: { code: { type: "string" }, message: { type: "string" } },
    },
  },
};
const jsonError = {
  description: "Gateway error",
  content: { "application/json": { schema: errorSchema } },
};
const multipartPaths = new Set([
  "/v1/images/edits",
  "/v1/audio/transcriptions",
  "/v1/audio/translations",
]);
const queryParameters: Record<string, string[]> = {
  "/internal/v1/plugins": ["capability"],
  "/internal/v1/plugins/builtins": ["capability"],
  "/internal/v1/pricing-overrides": ["project_id"],
  "/internal/v1/wallets/:projectId": ["currency"],
  "/internal/v1/upstream-accounts/:accountId/balance": ["currency"],
  "/internal/v1/charges": ["project_id", "api_key_id", "outcome", "from", "to", "limit", "offset"],
  "/internal/v1/async-tasks": ["project_id", "status", "refund_eligibility", "limit", "offset"],
  "/internal/v1/ledger-entries": [
    "side",
    "entity_id",
    "request_id",
    "kind",
    "from",
    "to",
    "limit",
    "offset",
  ],
  "/internal/v1/holds": ["state", "project_id", "limit", "offset"],
};

const paths: Record<string, Record<string, unknown>> = {};
try {
  for (const [listener, routes] of [
    ["public", publicApp.routes],
    ["internal", internalApp.routes],
  ] as const) {
    for (const { method, path } of routes) {
      if (method === "ALL" || path.endsWith("*")) continue;
      const internal = listener === "internal";
      const inference = !internal && method === "POST" && !path.startsWith("/v1/videos/:taskId");
      const apiKeyRequired =
        inference || path === "/v1/models" || path.startsWith("/v1/videos/:taskId");
      const security: Record<string, string[]>[] = [];
      if (internal) security.push({ internalBearer: [] });
      if (apiKeyRequired) security.push({ projectBearer: [] });
      if (path === "/v1/messages" || path === "/v1/models") security.push({ anthropicKey: [] });
      if (path === "/v1beta/models/:action" || path === "/v1/models") {
        security.push({ geminiHeaderKey: [] }, { geminiQueryKey: [] });
      }
      const parameters = [...path.matchAll(/:([\w]+)/g)].map(([, name]) => ({
        name,
        in: "path",
        required: true,
        schema:
          name?.endsWith("Id") && name !== "taskId"
            ? { type: "string", format: "uuid" }
            : { type: "string" },
        ...(name === "action"
          ? { description: "MODEL:generateContent or MODEL:streamGenerateContent" }
          : {}),
      }));
      if (inference) {
        parameters.push({
          name: "Idempotency-Key",
          in: "header",
          required: false,
          schema: { type: "string" },
        });
      }
      if (method === "GET") {
        for (const name of queryParameters[path] ?? []) {
          parameters.push({
            name,
            in: "query",
            required: false,
            schema: name.endsWith("_id") ? { type: "string", format: "uuid" } : { type: "string" },
          });
        }
      }
      const jsonBody = internalRequestBodies.get(`${method} ${path}`);
      const role = internalRouteRoles.get(`${method} ${path}`);
      if (internal && !role) throw new Error(`Missing authorization role for ${method} ${path}`);
      if (internal && method !== "GET" && !jsonBody) {
        throw new Error(`Missing request schema for ${method} ${path}`);
      }
      const requestContent: Record<string, unknown> = {};
      if (jsonBody) {
        const schema = z.toJSONSchema(jsonBody.schema, { io: "input" });
        delete schema.$schema;
        requestContent["application/json"] = { schema };
      }
      if (inference) {
        requestContent["application/json"] = {
          schema: {
            ...jsonObject,
            ...(path !== "/v1beta/models/:action"
              ? { required: ["model"], properties: { model: { type: "string" } } }
              : {}),
          },
        };
      }
      if (multipartPaths.has(path)) {
        requestContent["multipart/form-data"] = { schema: jsonObject };
      }
      const responseContent: Record<string, unknown> =
        path === "/metrics"
          ? { "text/plain": { schema: { type: "string" } } }
          : { "application/json": { schema: {} } };
      if (inference) {
        responseContent["text/event-stream"] = { schema: { type: "string" } };
        responseContent["*/*"] = { schema: { type: "string", format: "binary" } };
      }
      const openapiPath = path.replace(/:([\w]+)/g, "{$1}");
      (paths[openapiPath] ??= {})[method.toLowerCase()] = {
        "x-stg-listener": listener,
        ...(path === "/metrics" ? { "x-stg-production-listener": "internal" } : {}),
        ...(role ? { "x-stg-role": role } : {}),
        security,
        ...(parameters.length ? { parameters } : {}),
        ...(Object.keys(requestContent).length
          ? { requestBody: { required: jsonBody?.required ?? true, content: requestContent } }
          : {}),
        responses: {
          "200": { description: "Successful response", content: responseContent },
          ...(internal || apiKeyRequired ? { default: jsonError } : {}),
          ...(path === "/readyz"
            ? {
                "503": {
                  description: "Dependency unavailable or draining",
                  content: responseContent,
                },
              }
            : {}),
        },
      };
    }
  }
} finally {
  await close();
}

const spec = {
  openapi: "3.1.0",
  info: {
    title: "Super Token Gateway",
    version: String(SCHEMA_VERSION),
    description:
      "Public and internal routes use separate listeners. Inference payloads and success responses depend on the endpoint protocol and provider adapter.",
  },
  paths,
  components: {
    securitySchemes: {
      projectBearer: bearerAuth,
      internalBearer: bearerAuth,
      anthropicKey: { type: "apiKey", in: "header", name: "x-api-key" },
      geminiHeaderKey: { type: "apiKey", in: "header", name: "x-goog-api-key" },
      geminiQueryKey: { type: "apiKey", in: "query", name: "key" },
    },
  },
};

const destination = new URL("../docs/openapi.json", import.meta.url);
const output = `${JSON.stringify(spec, null, 2)}\n`;
if (process.argv.includes("--check")) {
  if (JSON.stringify(await Bun.file(destination).json()) !== JSON.stringify(spec)) {
    throw new Error("OpenAPI is out of date. Run bun run openapi.");
  }
} else {
  await Bun.write(destination, output);
  const format = Bun.spawn([process.execPath, "fmt", "--no-script", fileURLToPath(destination)], {
    stdout: "inherit",
    stderr: "inherit",
  });
  if ((await format.exited) !== 0) throw new Error("OpenAPI formatting failed.");
}
