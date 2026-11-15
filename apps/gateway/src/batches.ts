import { parseClientJson, stringifyExact } from "@gateway/adapters/protocol";
import { GatewayError, batchEndpoints } from "@gateway/contracts";
import { rewriteJSON, type AbiResult } from "@gateway/plugin-host";
import { batchIdentityDigest } from "@gateway/runtime";
import { collectReferences } from "./references.ts";
import { rejectUnmeteredTools } from "./tool-policy.ts";

type Endpoint = keyof typeof batchEndpoints;
type Attempt = {
  transformJSON(
    hook: "prepare" | "response",
    metadata: Record<string, unknown>,
    body: Uint8Array,
    signal?: AbortSignal,
  ): Promise<{ result: AbiResult; body?: Uint8Array }>;
  dispose(): void;
};
export type BatchInput = {
  model: string;
  endpoint: Endpoint;
  rows: { customId: string; body: Uint8Array }[];
  idsSha256: string;
  images: number | undefined;
};
export type BatchRecord = {
  endpoint: Endpoint;
  upstream_model: string;
  items: number;
  ids_sha256: string;
  ceilings: Record<string, string>;
  images?: number;
};

const rowKeys = new Set(["custom_id", "method", "url", "body"]);
const fileFields = ["input_file_id", "output_file_id", "error_file_id"] as const;
const workers = 4;

function invalid(message: string, status = 400): never {
  throw new GatewayError(status, "invalid_batch", message);
}

function record(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

export function batchMetadata(endpoint: Endpoint, model: string) {
  const operation = batchEndpoints[endpoint];
  return operation === "chat" || operation === "responses"
    ? { protocol: operation, operation, model }
    : { operation, model };
}

export function parseBatchInput(bytes: Uint8Array): BatchInput {
  let source: string;
  try {
    source = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    invalid("Batch input must be UTF-8 JSONL");
  }
  const lines = source.split(/\r?\n/).filter((line) => line.trim());
  if (!lines.length || lines.length > 50000) invalid("Batch must contain 1 to 50000 requests");
  const ids = new Set<string>();
  let model = "",
    endpoint = "",
    embeddingInputs = 0,
    images = 0;
  const rows = lines.map((line, index) => {
    let row: Record<string, unknown> | undefined;
    try {
      row = record(parseClientJson(line));
    } catch {
      row = undefined;
    }
    const body = record(row?.body);
    if (!row || !body || Object.keys(row).some((key) => !rowKeys.has(key)))
      invalid(`Batch line ${index + 1} must be an object with custom_id, method, url and body`);
    const customId = row.custom_id;
    if (
      row.method !== "POST" ||
      typeof customId !== "string" ||
      !customId.length ||
      customId.length > 200 ||
      ids.has(customId)
    )
      invalid("Each batch request requires a unique custom_id and POST method");
    ids.add(customId);
    if (
      body.stream === true ||
      typeof row.url !== "string" ||
      !Object.hasOwn(batchEndpoints, row.url)
    )
      invalid("Unsupported batch endpoint or streaming request", 422);
    if (typeof body.model !== "string" || !body.model)
      invalid("Each batch request requires a model");
    if ((model && model !== body.model) || (endpoint && endpoint !== row.url))
      invalid("A batch file must use one logical model and one endpoint");
    model = body.model;
    endpoint = row.url;
    collectReferences(body, false);
    rejectUnmeteredTools(body, batchEndpoints[endpoint as Endpoint]);
    if (endpoint === "/v1/embeddings")
      embeddingInputs +=
        Array.isArray(body.input) && typeof body.input[0] !== "number" ? body.input.length : 1;
    if (endpoint === "/v1/images/generations" || endpoint === "/v1/images/edits") {
      const count = body.n ?? 1;
      if (
        typeof count !== "number" ||
        !Number.isSafeInteger(count) ||
        count < 1 ||
        !Number.isSafeInteger(images + count)
      )
        invalid("Batch image counts must be positive integers within the batch budget");
      images += count;
    }
    return { customId, body: new TextEncoder().encode(stringifyExact(body)) };
  });
  if (embeddingInputs > 50000) invalid("Embedding batch input limit exceeded");
  return {
    model,
    endpoint: endpoint as Endpoint,
    rows,
    idsSha256: batchIdentityDigest(ids),
    images: images || undefined,
  };
}

const scale = 10n ** 18n;

function atoms(value: string) {
  const [whole, fraction = ""] = value.split(".");
  return BigInt(whole!) * scale + BigInt(fraction.padEnd(18, "0"));
}

export function sumQuantities(target: Record<string, string>, source: Record<string, string>) {
  for (const [key, value] of Object.entries(source)) {
    const total = atoms(target[key] ?? "0") + atoms(value);
    const whole = total / scale,
      fraction = (total % scale).toString().padStart(18, "0").replace(/0+$/, "");
    target[key] = fraction ? `${whole}.${fraction}` : String(whole);
  }
  return target;
}

export async function prepareBatchRows(
  input: BatchInput,
  open: () => Attempt,
  signal: AbortSignal,
  inspect: (body: Uint8Array) => void,
): Promise<{ bytes: Uint8Array; record: BatchRecord }> {
  const lines: string[] = Array.from({ length: input.rows.length }, () => "");
  const ceilings: Record<string, string> = {};
  const metadata = batchMetadata(input.endpoint, input.model);
  let upstream: string | undefined,
    next = 0;
  const decoder = new TextDecoder(),
    stop = new AbortController(),
    combined = AbortSignal.any([signal, stop.signal]);
  const work = async () => {
    const attempt = open();
    try {
      while (next < input.rows.length && !combined.aborted) {
        const index = next++;
        const row = input.rows[index]!;
        const transformed = await attempt.transformJSON("prepare", metadata, row.body, combined);
        const result = transformed.result;
        if (result.kind === "failure")
          invalid(`Batch line ${index + 1} was rejected by the route (${result.code})`);
        if (
          result.kind !== "prepared" ||
          !transformed.body ||
          result.request.method !== "POST" ||
          result.request.path !== input.endpoint ||
          result.request.body_format === "multipart" ||
          result.request.manifest.operation !== metadata.operation ||
          result.request.manifest.stream ||
          (upstream !== undefined && upstream !== result.request.manifest.upstream_model)
        )
          throw new GatewayError(
            422,
            "invalid_batch",
            `Batch line ${index + 1} cannot be sent through this route`,
          );
        inspect(transformed.body);
        upstream = result.request.manifest.upstream_model;
        sumQuantities(ceilings, result.request.manifest.ceilings as Record<string, string>);
        lines[index] =
          `{"custom_id":${JSON.stringify(row.customId)},"method":"POST","url":${JSON.stringify(input.endpoint)},"body":${decoder.decode(transformed.body)}}`;
      }
    } catch (error) {
      stop.abort();
      throw error;
    } finally {
      attempt.dispose();
    }
  };
  await Promise.all(Array.from({ length: Math.min(workers, input.rows.length) }, work));
  return {
    bytes: new TextEncoder().encode(`${lines.join("\n")}\n`),
    record: {
      endpoint: input.endpoint,
      upstream_model: upstream!,
      items: input.rows.length,
      ids_sha256: input.idsSha256,
      ceilings,
      ...(input.images ? { images: input.images } : {}),
    },
  };
}

export function batchFiles(body: Uint8Array) {
  let value: Record<string, unknown> | undefined;
  try {
    value = record(JSON.parse(new TextDecoder().decode(body)));
  } catch {
    value = undefined;
  }
  if (!value)
    throw new GatewayError(502, "upstream_invalid_response", "The upstream response was invalid");
  const files: Partial<Record<(typeof fileFields)[number], string>> = {};
  for (const field of fileFields) {
    const entry = value[field];
    if (entry === undefined || entry === null) continue;
    if (typeof entry !== "string" || !/^[A-Za-z0-9_:/.-]{1,500}$/.test(entry))
      throw new GatewayError(502, "upstream_invalid_response", "The upstream response was invalid");
    files[field] = entry;
  }
  return { status: value.status, files };
}

export function exposeBatchFiles(body: Uint8Array, mapping: Map<string, string>) {
  const { files } = batchFiles(body);
  return rewriteJSON(
    body,
    Object.entries(files).map(([field, remote]) => {
      const mapped = mapping.get(remote);
      return {
        op: "set" as const,
        pointer: `/${field}`,
        value: mapped === undefined ? "null" : JSON.stringify(mapped),
      };
    }),
  );
}

export type BatchLine = { customId: string; ok: boolean; body: Uint8Array | undefined };

export function parseBatchOutput(bytes: Uint8Array): BatchLine[] {
  const source = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  return source
    .split(/\r?\n/)
    .filter((line) => line.trim())
    .map((line) => {
      const row = record(parseClientJson(line));
      const response = record(row?.response);
      if (!row || typeof row.custom_id !== "string") throw new Error("batch_output");
      const status = response?.status_code;
      const body = record(response?.body);
      const ok = typeof status === "number" && status >= 200 && status < 300 && !!body;
      return {
        customId: row.custom_id,
        ok,
        body: ok ? new TextEncoder().encode(stringifyExact(body)) : undefined,
      };
    });
}
