import { canonical } from "@gateway/kernel";
import {
  inputSchema,
  jsonMetadataSchema,
  resultSchema,
  sessionSchema,
  sha256,
  validateConfig,
  verifiedPackageRuntime,
  pluginModuleInfo,
  type PluginModule,
  type AbiResult,
  type Hook,
  type SessionIdentity,
  type VerifiedPackage,
} from "@gateway/plugin-sdk";
import { PluginHostError, type PluginSupervisor } from "./supervisor.ts";
import { createJSONDocuments, type JSONDocument, type JSONFact } from "./json-documents.ts";
import { createSSEStream } from "./sse-stream.ts";
import { createAwsEventStreamDecoder } from "./aws-eventstream.ts";

export type AttemptOptions = {
  tenant: string;
  attempt: string;
  incarnation: string;
  deadlineAt: number;
  config: unknown;
  maxStateBytes?: number;
  maxOutputBytes?: number;
  batchOutputBytes?: number;
  maxBatchInputBytes?: number;
  maxEventBytes?: number;
};
export type PluginExecutor = Pick<PluginSupervisor, "load" | "call" | "onDisconnect" | "close">;
export function createPluginAttempts(
  supervisor: PluginExecutor,
  limits: {
    maxAttempts?: number;
    maxStateBytes?: number;
    maxJSONBytes?: number;
    additionalExecutors?: readonly PluginExecutor[];
  } = {},
) {
  const maxAttempts = limits.maxAttempts ?? 128,
    maxStateBytes = limits.maxStateBytes ?? 16 << 20;
  if (
    !Number.isSafeInteger(maxAttempts) ||
    maxAttempts < 1 ||
    maxAttempts > 10000 ||
    !Number.isSafeInteger(maxStateBytes) ||
    maxStateBytes < 1 ||
    maxStateBytes > 256 << 20
  )
    throw new PluginHostError("CONFIG_INVALID");
  const live = new Map<string, () => void>();
  const preparing = new Set<() => void>();
  const connections = new Map<string, string>();
  const documents = createJSONDocuments(limits.maxJSONBytes);
  if ((limits.additionalExecutors?.length ?? 0) > 16) throw new PluginHostError("CONFIG_INVALID");
  const executors = new Set([supervisor, ...(limits.additionalExecutors ?? [])]);
  const subscriptions: ReturnType<PluginExecutor["onDisconnect"]>[] = [];
  try {
    for (const executor of executors)
      subscriptions.push(
        executor.onDisconnect((connection) => {
          for (const [owner, current] of connections)
            if (current === connection) live.get(owner)?.();
        }),
      );
  } catch (error) {
    for (const unsubscribe of subscriptions) unsubscribe();
    throw error;
  }
  const modules = new Map<string, { module: PluginModule; byteLength: number; refs: number }>();
  let retained = 0,
    reserved = 0,
    closed = false;
  function checkCapacity() {
    if (closed) throw new PluginHostError("HOST_CLOSED");
    if (live.size + preparing.size >= maxAttempts) throw new PluginHostError("ATTEMPT_BUDGET");
  }
  function dispose() {
    if (closed) return;
    closed = true;
    preparing.clear();
    for (const closeAttempt of live.values()) closeAttempt();
    for (const unsubscribe of subscriptions) unsubscribe();
  }
  return {
    dispose,
    reservePreparation() {
      checkCapacity();
      const release = () => {
        preparing.delete(release);
      };
      preparing.add(release);
      return release;
    },
    create(pkg: VerifiedPackage, options: AttemptOptions, executor: PluginExecutor = supervisor) {
      checkCapacity();
      if (!executors.has(executor)) throw new PluginHostError("CONFIG_INVALID");
      const maxState = options.maxStateBytes ?? 256 << 10,
        maxOutput = options.maxOutputBytes ?? 64 << 20,
        batchOutput = options.batchOutputBytes ?? 128 << 10;
      const batchInput = options.maxBatchInputBytes ?? 128 << 10;
      if (
        ![maxState, maxOutput, batchOutput].every((n) => Number.isSafeInteger(n) && n > 0) ||
        maxState > 1 << 20 ||
        batchOutput > 16 << 20 ||
        !Number.isSafeInteger(batchInput) ||
        batchInput < 1 ||
        batchInput > 16 << 20 ||
        maxOutput > 256 << 20 ||
        (options.maxEventBytes !== undefined &&
          (!Number.isSafeInteger(options.maxEventBytes) ||
            options.maxEventBytes < 1024 ||
            options.maxEventBytes > 16 << 20)) ||
        !Number.isSafeInteger(options.deadlineAt) ||
        options.deadlineAt <= Date.now() ||
        options.deadlineAt - Date.now() > 2147483647
      )
        throw new PluginHostError("CONFIG_INVALID");
      const trusted = verifiedPackageRuntime(pkg);
      const { hash: moduleHash, byteLength } = pluginModuleInfo(trusted.module);
      const config = validateConfig(trusted.configSchema, options.config);
      const binding: Omit<SessionIdentity, "sequence"> = {
        id: crypto.randomUUID(),
        tenant: options.tenant,
        attempt: options.attempt,
        incarnation: options.incarnation,
        plugin: trusted.digest,
        config: sha256(canonical(config)),
      };
      sessionSchema.parse({ ...binding, sequence: 0 });
      let code = modules.get(moduleHash);
      if (!code) {
        if (
          modules.size >= 4 ||
          [...modules.values()].reduce((n, item) => n + item.byteLength, byteLength) > 32 << 20
        )
          throw new PluginHostError("MODULE_BUDGET");
        code = { module: trusted.module, byteLength, refs: 0 };
        modules.set(moduleHash, code);
      }
      code.refs++;
      const module = code.module,
        entrypoint = trusted.manifest.entrypoint,
        capabilities = trusted.manifest.capabilities;
      let state: Uint8Array | undefined,
        sequence = 0,
        outputBytes = 0,
        busy = false,
        disposed = false,
        stream = false,
        finished = false;
      let jsonBusy = false,
        operationBusy = false;
      const observers = new Set<() => void>();
      let sse: ReturnType<typeof createSSEStream> | undefined;
      const controller = new AbortController();
      const deadline = setTimeout(dispose, Math.max(1, options.deadlineAt - Date.now()));
      function dispose() {
        if (disposed) return;
        disposed = true;
        controller.abort();
        clearTimeout(deadline);
        retained -= state?.length ?? 0;
        state = undefined;
        live.delete(binding.id);
        connections.delete(binding.id);
        sse?.dispose();
        documents.dispose(binding.id);
        if (--code!.refs === 0) modules.delete(moduleHash);
        const listeners = [...observers];
        observers.clear();
        for (const listener of listeners)
          try {
            listener();
          } catch {
            continue;
          }
      }
      live.set(binding.id, dispose);
      async function invokeRaw(
        hook: Hook,
        payload: Uint8Array = new Uint8Array(),
        signal?: AbortSignal,
        document?: JSONDocument,
        frame = false,
      ): Promise<AbiResult> {
        if (disposed || Date.now() >= options.deadlineAt) {
          dispose();
          throw new PluginHostError("ATTEMPT_CLOSED");
        }
        if (busy || (jsonBusy && !document)) throw new PluginHostError("BATCH_CONCURRENT");
        if (
          !document &&
          (hook === "prepare" ||
            hook === "response" ||
            hook === "error" ||
            hook === "resource_response")
        )
          throw new PluginHostError("JSON_DOCUMENT_REQUIRED");
        if (finished && hook !== "stream_dispose" && hook !== "stream_finish")
          throw new PluginHostError("STREAM_FINISHED");
        if (
          (hook === "stream_feed" || hook === "stream_drain" || hook === "stream_finish") &&
          !stream
        )
          throw new PluginHostError("STREAM_NOT_OPEN");
        if (hook === "stream_open" && stream) throw new PluginHostError("STREAM_ALREADY_OPEN");
        if (payload.length > batchInput) throw new PluginHostError("INPUT_BUDGET");
        if (outputBytes >= maxOutput) {
          dispose();
          throw new PluginHostError("OUTPUT_BUDGET");
        }
        if (retained + reserved + maxState > maxStateBytes)
          throw new PluginHostError("STATE_BUDGET");
        const combined = signal ? AbortSignal.any([signal, controller.signal]) : controller.signal;
        if (combined.aborted) {
          dispose();
          throw new PluginHostError("CALL_CANCELLED");
        }
        const input = inputSchema.parse({
          abi_version: 1,
          hook,
          session: { ...binding, sequence },
          config,
          payload: Buffer.from(payload).toString("base64"),
          output_budget: Math.min(batchOutput, maxOutput - outputBytes),
        });
        busy = true;
        reserved += maxState;
        try {
          const loaded = await executor.load(module, combined);
          if (disposed || combined.aborted) throw new PluginHostError("CALL_CANCELLED");
          const previousConnection = connections.get(binding.id);
          if (previousConnection && previousConnection !== loaded.connection)
            throw new PluginHostError("HELPER_RESTARTED");
          connections.set(binding.id, loaded.connection);
          const reply = await executor.call(
            loaded.hash,
            new TextEncoder().encode(JSON.stringify(input)),
            state,
            {
              signal: combined,
              timeoutMs: Math.max(1, options.deadlineAt - Date.now()),
              function: entrypoint,
            },
          );
          if (disposed || combined.aborted) throw new PluginHostError("CALL_CANCELLED");
          if (reply.state.length > maxState) throw new PluginHostError("STATE_BUDGET");
          let result: AbiResult;
          try {
            result = resultSchema.parse(
              JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(reply.output)),
            );
          } catch {
            throw new PluginHostError("ABI_INVALID");
          }
          if (
            (result.kind === "output" || result.kind === "failure") &&
            result.usage &&
            Object.keys(result.usage).some((key) => !capabilities.dimensions.includes(key as never))
          )
            throw new PluginHostError("ABI_OUTPUT");
          if (result.kind === "failure") {
            if (!frame) dispose();
            return result;
          }
          const expected =
            hook === "describe"
              ? "description"
              : hook === "resource_prepare"
                ? "resource_request"
                : hook === "prepare" || hook === "balance_prepare"
                  ? "prepared"
                  : hook === "stream_dispose"
                    ? "disposed"
                    : "output";
          if (result.kind === "inspect" && (!document || result.document !== document.handle))
            throw new PluginHostError("JSON_HANDLE");
          if (
            result.kind !== expected &&
            result.kind !== "inspect" &&
            !(frame && (result.kind === "buffer_append" || result.kind === "buffer_check"))
          )
            throw new PluginHostError("ABI_RESULT");
          if (
            result.kind === "description" &&
            canonical(result.capabilities) !== canonical(capabilities)
          )
            throw new PluginHostError("CAPABILITIES_MISMATCH");
          if (
            result.kind === "resource_request" &&
            (!capabilities.resources || !capabilities.auth.includes(result.request.auth.type))
          )
            throw new PluginHostError("PREPARE_INVALID");
          if (result.kind === "prepared") {
            if (
              !capabilities.operations.includes(result.request.manifest.operation) ||
              !capabilities.auth.includes(result.request.auth.type) ||
              result.request.manifest.input_bytes !== (document?.bytes ?? payload.length) ||
              [
                ...Object.keys(result.request.manifest.quantities ?? {}),
                ...Object.keys(result.request.binary?.usage ?? {}),
              ].some((key) => !capabilities.dimensions.includes(key as never)) ||
              (result.request.manifest.images !== undefined &&
                result.request.manifest.ceilings.images !== result.request.manifest.images)
            )
              throw new PluginHostError("PREPARE_INVALID");
            if (
              ["chat", "responses", "messages", "gemini", "completions"].includes(
                result.request.manifest.operation,
              ) &&
              !result.request.manifest.ceilings.output_tokens
            )
              throw new PluginHostError("OUTPUT_CEILING_REQUIRED");
          }
          if (result.kind === "output") {
            if (
              (result.body && (!document || result.body.document !== document.handle)) ||
              (document && !frame && (!result.body || result.chunks.length))
            )
              throw new PluginHostError("JSON_HANDLE");
            const bytes =
              result.chunks.reduce((sum, value) => sum + Buffer.byteLength(value, "base64"), 0) +
              (result.terminal ? Buffer.byteLength(result.terminal.bytes, "base64") : 0);
            if (
              result.consumed > payload.length ||
              (hook !== "stream_feed" && result.consumed !== 0) ||
              bytes > input.output_budget ||
              (result.resource !== undefined && !capabilities.resources) ||
              (result.terminal?.kind === "pending" &&
                (frame || !result.resource || result.finality === "FINAL")) ||
              outputBytes + bytes > maxOutput ||
              (result.finality === "FINAL" &&
                (!result.usage || !Object.keys(result.usage).length)) ||
              (result.usage &&
                Object.keys(result.usage).some(
                  (key) => !capabilities.dimensions.includes(key as never),
                ))
            )
              throw new PluginHostError("ABI_OUTPUT");
            outputBytes += bytes;
            if (result.terminal) finished = true;
          }
          retained -= state?.length ?? 0;
          state = reply.state;
          retained += state.length;
          sequence++;
          if (hook === "stream_open") stream = true;
          if ((hook === "stream_finish" && !frame) || hook === "stream_dispose") dispose();
          return result;
        } catch (error) {
          dispose();
          throw error;
        } finally {
          reserved -= maxState;
          busy = false;
        }
      }
      function takeOutput(bytes: number) {
        if (outputBytes + bytes > maxOutput) throw new PluginHostError("OUTPUT_BUDGET");
        outputBytes += bytes;
      }
      if (capabilities.stream_codec !== "none")
        sse = createSSEStream({
          owner: binding.id,
          documents,
          call: invokeRaw,
          sequence: () => sequence,
          disposed: () => disposed,
          dispose,
          takeOutput,
          ...(options.maxEventBytes ? { maxEventBytes: options.maxEventBytes } : {}),
          maxInputBytes: batchInput,
          maxOutputBytes: maxOutput,
          ...(capabilities.stream_codec === "aws-eventstream"
            ? { decoder: createAwsEventStreamDecoder(options.maxEventBytes) }
            : {}),
        });
      return {
        id: binding.id,
        onDispose(listener: () => void) {
          if (disposed) {
            listener();
            return () => {};
          }
          if (observers.size >= 16) throw new PluginHostError("OBSERVER_BUDGET");
          observers.add(listener);
          return () => {
            observers.delete(listener);
          };
        },
        async invoke(hook: Hook, payload?: Uint8Array, signal?: AbortSignal) {
          if (operationBusy) throw new PluginHostError("BATCH_CONCURRENT");
          operationBusy = true;
          try {
            return await (sse && hook.startsWith("stream_")
              ? sse.invoke(hook, payload ?? new Uint8Array(), signal)
              : invokeRaw(hook, payload, signal));
          } finally {
            operationBusy = false;
          }
        },
        releaseOutput: (id: string) => sse?.release(id) ?? false,
        async transformJSON(
          hook: "prepare" | "response" | "error" | "resource_response",
          metadata: Record<string, unknown>,
          body: Uint8Array,
          signal?: AbortSignal,
        ): Promise<{ result: AbiResult; body?: Uint8Array; bodyDigest?: string }> {
          if (disposed || operationBusy || jsonBusy || busy)
            throw new PluginHostError(disposed ? "ATTEMPT_CLOSED" : "BATCH_CONCURRENT");
          jsonBusy = true;
          operationBusy = true;
          let indexed = false;
          try {
            if (signal?.aborted) throw new PluginHostError("CALL_CANCELLED");
            const safeMetadata = jsonMetadataSchema.parse(metadata);
            const document = documents.register(binding.id, body);
            indexed = true;
            const inspection: Record<string, JSONFact> = Object.create(null) as Record<
              string,
              JSONFact
            >;
            for (let round = 0; round <= 4; round++) {
              const result = await invokeRaw(
                hook,
                new TextEncoder().encode(JSON.stringify({ ...safeMetadata, document, inspection })),
                signal,
                document,
              );
              if (result.kind === "inspect") {
                if (round === 4) throw new PluginHostError("JSON_INSPECTION_BUDGET");
                if (result.pointers.some((pointer) => Object.hasOwn(inspection, pointer)))
                  throw new PluginHostError("JSON_INSPECTION_REPEAT");
                Object.assign(
                  inspection,
                  documents.inspect(binding.id, document.handle, result.pointers),
                );
                continue;
              }
              if (result.kind === "failure") return { result };
              const plan =
                result.kind === "prepared"
                  ? result.request.body
                  : result.kind === "output"
                    ? result.body
                    : undefined;
              if (!plan || plan.document !== document.handle)
                throw new PluginHostError("JSON_HANDLE");
              let materialized: ReturnType<typeof documents.materialize>;
              try {
                materialized = documents.materialize(binding.id, plan);
              } catch (error) {
                if (error instanceof PluginHostError && error.code === "JSON_COMPOSE_SOURCE")
                  return {
                    result: {
                      abi_version: 1,
                      kind: "failure",
                      status: hook === "prepare" ? 400 : 502,
                      code: "invalid_json",
                    },
                  };
                throw error;
              }
              if (Date.now() >= options.deadlineAt || signal?.aborted || disposed)
                throw new PluginHostError("DEADLINE");
              if (outputBytes + materialized.bytes.length > maxOutput)
                throw new PluginHostError("OUTPUT_BUDGET");
              outputBytes += materialized.bytes.length;
              return { result, body: materialized.bytes, bodyDigest: materialized.digest };
            }
            throw new PluginHostError("JSON_INSPECTION_BUDGET");
          } catch (error) {
            dispose();
            if (
              !indexed &&
              (error instanceof TypeError ||
                (error instanceof PluginHostError &&
                  [
                    "JSON_INVALID",
                    "JSON_DUPLICATE_KEY",
                    "JSON_NUMERIC_LIMIT",
                    "JSON_LIMIT",
                    "JSON_BODY_BUDGET",
                  ].includes(error.code)))
            )
              return {
                result: {
                  abi_version: 1,
                  kind: "failure",
                  status:
                    error instanceof PluginHostError && error.code === "JSON_BODY_BUDGET"
                      ? 413
                      : hook === "prepare"
                        ? 400
                        : 502,
                  code:
                    error instanceof PluginHostError && error.code === "JSON_NUMERIC_LIMIT"
                      ? "numeric_limit"
                      : "invalid_json",
                },
              };
            throw error;
          } finally {
            jsonBusy = false;
            operationBusy = false;
          }
        },
        dispose,
        snapshot: () => ({
          sequence,
          stateBytes: state?.length ?? 0,
          outputBytes,
          disposed,
          busy,
          ...(sse ? { stream: sse.snapshot() } : {}),
        }),
      };
    },
    snapshot: () => ({
      attempts: live.size,
      stateBytes: retained,
      reservedStateBytes: reserved,
      moduleBytes: [...modules.values()].reduce((n, item) => n + item.byteLength, 0),
      json: documents.snapshot(),
      closed,
    }),
    async close() {
      dispose();
      const results = await Promise.allSettled([...executors].map((executor) => executor.close()));
      const failure = results.find((result) => result.status === "rejected");
      if (failure?.status === "rejected") throw failure.reason;
    },
  };
}
