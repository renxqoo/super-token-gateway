import { createPluginSupervisor, runPluginFixtures, PluginHostError } from "@gateway/plugin-host";
import type { VerifiedPackage } from "@gateway/plugin-sdk";
import { validationPolicy } from "./releases.ts";
export type ValidationResult = {
  cases: number;
  steps: number;
  cleanup_confirmed: boolean;
  error_code: string | null;
};
export async function validateCatalogPackage(
  pkg: VerifiedPackage,
  executable: string,
  options: {
    signal?: AbortSignal;
    wallMs?: number;
    onResource?: (resource: { pid: number; connection: string }) => Promise<void>;
  } = {},
): Promise<ValidationResult> {
  if (
    options.wallMs !== undefined &&
    (!Number.isSafeInteger(options.wallMs) ||
      options.wallMs < 1 ||
      options.wallMs > validationPolicy.wall_ms)
  )
    throw new PluginHostError("CONFIG_INVALID");
  const supervisor = createPluginSupervisor({
    executable,
    native: validationPolicy.native,
    maxRestarts: 0,
  });
  let cases = 0,
    steps = 0,
    errorCode: string | null = null,
    cleanup = false,
    timedOut = false;
  let closing: Promise<void> | undefined;
  const close = () => (closing ??= supervisor.close());
  const cancel = () => {
    void close().catch(() => undefined);
  };
  const timeout = setTimeout(() => {
    timedOut = true;
    cancel();
  }, options.wallMs ?? validationPolicy.wall_ms);
  options.signal?.addEventListener("abort", cancel, { once: true });
  try {
    if (options.signal?.aborted) throw new PluginHostError("CALL_CANCELLED");
    const execution = runPluginFixtures(supervisor, pkg);
    void execution.catch(() => undefined);
    try {
      const resource = supervisor.snapshot();
      if (options.onResource) {
        if (!resource.pid || !resource.connection)
          throw new PluginHostError("RESOURCE_REGISTRATION_FAILED");
        await options.onResource({ pid: resource.pid, connection: resource.connection });
      }
    } catch (error) {
      await close();
      await execution.catch(() => undefined);
      throw error;
    }
    const result = await execution;
    cases = result.cases;
    steps = result.steps;
    if (options.signal?.aborted) throw new PluginHostError("CALL_CANCELLED");
  } catch (error) {
    errorCode =
      error instanceof PluginHostError && /^[A-Z][A-Z0-9_]{0,79}$/.test(error.code)
        ? error.code
        : "VALIDATION_FAILED";
  } finally {
    clearTimeout(timeout);
    options.signal?.removeEventListener("abort", cancel);
    try {
      await close();
      const state = supervisor.snapshot();
      cleanup =
        state.closed &&
        state.dead &&
        state.pending === 0 &&
        state.pendingBytes === 0 &&
        state.writeBytes === 0 &&
        state.loaded === 0;
    } catch {
      cleanup = false;
    }
  }
  return {
    cases,
    steps,
    cleanup_confirmed: cleanup,
    error_code: cleanup
      ? timedOut
        ? "VALIDATION_TIMEOUT"
        : options.signal?.aborted
          ? "CALL_CANCELLED"
          : errorCode
      : "CLEANUP_PENDING",
  };
}
