import { GatewayError } from "@gateway/contracts";
import type { CancelRegistrar } from "@gateway/runtime";
import { ProxyRuntimeState } from "../proxy-state.ts";
import type { ProxyDependencies } from "./types.ts";

const sharedAbortSubscribers = new WeakMap<AbortSignal, Set<(reason: unknown) => void>>();

function subscribeSharedAbort(signal: AbortSignal, fn: (reason: unknown) => void): () => void {
  if (signal.aborted) {
    fn(signal.reason);
    return () => {};
  }
  let subs = sharedAbortSubscribers.get(signal);
  if (!subs) {
    subs = new Set();
    sharedAbortSubscribers.set(signal, subs);
    const active = subs;
    signal.addEventListener(
      "abort",
      () => {
        const reason = signal.reason;
        for (const listener of active) listener(reason);
        active.clear();
      },
      { once: true },
    );
  }
  subs.add(fn);
  return () => {
    subs!.delete(fn);
  };
}

export function createInferenceResources(
  deps: ProxyDependencies,
  state: ProxyRuntimeState,
  requestSignal: AbortSignal,
) {
  let drainAborted = false;
  let billingAccountPermitRevoked = false;
  let channelPermitRevoked = false;
  let activeAbortNotify: ((reason: unknown) => void) | null = null;
  let releaseGuard: (() => void) | undefined;
  let releaseBodyBuffer: (() => void) | undefined;
  let releaseLocalAccount: (() => void) | undefined;
  let releaseAccountLimit: (() => void) | undefined;
  let releaseChannelLimit: (() => void) | undefined;
  let releaseOutputAdmission: (() => void) | undefined;
  let disposeAttempt: (() => void) | undefined;
  let handedToStream = false;

  function dispose() {
    disposeAttempt?.();
    disposeAttempt = undefined;
    activeAbortNotify = null;
    clearChannelLimit();
    clearOutputAdmission();
    releaseAccountLimit?.();
    releaseAccountLimit = undefined;
    releaseLocalAccount?.();
    releaseLocalAccount = undefined;
    releaseBodyBuffer?.();
    releaseBodyBuffer = undefined;
    releaseGuard?.();
    releaseGuard = undefined;
  }

  function clearChannelLimit() {
    releaseChannelLimit?.();
    releaseChannelLimit = undefined;
  }

  function clearOutputAdmission() {
    releaseOutputAdmission?.();
    releaseOutputAdmission = undefined;
  }

  function link(
    sources: readonly (AbortSignal | undefined)[],
    includeChannelPermit = false,
    attachToRequest = true,
  ) {
    const controller = new AbortController();
    let disposed = false;
    let unsubscribes: (() => void)[] | undefined;
    const trigger = (reason: unknown) => {
      if (!controller.signal.aborted) controller.abort(reason);
    };
    if (drainAborted) {
      trigger(new Error("Gateway shutting down"));
    } else if (billingAccountPermitRevoked || (includeChannelPermit && channelPermitRevoked)) {
      trigger(new Error("Shared concurrency permit could not be renewed"));
    }
    if (!controller.signal.aborted) {
      for (let i = 0; i < sources.length; i++) {
        const src = sources[i];
        if (!src || deps.snapshot.isNoopPluginSignal(src)) continue;
        if (src.aborted) {
          trigger(src.reason);
          break;
        }
        const unsub =
          src === requestSignal
            ? (() => {
                const onAbort = () => trigger(src.reason);
                src.addEventListener("abort", onAbort, { once: true });
                return () => src.removeEventListener("abort", onAbort);
              })()
            : subscribeSharedAbort(src, trigger);
        if (!unsubscribes) unsubscribes = [unsub];
        else unsubscribes.push(unsub);
      }
    }
    if (!controller.signal.aborted && attachToRequest) {
      activeAbortNotify = trigger;
    }
    const dispose = () => {
      if (disposed) return;
      disposed = true;
      if (unsubscribes) {
        for (let i = 0; i < unsubscribes.length; i++) unsubscribes[i]!();
        unsubscribes = undefined;
      }
      if (attachToRequest && activeAbortNotify === trigger) activeAbortNotify = null;
    };
    return { signal: controller.signal, trigger, dispose };
  }

  function startAttempt(pluginSignal: AbortSignal, attemptTimeoutMs: number) {
    let attemptAborted = false;
    let attemptAbortReason: unknown;
    let attemptCancelCb: ((reason: unknown) => void) | null = null;
    let lazyAttemptController: AbortController | null = null;
    let attemptDisposed = false;
    const triggerAttemptAbort = (reason: unknown) => {
      if (attemptAborted) return;
      attemptAborted = true;
      attemptAbortReason = reason;
      const cb = attemptCancelCb;
      attemptCancelCb = null;
      if (cb) cb(reason);
      lazyAttemptController?.abort(reason);
    };
    if (drainAborted) {
      triggerAttemptAbort(new Error("Gateway shutting down"));
    } else if (billingAccountPermitRevoked || channelPermitRevoked) {
      triggerAttemptAbort(new Error("Shared concurrency permit could not be renewed"));
    } else if (requestSignal.aborted) {
      triggerAttemptAbort(requestSignal.reason);
    } else if (pluginSignal.aborted) {
      triggerAttemptAbort(pluginSignal.reason);
    }
    const onReqAbort = () => triggerAttemptAbort(requestSignal.reason);
    const watchPlugin = !deps.snapshot.isNoopPluginSignal(pluginSignal);
    let unsubPlugin: (() => void) | undefined;
    if (!attemptAborted) {
      requestSignal.addEventListener("abort", onReqAbort, { once: true });
      if (watchPlugin) unsubPlugin = subscribeSharedAbort(pluginSignal, triggerAttemptAbort);
      activeAbortNotify = triggerAttemptAbort;
    }
    const ensureAttemptSignal = (): AbortSignal => {
      if (!lazyAttemptController) {
        lazyAttemptController = new AbortController();
        if (attemptAborted) lazyAttemptController.abort(attemptAbortReason);
      }
      return lazyAttemptController.signal;
    };
    const attemptLinked = {
      trigger: triggerAttemptAbort,
      dispose() {
        if (attemptDisposed) return;
        attemptDisposed = true;
        attemptCancelCb = null;
        requestSignal.removeEventListener("abort", onReqAbort);
        if (unsubPlugin) {
          unsubPlugin();
          unsubPlugin = undefined;
        }
        if (activeAbortNotify === triggerAttemptAbort) activeAbortNotify = null;
      },
    };
    const cancelTimeout = state.deadlines.schedule(attemptTimeoutMs, () =>
      triggerAttemptAbort(new Error("attempt_timeout")),
    );
    const onAttemptCancelRegister: CancelRegistrar = (fn) => {
      if (attemptAborted) {
        fn(attemptAbortReason);
        return () => {};
      }
      attemptCancelCb = fn;
      return () => {
        if (attemptCancelCb === fn) attemptCancelCb = null;
      };
    };

    function dispose() {
      cancelTimeout();
      attemptLinked.dispose();
      if (disposeAttempt === dispose) disposeAttempt = undefined;
    }
    disposeAttempt = dispose;
    return {
      get aborted() {
        return attemptAborted;
      },
      get reason() {
        return attemptAbortReason;
      },
      abort: triggerAttemptAbort,
      signal: ensureAttemptSignal,
      cancelTimeout,
      onCancelRegister: onAttemptCancelRegister,
      dispose,
      finishExchange() {
        dispose();
        clearChannelLimit();
      },
    };
  }

  return {
    enter() {
      releaseGuard = deps.lifecycle.enterCallback(() => {
        drainAborted = true;
        activeAbortNotify?.(new Error("Gateway shutting down"));
      }).release;
    },
    get streamOwnsRequest() {
      return handedToStream;
    },
    isDraining: () => drainAborted,
    setAbortNotify(fn: ((reason: unknown) => void) | null) {
      activeAbortNotify = fn;
    },
    setBodyBuffer(release: () => void) {
      releaseBodyBuffer = release;
    },
    setLocalAccount(release: () => void) {
      releaseLocalAccount = release;
    },
    setAccountLimit(release: () => void) {
      releaseAccountLimit = release;
    },
    setChannelLimit(release: () => void) {
      releaseChannelLimit = release;
    },
    setOutputAdmission(release: (() => void) | undefined) {
      releaseOutputAdmission = release;
    },
    clearChannelLimit,
    clearOutputAdmission,
    resetChannelPermit() {
      channelPermitRevoked = false;
    },
    revokeAccountPermit() {
      billingAccountPermitRevoked = true;
      activeAbortNotify?.(new Error("Shared concurrency permit could not be renewed"));
    },
    revokeChannelPermit() {
      channelPermitRevoked = true;
      activeAbortNotify?.(new Error("Shared concurrency permit could not be renewed"));
    },
    quotaLost(includeChannel = false) {
      return billingAccountPermitRevoked || (includeChannel && channelPermitRevoked);
    },
    interrupted(includeChannel = false) {
      return requestSignal.aborted || drainAborted || this.quotaLost(includeChannel);
    },
    admissionError(quotaLost: boolean) {
      return requestSignal.aborted
        ? new GatewayError(499, "client_aborted", "Client disconnected")
        : quotaLost
          ? new GatewayError(503, "rate_limit_unavailable", "Shared concurrency permit was revoked")
          : new GatewayError(503, "overloaded", "Gateway is draining");
    },
    link,
    startAttempt,
    handToStream() {
      handedToStream = true;
      return dispose;
    },
    finishRequest() {
      if (!handedToStream) dispose();
    },
  };
}

export type InferenceResources = ReturnType<typeof createInferenceResources>;
export type AttemptLifetime = ReturnType<InferenceResources["startAttempt"]>;
