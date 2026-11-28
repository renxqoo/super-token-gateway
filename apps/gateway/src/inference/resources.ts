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

export class AttemptLifetime {
  private attemptAborted = false;
  private attemptAbortReason: unknown;
  private attemptCancelCb: ((reason: unknown) => void) | null = null;
  private lazyAttemptController: AbortController | null = null;
  private attemptDisposed = false;
  private watchingReqSignal = false;
  private unsubPlugin: (() => void) | undefined;
  private postDispatchGraceMs = 0;
  private readonly attemptDeadlineMs: number;
  private readonly cancelTimeoutFn: () => void;
  private cancelGraceTimeoutFn: (() => void) | undefined;

  constructor(
    private readonly owner: InferenceResources,
    private readonly requestSignal: AbortSignal,
    pluginSignal: AbortSignal,
    attemptTimeoutMs: number,
    watchPlugin: boolean,
    private readonly state: ProxyRuntimeState,
  ) {
    this.attemptDeadlineMs = performance.now() + attemptTimeoutMs;
    if (owner.isDraining()) {
      this.abort(new Error("Gateway shutting down"));
    } else if (owner.quotaLost(true)) {
      this.abort(new Error("Shared concurrency permit could not be renewed"));
    } else if (requestSignal.aborted) {
      this.abort(requestSignal.reason);
    } else if (pluginSignal.aborted) {
      this.abort(pluginSignal.reason);
    }
    if (!this.attemptAborted) {
      this.watchingReqSignal = true;
      requestSignal.addEventListener("abort", this.onReqAbort, { once: true });
      if (watchPlugin) this.unsubPlugin = subscribeSharedAbort(pluginSignal, this.abort);
      owner.setAbortNotify(this.abort);
    }
    this.cancelTimeoutFn = state.deadlines.schedule(attemptTimeoutMs, () =>
      this.abort(new Error("attempt_timeout")),
    );
  }

  enablePostDispatchClientAbortGrace(graceMs: number): void {
    if (graceMs <= 0 || this.attemptAborted || this.attemptDisposed) return;
    this.postDispatchGraceMs = graceMs;
  }

  get hasPostDispatchGrace(): boolean {
    return this.postDispatchGraceMs > 0;
  }

  private readonly onReqAbort = (): void => {
    if (this.postDispatchGraceMs > 0) {
      const remainingMs = this.attemptDeadlineMs - performance.now();
      if (this.postDispatchGraceMs < remainingMs && !this.cancelGraceTimeoutFn) {
        this.cancelGraceTimeoutFn = this.state.deadlines.schedule(this.postDispatchGraceMs, () =>
          this.abort(new GatewayError(499, "client_aborted", "Client disconnected")),
        );
      }
      return;
    }
    this.abort(this.requestSignal.reason);
  };

  get aborted(): boolean {
    return this.attemptAborted;
  }

  get reason(): unknown {
    return this.attemptAbortReason;
  }

  readonly abort = (reason: unknown): void => {
    if (this.attemptAborted) return;
    this.attemptAborted = true;
    this.attemptAbortReason = reason;
    const cb = this.attemptCancelCb;
    this.attemptCancelCb = null;
    if (cb) cb(reason);
    this.lazyAttemptController?.abort(reason);
  };

  signal(): AbortSignal {
    if (!this.lazyAttemptController) {
      this.lazyAttemptController = new AbortController();
      if (this.attemptAborted) this.lazyAttemptController.abort(this.attemptAbortReason);
    }
    return this.lazyAttemptController.signal;
  }

  cancelTimeout(): void {
    this.cancelTimeoutFn();
    this.cancelGraceTimeoutFn?.();
    this.cancelGraceTimeoutFn = undefined;
  }

  readonly onCancelRegister: CancelRegistrar = (fn) => {
    if (this.attemptAborted) {
      fn(this.attemptAbortReason);
      return () => {};
    }
    this.attemptCancelCb = fn;
    return () => {
      if (this.attemptCancelCb === fn) this.attemptCancelCb = null;
    };
  };

  readonly dispose = (): void => {
    this.cancelTimeoutFn();
    this.cancelGraceTimeoutFn?.();
    this.cancelGraceTimeoutFn = undefined;
    if (!this.attemptDisposed) {
      this.attemptDisposed = true;
      this.attemptCancelCb = null;
      if (this.watchingReqSignal) {
        this.watchingReqSignal = false;
        this.requestSignal.removeEventListener("abort", this.onReqAbort);
      }
      if (this.unsubPlugin) {
        this.unsubPlugin();
        this.unsubPlugin = undefined;
      }
      this.owner.clearAbortNotifyIf(this.abort);
    }
    this.owner.clearAttemptIf(this);
  };

  finishExchange(): void {
    this.dispose();
    this.owner.clearChannelLimit();
  }
}

export class InferenceResources {
  private drainAborted = false;
  private billingAccountPermitRevoked = false;
  private channelPermitRevoked = false;
  private activeAbortNotify: ((reason: unknown) => void) | null = null;
  private releaseGuard: (() => void) | undefined;
  private releaseBodyBuffer: (() => void) | undefined;
  private releaseLocalAccount: (() => void) | undefined;
  private releaseAccountLimit: (() => void) | undefined;
  private releaseChannelLimit: (() => void) | undefined;
  private releaseOutputAdmission: (() => void) | undefined;
  private releaseVideoAdmission: (() => Promise<void>) | undefined;
  private retainVideoAdmission = false;
  private activeAttempt: AttemptLifetime | undefined;
  private handedToStream = false;

  constructor(
    private readonly deps: ProxyDependencies,
    private readonly state: ProxyRuntimeState,
    private readonly requestSignal: AbortSignal,
  ) {}

  enter(): void {
    this.releaseGuard = this.deps.lifecycle.enterCallback(() => {
      this.drainAborted = true;
      this.activeAbortNotify?.(new Error("Gateway shutting down"));
    }).release;
  }

  get streamOwnsRequest(): boolean {
    return this.handedToStream;
  }

  readonly isDraining = (): boolean => this.drainAborted;

  readonly setAbortNotify = (fn: ((reason: unknown) => void) | null): void => {
    this.activeAbortNotify = fn;
  };

  clearAbortNotifyIf(fn: (reason: unknown) => void): void {
    if (this.activeAbortNotify === fn) this.activeAbortNotify = null;
  }

  clearAttemptIf(attempt: AttemptLifetime): void {
    if (this.activeAttempt === attempt) this.activeAttempt = undefined;
  }

  setBodyBuffer(release: () => void): void {
    this.releaseBodyBuffer = release;
  }

  setLocalAccount(release: () => void): void {
    this.releaseLocalAccount = release;
  }

  setAccountLimit(release: () => void): void {
    this.releaseAccountLimit = release;
  }

  setChannelLimit(release: () => void): void {
    this.releaseChannelLimit = release;
  }

  setVideoAdmission(release: () => Promise<void>): void {
    this.releaseVideoAdmission = release;
  }

  get videoAdmissionRetained(): boolean {
    return this.retainVideoAdmission;
  }

  setVideoAdmissionRetained(retain: boolean): void {
    this.retainVideoAdmission = retain;
  }

  setOutputAdmission(release: (() => void) | undefined): void {
    this.releaseOutputAdmission = release;
  }

  clearChannelLimit(): void {
    this.releaseChannelLimit?.();
    this.releaseChannelLimit = undefined;
  }

  clearOutputAdmission(): void {
    this.releaseOutputAdmission?.();
    this.releaseOutputAdmission = undefined;
  }

  resetChannelPermit(): void {
    this.channelPermitRevoked = false;
  }

  readonly revokeAccountPermit = (): void => {
    this.billingAccountPermitRevoked = true;
    this.activeAbortNotify?.(new Error("Shared concurrency permit could not be renewed"));
  };

  readonly revokeChannelPermit = (): void => {
    this.channelPermitRevoked = true;
    this.activeAbortNotify?.(new Error("Shared concurrency permit could not be renewed"));
  };

  quotaLost(includeChannel = false): boolean {
    return this.billingAccountPermitRevoked || (includeChannel && this.channelPermitRevoked);
  }

  interrupted(includeChannel = false): boolean {
    return this.requestSignal.aborted || this.drainAborted || this.quotaLost(includeChannel);
  }

  serverInterrupted(includeChannel = false): boolean {
    return this.drainAborted || this.quotaLost(includeChannel);
  }

  admissionError(quotaLost: boolean): GatewayError {
    return this.requestSignal.aborted
      ? new GatewayError(499, "client_aborted", "Client disconnected")
      : this.serverAdmissionError(quotaLost);
  }

  serverAdmissionError(quotaLost: boolean): GatewayError {
    return quotaLost
      ? new GatewayError(503, "rate_limit_unavailable", "Shared concurrency permit was revoked")
      : new GatewayError(503, "overloaded", "Gateway is draining");
  }

  link(
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
    if (this.drainAborted) {
      trigger(new Error("Gateway shutting down"));
    } else if (
      this.billingAccountPermitRevoked ||
      (includeChannelPermit && this.channelPermitRevoked)
    ) {
      trigger(new Error("Shared concurrency permit could not be renewed"));
    }
    if (!controller.signal.aborted) {
      for (let i = 0; i < sources.length; i++) {
        const src = sources[i];
        if (!src || this.deps.snapshot.isNoopPluginSignal(src)) continue;
        if (src.aborted) {
          trigger(src.reason);
          break;
        }
        const unsub =
          src === this.requestSignal
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
      this.activeAbortNotify = trigger;
    }
    const dispose = () => {
      if (disposed) return;
      disposed = true;
      if (unsubscribes) {
        for (let i = 0; i < unsubscribes.length; i++) unsubscribes[i]!();
        unsubscribes = undefined;
      }
      if (attachToRequest && this.activeAbortNotify === trigger) this.activeAbortNotify = null;
    };
    return { signal: controller.signal, trigger, dispose };
  }

  startAttempt(pluginSignal: AbortSignal, attemptTimeoutMs: number): AttemptLifetime {
    const attempt = new AttemptLifetime(
      this,
      this.requestSignal,
      pluginSignal,
      attemptTimeoutMs,
      !this.deps.snapshot.isNoopPluginSignal(pluginSignal),
      this.state,
    );
    this.activeAttempt = attempt;
    return attempt;
  }

  private readonly dispose = (): Promise<void> | undefined => {
    this.activeAttempt?.dispose();
    this.activeAttempt = undefined;
    this.activeAbortNotify = null;
    this.clearChannelLimit();
    this.clearOutputAdmission();
    this.releaseAccountLimit?.();
    this.releaseAccountLimit = undefined;
    this.releaseLocalAccount?.();
    this.releaseLocalAccount = undefined;
    this.releaseBodyBuffer?.();
    this.releaseBodyBuffer = undefined;
    const releaseGuard = this.releaseGuard;
    this.releaseGuard = undefined;
    const releaseVideoAdmission = this.releaseVideoAdmission;
    this.releaseVideoAdmission = undefined;
    if (releaseVideoAdmission && !this.retainVideoAdmission) {
      return releaseVideoAdmission()
        .catch(() => undefined)
        .finally(releaseGuard);
    }
    releaseGuard?.();
  };

  handToStream(): () => void {
    this.handedToStream = true;
    return () => {
      void this.dispose();
    };
  }

  finishRequest(): Promise<void> | undefined {
    if (!this.handedToStream) return this.dispose();
  }
}

export function createInferenceResources(
  deps: ProxyDependencies,
  state: ProxyRuntimeState,
  requestSignal: AbortSignal,
): InferenceResources {
  return new InferenceResources(deps, state, requestSignal);
}
