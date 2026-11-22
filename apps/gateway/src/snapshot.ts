import { GatewayError } from "@gateway/contracts";
import type {
  ChannelRecord,
  ConfigSnapshot,
  EpochStamp,
  RouteRecord,
  createCatalogRepository,
  createGatewayConfigNodeRepository,
} from "@gateway/ledger";
import { verifyPluginIntegrity } from "@gateway/plugin-engine";
import { decrypt } from "@gateway/runtime";

type CatalogRepo = Pick<ReturnType<typeof createCatalogRepository>, "getEpoch" | "loadSnapshot"> &
  Partial<Pick<ReturnType<typeof createCatalogRepository>, "getEpochStamp">>;

export type SnapshotManagerOptions = {
  signingPublicKeyPem?: string;
  onDivergence?: (info: {
    currentEpoch: number;
    remoteEpoch: number;
    currentLineage?: string;
    remoteLineage?: string;
  }) => void;
  onIntegrityFailure?: (pluginId: string, reason: string) => void;
  onSnapshotApplied?: (snapshot: ConfigSnapshot) => void;
};

const NOOP_PLUGIN_SIGNAL = new AbortController().signal;

export class SnapshotManager {
  private current: ConfigSnapshot | null = null;
  private timer: ReturnType<typeof setInterval> | null = null;
  private stopped = false;
  private diverged = false;
  private lastVerifiedAt = 0;
  private pollInFlight: Promise<void> | null = null;
  private refreshing: Promise<ConfigSnapshot> | null = null;
  private reporting: Promise<void> | null = null;
  private reportRequested = false;
  private reportFailed = false;
  private readonly pendingAcknowledgements = new Set<number>();
  private readonly revisionControllers = new Map<string, WeakRef<AbortController>>();
  private readonly signalOwners = new WeakMap<AbortSignal, AbortController>();
  private readonly routeSignals = new WeakMap<RouteRecord, AbortSignal>();
  private readonly sourceSignals = new WeakMap<AbortSignal, AbortSignal[]>();
  private readonly secretCache = new Map<string, { revision: number; secret: string }>();
  private readonly integrityByBuffer = new WeakMap<
    Uint8Array,
    Map<string, { ok: boolean; reason?: string }>
  >();
  private readonly verifiedPluginIds = new Set<string>();

  constructor(
    private readonly catalog: CatalogRepo,
    private readonly encryptionKey: string,
    private readonly pollMs: number,
    private readonly node: {
      id: string;
      repository: ReturnType<typeof createGatewayConfigNodeRepository>;
    },
    private readonly options: SnapshotManagerOptions = {},
  ) {}

  private markDiverged(
    currentEpoch: number,
    remoteEpoch: number,
    currentLineage?: string,
    remoteLineage?: string,
  ): void {
    const first = !this.diverged;
    this.diverged = true;
    if (first) {
      console.error(
        JSON.stringify({
          service: "gateway",
          event: "config_epoch_diverged",
          node_id: this.node.id,
          current_epoch: currentEpoch,
          remote_epoch: remoteEpoch,
          current_lineage: currentLineage ?? null,
          remote_lineage: remoteLineage ?? null,
        }),
      );
    }
    this.options.onDivergence?.({
      currentEpoch,
      remoteEpoch,
      ...(currentLineage !== undefined ? { currentLineage } : {}),
      ...(remoteLineage !== undefined ? { remoteLineage } : {}),
    });
  }

  private verifySnapshotPlugins(snap: ConfigSnapshot): void {
    this.verifiedPluginIds.clear();
    for (const plugin of snap.plugins.values()) {
      if (plugin.kind !== "WASM") continue;
      if (!plugin.wasm_bytes) {
        this.options.onIntegrityFailure?.(plugin.id, "missing_bytes");
        console.error(
          JSON.stringify({
            service: "gateway",
            event: "plugin_integrity_failed",
            plugin_id: plugin.id,
            reason: "missing_bytes",
          }),
        );
        continue;
      }
      const key = `${plugin.id}:${plugin.digest}:${plugin.signature ?? ""}:${this.options.signingPublicKeyPem ?? ""}`;
      let perBuf = this.integrityByBuffer.get(plugin.wasm_bytes);
      const cached = perBuf?.get(key);
      if (cached) {
        if (cached.ok) this.verifiedPluginIds.add(plugin.id);
        continue;
      }
      const result = verifyPluginIntegrity(plugin, this.options.signingPublicKeyPem);
      if (!perBuf) {
        perBuf = new Map();
        this.integrityByBuffer.set(plugin.wasm_bytes, perBuf);
      }
      if (perBuf.size >= 16) perBuf.clear();
      if (result.ok) {
        perBuf.set(key, { ok: true });
        this.verifiedPluginIds.add(plugin.id);
      } else {
        perBuf.set(key, { ok: false, reason: result.reason });
        this.options.onIntegrityFailure?.(plugin.id, result.reason);
        console.error(
          JSON.stringify({
            service: "gateway",
            event: "plugin_integrity_failed",
            plugin_id: plugin.id,
            reason: result.reason,
          }),
        );
      }
    }
  }

  isPluginIntegrityVerified(pluginId: string): boolean {
    return this.verifiedPluginIds.has(pluginId);
  }

  async load(): Promise<ConfigSnapshot> {
    if (this.refreshing) return this.refreshing;
    this.refreshing = this.catalog
      .loadSnapshot(this.current ?? undefined)
      .then(async (snap) => {
        if (this.current) {
          if (snap.epoch < this.current.epoch) {
            this.markDiverged(
              this.current.epoch,
              snap.epoch,
              this.current.lineageToken,
              snap.lineageToken,
            );
            throw new Error("Configuration epoch moved backwards");
          }
          if (
            snap.epoch === this.current.epoch &&
            this.current.lineageToken &&
            snap.lineageToken &&
            snap.lineageToken !== this.current.lineageToken
          ) {
            this.markDiverged(
              this.current.epoch,
              snap.epoch,
              this.current.lineageToken,
              snap.lineageToken,
            );
            throw new Error("Configuration epoch lineage diverged");
          }
          for (const revokedId of this.current.revokedPlugins) {
            if (!snap.revokedPlugins.has(revokedId)) {
              this.markDiverged(
                this.current.epoch,
                snap.epoch,
                this.current.lineageToken,
                snap.lineageToken,
              );
              throw new Error("Configuration revocation history regressed");
            }
          }
        }
        if (this.diverged) {
          throw new Error("Configuration epoch lineage diverged");
        }
        if (this.current?.epoch !== snap.epoch) this.pendingAcknowledgements.add(snap.epoch);
        this.current = snap;
        for (const [id, weak] of this.revisionControllers) {
          const controller = weak.deref();
          if (!controller || snap.revokedPlugins.has(id)) {
            controller?.abort(this.revokedError());
            this.revisionControllers.delete(id);
          }
        }
        this.lastVerifiedAt = performance.now();
        for (const id of this.secretCache.keys()) {
          const ch = snap.channels.get(id);
          if (!ch || ch.config_revision !== this.secretCache.get(id)?.revision) {
            this.secretCache.delete(id);
          }
        }
        this.verifySnapshotPlugins(snap);
        this.options.onSnapshotApplied?.(snap);
        this.requestReport();
        return snap;
      })
      .finally(() => {
        this.refreshing = null;
      });
    return this.refreshing;
  }

  async refreshTo(epoch: number): Promise<void> {
    if (this.stopped || this.diverged || (this.current && this.current.epoch >= epoch)) return;
    const inProgress = this.refreshing !== null;
    const snap = await this.load();
    if (!this.stopped && !this.diverged && inProgress && snap.epoch < epoch) await this.load();
  }

  private revokedError(): GatewayError {
    return new GatewayError(503, "plugin_revoked", "A plugin revision for this route was revoked");
  }

  private revisionSignal(id: string): AbortSignal {
    if (this.current?.revokedPlugins.has(id)) return AbortSignal.abort(this.revokedError());
    const active = this.revisionControllers.get(id)?.deref();
    if (active) return active.signal;
    const controller = new AbortController();
    this.revisionControllers.set(id, new WeakRef(controller));
    this.signalOwners.set(controller.signal, controller);
    return controller.signal;
  }

  isNoopPluginSignal(signal: AbortSignal): boolean {
    return signal === NOOP_PLUGIN_SIGNAL;
  }

  pluginSignal(route: RouteRecord, channel: ChannelRecord): AbortSignal {
    const existing = this.routeSignals.get(route);
    if (existing) return existing;
    const ids = new Set<string>();
    if (channel.provider_plugin_id) ids.add(channel.provider_plugin_id);
    for (const step of route.plugin_pipeline) {
      if (step.enabled !== false) ids.add(step.plugin_id);
    }
    if (ids.size === 0) {
      this.routeSignals.set(route, NOOP_PLUGIN_SIGNAL);
      return NOOP_PLUGIN_SIGNAL;
    }
    const signals = [...ids].map((id) => this.revisionSignal(id));
    const signal = signals.length === 1 ? signals[0]! : AbortSignal.any(signals);
    this.routeSignals.set(route, signal);
    if (signals.length > 1) this.sourceSignals.set(signal, signals);
    return signal;
  }

  private requestReport(): void {
    this.reportRequested = true;
    if (this.stopped || this.diverged || !this.current || this.reporting) return;
    this.reporting = this.report().finally(() => {
      this.reporting = null;
    });
  }

  private async report(): Promise<void> {
    while (this.reportRequested && !this.stopped && !this.diverged && this.current) {
      this.reportRequested = false;
      const acknowledged = [...this.pendingAcknowledgements];
      try {
        await this.node.repository.report(this.node.id, this.current.epoch, acknowledged);
        this.reportFailed = false;
        for (const epoch of acknowledged) this.pendingAcknowledgements.delete(epoch);
      } catch {
        if (!this.reportFailed) {
          console.error(
            JSON.stringify({
              service: "gateway",
              event: "config_node_report_failed",
              node_id: this.node.id,
              epoch: this.current.epoch,
            }),
          );
          this.reportFailed = true;
        }
        return;
      }
    }
  }

  isDiverged(): boolean {
    return this.diverged;
  }

  isFresh(): boolean {
    return this.isLoaded() && !this.diverged && performance.now() - this.lastVerifiedAt <= 6000;
  }

  verifiedAtSeconds(): number {
    return this.current ? (performance.timeOrigin + this.lastVerifiedAt) / 1000 : 0;
  }

  get(): ConfigSnapshot {
    if (!this.current) throw new Error("ConfigSnapshot has not been loaded");
    return this.current;
  }

  isLoaded(): boolean {
    return this.current !== null;
  }

  decryptChannelSecret(channel: ChannelRecord): string {
    const cached = this.secretCache.get(channel.id);
    if (cached && cached.revision === channel.config_revision) {
      return cached.secret;
    }
    const secret = decrypt(channel.secret_ciphertext, this.encryptionKey, channel.id);
    this.secretCache.set(channel.id, { revision: channel.config_revision, secret });
    return secret;
  }

  async start(): Promise<void> {
    this.stopped = false;
    if (this.timer) return;
    this.timer = setInterval(
      () => {
        if (this.pollInFlight) return;
        this.pollInFlight = this.pollEpoch().finally(() => {
          this.pollInFlight = null;
          this.requestReport();
        });
      },
      this.pollMs > 0 ? Math.min(this.pollMs, 2000) : 2000,
    );
    await this.reporting;
  }

  private async readRemoteStamp(): Promise<EpochStamp> {
    if (this.catalog.getEpochStamp) return this.catalog.getEpochStamp();
    return { version: await this.catalog.getEpoch(), lineageToken: "" };
  }

  async pollEpoch(): Promise<void> {
    try {
      const remote = await this.readRemoteStamp();
      if (!this.current) {
        await this.refreshTo(remote.version);
        return;
      }
      if (remote.version < this.current.epoch) {
        this.markDiverged(
          this.current.epoch,
          remote.version,
          this.current.lineageToken,
          remote.lineageToken,
        );
        return;
      }
      if (
        remote.version === this.current.epoch &&
        this.current.lineageToken &&
        remote.lineageToken &&
        remote.lineageToken !== this.current.lineageToken
      ) {
        this.markDiverged(
          this.current.epoch,
          remote.version,
          this.current.lineageToken,
          remote.lineageToken,
        );
        return;
      }
      if (this.diverged) return;
      if (remote.version > this.current.epoch) {
        await this.refreshTo(remote.version);
      } else {
        this.lastVerifiedAt = performance.now();
      }
    } catch {
      void 0;
    }
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    await Promise.allSettled([this.pollInFlight, this.refreshing, this.reporting]);
    if (this.current) await this.node.repository.stop(this.node.id).catch(() => undefined);
  }
}
