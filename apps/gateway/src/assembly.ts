import { createCatalogApplication } from "@gateway/core-api/catalog";
import { createDatabase, Store, FundsStore, SCHEMA_VERSION } from "@gateway/storage";
import {
  Lifecycle,
  Limits,
  Metrics,
  MemoryBudget,
  UsageProducer,
  createTransport,
} from "@gateway/runtime";
import { Metering } from "./usage.ts";
import type { Config } from "./config.ts";
import type { Dependencies } from "./context.ts";

export async function assemble(config: Config): Promise<Dependencies> {
  const db = createDatabase(config.DATABASE_URL, config.DB_POOL_SIZE);
  const limits = new Limits([config.REDIS_URL, ...config.REDIS_EXTRA_URLS], config.REDIS_NAMESPACE);
  const transport = createTransport(config.UPSTREAM_POOL_SIZE);
  let producer: UsageProducer | undefined;
  try {
    await limits.ping();
    await db`SELECT 1`;
    const schema = await db<
      { version: number }[]
    >`SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1`;
    if (schema.length !== 1 || schema[0]?.version !== SCHEMA_VERSION)
      throw new Error(
        "Incompatible database schema; only the current durable-queue contract is supported",
      );
    producer = new UsageProducer({
      url: config.QUEUE_REDIS_URL,
      namespace: config.REDIS_NAMESPACE,
      replicas: config.QUEUE_REQUIRED_REPLICAS,
      timeoutMs: config.QUEUE_ACK_TIMEOUT_MS,
      batchSize: config.QUEUE_BATCH_SIZE,
      batchDelayMs: config.QUEUE_BATCH_DELAY_MS,
      maxPendingJobs: config.QUEUE_MAX_PENDING_JOBS,
      maxInFlight: config.MAX_INFLIGHT,
      requireAllReplicas: config.NODE_ENV === "production",
    });
    const pluginSettings = [
      config.PLUGIN_RELEASE_POLICY,
      config.PLUGIN_RELEASE_DIRECTORY,
      config.PLUGIN_HELPER_EXECUTABLE,
    ];
    if (pluginSettings.some(Boolean) && !pluginSettings.every(Boolean))
      throw new Error("All plugin catalog trust settings are required");
    const dependencies: Dependencies = {
      ...(config.PLUGIN_RELEASE_POLICY &&
      config.PLUGIN_RELEASE_DIRECTORY &&
      config.PLUGIN_HELPER_EXECUTABLE
        ? {
            catalog: createCatalogApplication({
              db,
              adminKey: config.ADMIN_KEY,
              pepper: config.KEY_PEPPER,
              policyFile: config.PLUGIN_RELEASE_POLICY,
              releaseDirectory: config.PLUGIN_RELEASE_DIRECTORY,
              helperExecutable: config.PLUGIN_HELPER_EXECUTABLE,
            }),
          }
        : {}),
      config,
      usage: new Metering(producer),
      database: {
        ping: async () => {
          await db`SELECT 1`;
        },
        close: async (timeout = 1) => {
          await dependencies.catalog?.close();
          await db.close({ timeout });
        },
      },
      store: new Store(db),
      funds: new FundsStore(db),
      transport,
      limits,
      lifecycle: new Lifecycle(config.MAX_INFLIGHT),
      metrics: new Metrics(),
      memory: new MemoryBudget(config.MAX_BUFFERED_BYTES),
    };
    await dependencies.usage.producer.ready();
    return dependencies;
  } catch (error) {
    producer?.close();
    transport.close();
    limits.close();
    await db.close({ timeout: 1 }).catch(() => undefined);
    throw error;
  }
}
