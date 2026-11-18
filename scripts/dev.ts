import { chmod, mkdir } from "node:fs/promises";
import { createMockApp } from "../apps/mock-providers/src/app.ts";
import { withFinancial } from "../tests/support/financial-execution.ts";
import {
  clearQueueNamespace,
  startCoreWorker,
  startGateway,
  startTlsUpstream,
  withSettlementRole,
} from "../tests/support/gateway-process.ts";

if (process.env.NODE_ENV === "production")
  throw new Error("The development stack is forbidden in production");
const { app } = createMockApp(crypto.randomUUID());
const stopped = Promise.withResolvers<void>();
for (const signal of ["SIGINT", "SIGTERM"] as const) process.once(signal, () => stopped.resolve());
await withFinancial((f, db) =>
  withSettlementRole(db, async (workerDatabaseUrl) => {
    const upstream = await startTlsUpstream((request) => {
      const url = new URL(request.url);
      url.pathname = `/openai${url.pathname}`;
      return app.fetch(new Request(url, request));
    });
    const namespace = `dev_${crypto.randomUUID().replaceAll("-", "")}`;
    try {
      const setup = await f.bootstrap(
        "USD",
        "1000000000000",
        100,
        "PROVIDER_USAGE",
        {
          rates: {
            input_tokens: { numerator: "1", denominator: "3" },
            output_tokens: { numerator: "1", denominator: "3" },
          },
          ceilings: { input_tokens: "10000", output_tokens: "100" },
        },
        { baseUrl: upstream.baseUrl },
      );
      const credit = await f.request(`/projects/${setup.project.id}/credits`, {
        external_ref: `dev-${namespace}`,
        currency: "USD",
        amount_atoms: "1000000000000",
        kind: "CASH",
      });
      if (credit.status !== 201) throw new Error(`Development credit: ${credit.status}`);
      for (const [path, limits] of [
        [`/providers/${setup.provider.id}`, { concurrency: 1000, rpm: 1000000 }],
        [`/projects/${setup.project.id}`, { concurrency: 1000, rpm: 1000000, tpm: 1000000000 }],
      ] as const) {
        const patched = await f.request(path, limits, undefined, "PATCH");
        if (patched.status !== 200) throw new Error(`Development limits: ${patched.status}`);
      }
      const worker = await startCoreWorker(namespace, {
        databaseUrl: workerDatabaseUrl,
        identity: f.identity,
      });
      const gateway = await startGateway(f, {
        namespace,
        upstreamCertificate: upstream.certificate,
      });
      try {
        await mkdir(new URL("../.local/", import.meta.url), { recursive: true });
        const path = new URL("../.local/client.env", import.meta.url);
        await Bun.write(
          path,
          `GATEWAY_URL=${gateway.origin}\nGATEWAY_API_KEY=${setup.key.key}\nGATEWAY_MODEL=${setup.model}\nGATEWAY_PROJECT_ID=${setup.project.id}\n`,
        );
        await chmod(path, 0o600);
        console.log(
          JSON.stringify({
            gateway: gateway.origin,
            model: setup.model,
            credential: ".local/client.env",
          }),
        );
        await stopped.promise;
        await gateway.stop();
        await worker.stop();
      } finally {
        gateway.kill();
        worker.kill();
      }
    } finally {
      await upstream.stop();
      await clearQueueNamespace(namespace);
    }
  }),
);
