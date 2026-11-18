import { expect, test } from "bun:test";
import type { SQL } from "bun";
import { withFinancial } from "../support/financial-execution.ts";

function candidateReadFlow(db: SQL, model: string) {
  const ready = Promise.withResolvers<void>();
  let stopped = false;
  function hold() {
    const entered = Promise.withResolvers<void>(),
      released = Promise.withResolvers<void>();
    const done = db.begin(async (tx) => {
      await tx`SELECT core_candidates_v1(${model},'chat','USD')`;
      entered.resolve();
      await released.promise;
    });
    void done.catch(entered.reject);
    return { ready: entered.promise, release: () => released.resolve(), done };
  }
  const running = (async () => {
    let current: ReturnType<typeof hold> | undefined;
    try {
      while (!stopped) {
        const next = hold();
        try {
          await next.ready;
        } catch (error) {
          await next.done.catch(() => {});
          if (current) {
            current.release();
            await current.done;
            current = undefined;
          }
          if (!["55P03", "P0503"].includes(String((error as { errno?: string }).errno)))
            throw error;
          await Bun.sleep(1);
          continue;
        }
        if (current) {
          current.release();
          await current.done;
        }
        current = next;
        ready.resolve();
        await Bun.sleep(5);
      }
    } finally {
      if (current) {
        current.release();
        await current.done;
      }
    }
  })();
  void running.catch(ready.reject);
  return {
    ready: ready.promise,
    async stop() {
      stopped = true;
      await running;
    },
  };
}

for (const operation of ["create", "update"] as const)
  test(
    `route ${operation} completes under an unbroken flow of warm candidate readers instead of timing out`,
    () =>
      withFinancial(async (f, db) => {
        const setup = await f.bootstrap();
        await db`SELECT core_candidates_v1(${setup.model},'chat','USD')`;
        const before = Number((await db`SELECT count(*) n FROM cost_versions`)[0].n);
        const readers = candidateReadFlow(db, setup.model);
        let target: string;
        try {
          await readers.ready;
          const response =
            operation === "create"
              ? await f.request(`/models/${setup.model}/routes`, {
                  provider_id: setup.provider.id,
                  upstream_model: "synthetic",
                  operations: ["chat", "responses"],
                  pricing: {
                    currency: "USD",
                    rates: {
                      input_tokens: { numerator: "1", denominator: "3" },
                      output_tokens: { numerator: "1", denominator: "3" },
                    },
                  },
                  ceilings: { input_tokens: "100", output_tokens: "50" },
                })
              : await f.request(
                  `/routes/${setup.route.id}`,
                  { enabled: false },
                  undefined,
                  "PATCH",
                );
          expect(response.status).toBe(operation === "create" ? 201 : 200);
          target = operation === "create" ? String(response.value.id) : String(setup.route.id);
        } finally {
          await readers.stop();
        }
        const [route] = await db`SELECT enabled FROM routes WHERE id=${target}`;
        expect(route.enabled).toBe(operation === "create");
        expect(
          Number(
            (
              await db`SELECT count(*) n FROM audit_events WHERE resource_id=${target} AND action=${`route.${operation}`}`
            )[0].n,
          ),
        ).toBe(1);
        expect(Number((await db`SELECT count(*) n FROM cost_versions`)[0].n)).toBe(
          before + (operation === "create" ? 1 : 0),
        );
        expect(
          (
            await db`SELECT jsonb_array_length(core_candidates_v1(${setup.model},'chat','USD')) n`
          )[0].n,
        ).toBe(operation === "create" ? 2 : 0);
      }),
    30000,
  );
