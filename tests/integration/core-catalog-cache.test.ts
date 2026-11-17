import { expect, test } from "bun:test";
import { SQL } from "bun";
import { executionContextResponseSchema } from "@gateway/contracts";
import { createSettlementStore } from "@gateway/storage/settlement";
import { event, granted } from "../support/core-settlement.ts";
import { withFinancial } from "../support/financial-execution.ts";

type Fixture = Parameters<Parameters<typeof withFinancial>[0]>[0];
type Setup = Awaited<ReturnType<Fixture["bootstrap"]>>;
const read = async (db: SQL, model: string, operation = "chat", currency = "USD") =>
  (await db`SELECT core_candidates_v1(${model},${operation},${currency}) value`)[0].value as Array<
    Record<string, unknown>
  >;
const epoch = async (db: SQL) =>
  BigInt((await db`SELECT revision::text value FROM core_catalog_cache.epoch`)[0].value);
async function consistent(db: SQL) {
  const value = (
    await db`SELECT u.byte_count::bigint=coalesce(sum(c.payload_bytes),0) bytes,u.entry_count=count(c.scope_digest) entries FROM core_catalog_cache.usage u LEFT JOIN core_catalog_cache.candidates c ON true GROUP BY u.byte_count,u.entry_count`
  )[0];
  expect(value).toEqual({ bytes: true, entries: true });
}

test("cold and warm candidates retain the configured rates, identity and exact snapshot without caching unknown scopes", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap("USD", "1000000", 100, "REQUEST_COMPLETION", {
      rates: {
        input_tokens: { numerator: "2", denominator: "1" },
        output_tokens: { numerator: "3", denominator: "1" },
      },
      ceilings: { input_tokens: "60", output_tokens: "30" },
    });
    const first = await read(db, setup.model);
    expect(first).toHaveLength(1);
    expect(first[0]!.cost_rates).toEqual({
      input_tokens: { numerator: "2", denominator: "1" },
      output_tokens: { numerator: "3", denominator: "1" },
    });
    expect(first[0]!.cost_currency).toBe("USD");
    expect((first[0]!.ref as Record<string, unknown>).channel_id).toBe(setup.provider.id);
    expect(await read(db, setup.model)).toEqual(first);
    const size = (await db`SELECT count(*)::int n FROM core_catalog_cache.candidates`)[0].n;
    for (let n = 0; n < 20; n++)
      expect(await read(db, `missing-${crypto.randomUUID()}`)).toEqual([]);
    expect(await read(db, setup.model, "unknown")).toEqual([]);
    expect(await read(db, setup.model, "chat", "ZZZ")).toEqual([]);
    expect((await db`SELECT count(*)::int n FROM core_catalog_cache.candidates`)[0].n).toBe(size);
    await consistent(db);
  }));

const changes: Array<[string, (db: SQL, setup: Setup, fixture: Fixture) => Promise<unknown>]> = [
  [
    "model output limit",
    (db, s) => db`UPDATE models SET max_output_tokens=max_output_tokens+1 WHERE name=${s.model}`,
  ],
  [
    "model routing strategy",
    (db, s) => db`UPDATE models SET routing_strategy='LEAST_LATENCY' WHERE name=${s.model}`,
  ],
  [
    "route priority",
    (db, s) => db`UPDATE routes SET priority=priority+1 WHERE model_name=${s.model}`,
  ],
  [
    "channel name included by the existing digest",
    (db, s) => db`UPDATE channels SET name=name||'-changed' WHERE id=${s.provider.id}`,
  ],
  [
    "channel origin policy",
    (db, s) => db`UPDATE channels SET base_url=base_url||'/changed' WHERE id=${s.provider.id}`,
  ],
  ["channel request limit", (db, s) => db`UPDATE channels SET rpm=rpm+1 WHERE id=${s.provider.id}`],
  [
    "channel concurrency",
    (db, s) => db`UPDATE channels SET concurrency=concurrency+1 WHERE id=${s.provider.id}`,
  ],
  [
    "brand disable",
    (db, s) => db`UPDATE supplier_brands SET enabled=false WHERE id=${s.provider.brand_id}`,
  ],
  [
    "account disable",
    (db, s) =>
      db`UPDATE upstream_accounts SET state='DISABLED',revision=revision+1 WHERE id=${s.provider.account_id}`,
  ],
  [
    "tariff disable",
    (db, s) =>
      db`UPDATE billing_tariffs SET enabled=false WHERE model=${s.model} AND operation='chat'`,
  ],
  [
    "plugin quarantine",
    async (db, setup, fixture) =>
      fixture.catalog.application.quarantine(
        await fixture.catalog.actor(),
        setup.publication.version_id,
        {
          command_id: crypto.randomUUID(),
          expected_revision: (
            await db`SELECT revision::text value FROM plugin_versions WHERE id=${setup.publication.version_id}`
          )[0].value,
          reason: "Catalog cache quarantine regression",
        },
      ),
  ],
];
for (const [name, change] of changes)
  test(`a committed ${name} invalidates warm candidates in the same transaction`, () =>
    withFinancial(async (f, db) => {
      const setup = await f.bootstrap();
      const first = await read(db, setup.model),
        before = await epoch(db);
      await change(db, setup, f);
      expect(await epoch(db)).toBeGreaterThan(before);
      expect((await db`SELECT count(*)::int n FROM core_catalog_cache.candidates`)[0].n).toBe(0);
      expect(await read(db, setup.model)).not.toEqual(first);
      await consistent(db);
    }));

test("label and balance changes preserve the epoch and successful funding traffic does not invalidate the catalog", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap();
    const first = await read(db, setup.model),
      before = await epoch(db);
    await db`UPDATE upstream_accounts SET name=name||'-label',revision=revision+1 WHERE id=${setup.provider.account_id}`;
    await db`UPDATE supplier_brands SET name=name||'-label' WHERE id=${setup.provider.brand_id}`;
    await db`UPDATE channels SET enabled=enabled WHERE id=${setup.provider.id}`;
    expect(await epoch(db)).toBe(before);
    expect(await read(db, setup.model)).toEqual(first);
    const client = f.client();
    await client.startup();
    const { prepared, owner } = await granted(db, setup, client);
    const result = await createSettlementStore(db).persist([
      {
        events: [
          event(owner, prepared.attemptId, {
            finality: "FINAL",
            attempt_state: "SUCCEEDED",
            request_state: "SUCCEEDED",
            usage: { input_tokens: "7", output_tokens: "5" },
            final_dimensions: ["input_tokens", "output_tokens"],
          }),
        ],
      },
    ]);
    expect(result[0]!.status).toBe("COMMITTED");
    expect(await epoch(db)).toBe(before);
    expect(await read(db, setup.model)).toEqual(first);
    await consistent(db);
  }));

test("warm candidates can pass a nonsemantic account write while cold candidates preserve the original NOWAIT fence", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap();
    const first = await read(db, setup.model);
    const ready = Promise.withResolvers<void>(),
      release = Promise.withResolvers<void>();
    const held = db.begin(async (tx) => {
      await tx`UPDATE upstream_accounts SET name=name||'-held' WHERE id=${setup.provider.account_id}`;
      ready.resolve();
      await release.promise;
    });
    await Promise.race([ready.promise, held]);
    try {
      expect(await read(db, setup.model)).toEqual(first);
      await db.begin(async (tx) => {
        await tx`DELETE FROM core_catalog_cache.candidates`;
        await tx`UPDATE core_catalog_cache.usage SET byte_count=0,entry_count=0`;
      });
      await expect(read(db, setup.model)).rejects.toMatchObject({ errno: "55P03" });
    } finally {
      release.resolve();
      await held;
    }
    expect(await read(db, setup.model)).toEqual(first);
  }));

test("a candidate reader retains its epoch SHARE until commit and a metadata writer waits before its new values become visible", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap();
    const ready = Promise.withResolvers<void>(),
      release = Promise.withResolvers<void>();
    const held = db.begin(async (tx) => {
      expect(await read(tx, setup.model)).toHaveLength(1);
      ready.resolve();
      await release.promise;
    });
    await Promise.race([ready.promise, held]);
    let done = false;
    const writer = Promise.resolve(
      db`UPDATE channels SET enabled=false WHERE id=${setup.provider.id}`,
    ).then(() => {
      done = true;
    });
    try {
      let blocked = false;
      for (let n = 0; n < 100; n++) {
        const rows =
          await db`SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND query LIKE 'UPDATE channels SET enabled=false%' AND wait_event_type='Lock'`;
        if (rows.length) {
          blocked = true;
          break;
        }
        await Bun.sleep(5);
      }
      expect(blocked).toBe(true);
      expect(done).toBe(false);
    } finally {
      release.resolve();
      await Promise.all([held, writer]);
    }
    expect(await read(db, setup.model)).toEqual([]);
  }));

test("writer-first catalog mutation makes a new reader return bounded BUSY before commit then exposes the new state", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap();
    await read(db, setup.model);
    const ready = Promise.withResolvers<void>(),
      release = Promise.withResolvers<void>();
    const writer = db.begin(async (tx) => {
      await tx`UPDATE channels SET enabled=false WHERE id=${setup.provider.id}`;
      ready.resolve();
      await release.promise;
    });
    await Promise.race([ready.promise, writer]);
    try {
      await expect(
        db.begin(async (tx) => {
          await tx`SET LOCAL statement_timeout=500`;
          return read(tx, setup.model);
        }),
      ).rejects.toMatchObject({ errno: "55P03" });
    } finally {
      release.resolve();
      await writer;
    }
    expect(await read(db, setup.model)).toEqual([]);
  }));

test("a concurrent TRUNCATE cannot make a cold epoch reader wait on a source relation", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap();
    const ready = Promise.withResolvers<void>(),
      proceed = Promise.withResolvers<void>();
    const reader = db.begin(async (tx) => {
      await tx`SELECT revision FROM core_catalog_cache.epoch WHERE singleton FOR SHARE`;
      ready.resolve();
      await proceed.promise;
      await tx`SET LOCAL statement_timeout=300`;
      return read(tx, setup.model);
    });
    void reader.catch(() => undefined);
    await Promise.race([ready.promise, reader]);
    const rollback = new Error("rollback catalog truncate");
    const writer = db.begin(async (tx) => {
      await tx`TRUNCATE billing_tariffs`;
      throw rollback;
    });
    void writer.catch(() => undefined);
    try {
      let blocked = false;
      for (let n = 0; n < 100; n++) {
        const rows =
          await db`SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND query='TRUNCATE billing_tariffs' AND wait_event_type='Lock'`;
        if (rows.length) {
          blocked = true;
          break;
        }
        await Bun.sleep(5);
      }
      expect(blocked).toBe(true);
      proceed.resolve();
      await expect(reader).rejects.toMatchObject({ errno: "55P03" });
      await expect(writer).rejects.toBe(rollback);
    } finally {
      proceed.resolve();
      await Promise.allSettled([reader, writer]);
    }
    expect(await read(db, setup.model)).toHaveLength(1);
    await consistent(db);
  }));

test("a committed TRUNCATE invalidates warm tariff candidates and its byte accounting", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap();
    expect(await read(db, setup.model)).toHaveLength(1);
    const before = await epoch(db);
    await db`TRUNCATE billing_tariffs`;
    expect(await epoch(db)).toBeGreaterThan(before);
    expect((await db`SELECT count(*)::int n FROM core_catalog_cache.candidates`)[0].n).toBe(0);
    expect(await read(db, setup.model)).toEqual([]);
    await consistent(db);
  }));

test("same-scope cold contention uses BUSY while another cold scope can compute without waiting for the cache budget lock", () =>
  withFinancial(async (f, db) => {
    const first = await f.bootstrap(),
      second = await f.bootstrap();
    const ready = Promise.withResolvers<void>(),
      release = Promise.withResolvers<void>();
    const held = db.begin(async (tx) => {
      expect(await read(tx, first.model)).toHaveLength(1);
      ready.resolve();
      await release.promise;
    });
    await Promise.race([ready.promise, held]);
    try {
      await expect(
        db.begin(async (tx) => {
          await tx`SET LOCAL statement_timeout=500`;
          return read(tx, first.model);
        }),
      ).rejects.toMatchObject({ errno: "P0503" });
      const other = await db.begin(async (tx) => {
        await tx`SET LOCAL statement_timeout=500`;
        return read(tx, second.model);
      });
      expect(other).toHaveLength(1);
      expect((other[0]!.ref as Record<string, unknown>).channel_id).toBe(second.provider.id);
    } finally {
      release.resolve();
      await held;
    }
    expect(await read(db, second.model)).toHaveLength(1);
    await consistent(db);
  }));

test("a valid context waits through brief cold catalog contention without exposing a transient identity_busy", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const ready = Promise.withResolvers<void>(),
      release = Promise.withResolvers<void>();
    const holder = db.begin(async (tx) => {
      expect(await read(tx, setup.model)).toHaveLength(1);
      ready.resolve();
      await release.promise;
    });
    await Promise.race([ready.promise, holder]);
    let settled = false;
    const request = client
      .execution(
        "execution_context_v1",
        {
          customer_token: setup.key.key,
          ip: "127.0.0.1",
          operation: "chat",
          model: setup.model,
          fingerprint: "0".repeat(64),
        },
        { deadlineAt: new Date(Date.now() + 5000).toISOString() },
      )
      .then(
        (value) => {
          settled = true;
          return value;
        },
        (error: unknown) => {
          settled = true;
          throw error;
        },
      );
    void request.catch(() => undefined);
    try {
      await Bun.sleep(50);
      expect(settled).toBe(false);
    } finally {
      release.resolve();
      await Promise.allSettled([holder, request]);
    }
    const result = executionContextResponseSchema.parse(await request);
    expect(result.candidates).toHaveLength(1);
    expect(await db`SELECT id FROM execution_contexts`).toEqual([{ id: result.context_id }]);
  }));

test("cold context contention stops at the request deadline or retry budget without creating a late context", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const ready = Promise.withResolvers<void>(),
      release = Promise.withResolvers<void>();
    const holder = db.begin(async (tx) => {
      await read(tx, setup.model);
      ready.resolve();
      await release.promise;
    });
    await Promise.race([ready.promise, holder]);
    try {
      for (const deadlineMs of [250, 5000]) {
        const started = performance.now();
        const result = await Promise.allSettled([
          client.execution(
            "execution_context_v1",
            {
              customer_token: setup.key.key,
              ip: "127.0.0.1",
              operation: "chat",
              model: setup.model,
              fingerprint: "0".repeat(64),
            },
            { deadlineAt: new Date(Date.now() + deadlineMs).toISOString() },
          ),
        ]);
        expect(result[0]!.status).toBe("rejected");
        expect(performance.now() - started).toBeGreaterThan(100);
        expect(performance.now() - started).toBeLessThan(Math.min(deadlineMs + 500, 1500));
      }
      expect((await db`SELECT count(*)::int n FROM execution_contexts`)[0].n).toBe(0);
    } finally {
      release.resolve();
      await holder;
    }
    await Bun.sleep(50);
    expect((await db`SELECT count(*)::int n FROM execution_contexts`)[0].n).toBe(0);
  }));

test("disconnecting a cold context request prevents a late context write after its fill lock is released", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      client = f.client();
    await client.startup();
    const ready = Promise.withResolvers<void>(),
      release = Promise.withResolvers<void>();
    const holder = db.begin(async (tx) => {
      await read(tx, setup.model);
      ready.resolve();
      await release.promise;
    });
    await Promise.race([ready.promise, holder]);
    const request = client.execution(
      "execution_context_v1",
      {
        customer_token: setup.key.key,
        ip: "127.0.0.1",
        operation: "chat",
        model: setup.model,
        fingerprint: "0".repeat(64),
      },
      { deadlineAt: new Date(Date.now() + 5000).toISOString() },
    );
    const outcome = Promise.allSettled([request]);
    try {
      let observed = false;
      for (let n = 0; n < 100; n++) {
        if (
          (
            await db`SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND query LIKE 'SELECT core_context_v1%'`
          ).length
        ) {
          observed = true;
          break;
        }
        await Bun.sleep(5);
      }
      expect(observed).toBe(true);
      await client.close();
      expect((await outcome)[0]!.status).toBe("rejected");
      await Bun.sleep(50);
    } finally {
      await client.close();
      release.resolve();
      await Promise.allSettled([holder, outcome]);
    }
    await Bun.sleep(100);
    expect((await db`SELECT count(*)::int n FROM execution_contexts`)[0].n).toBe(0);
  }));

test("metadata rollback restores the epoch and the previously cached snapshot with its byte accounting", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      first = await read(db, setup.model),
      before = await epoch(db);
    const failure = new Error("rollback metadata");
    await expect(
      db.begin(async (tx) => {
        await tx`UPDATE channels SET name=name||'-rollback' WHERE id=${setup.provider.id}`;
        expect(await read(tx, setup.model)).not.toEqual(first);
        throw failure;
      }),
    ).rejects.toBe(failure);
    expect(await epoch(db)).toBe(before);
    expect(await read(db, setup.model)).toEqual(first);
    await consistent(db);
  }));

test("a missing epoch fails closed and runtime cannot read or populate the private candidate cache", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      before = await epoch(db);
    await read(db, setup.model);
    await db`DELETE FROM core_catalog_cache.epoch`;
    try {
      await expect(read(db, setup.model)).rejects.toMatchObject({ errno: "P0002" });
    } finally {
      await db`INSERT INTO core_catalog_cache.epoch(singleton,revision) VALUES(true,${before.toString()}::numeric)`;
    }
    const role = new SQL(f.identity.apiUrl);
    try {
      for (const query of [
        () => role`SELECT * FROM core_catalog_cache.epoch`,
        () => role`UPDATE core_catalog_cache.epoch SET revision=revision+1`,
        () => role`SELECT * FROM core_catalog_cache.candidates`,
        () => role`SELECT core_catalog_cache.compute_v2('unknown','chat','USD')`,
        () => role`CREATE TABLE core_catalog_cache.forged(value int)`,
      ])
        await expect(query()).rejects.toMatchObject({ errno: "42501" });
    } finally {
      await role.close();
    }
  }));

test("route insertion and deletion replace the cached candidate set at the 128 route boundary", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap();
    const first = await read(db, setup.model);
    const original = (first[0]!.ref as Record<string, unknown>).route_id;
    const added =
      await db`INSERT INTO routes SELECT (jsonb_populate_record(NULL::routes,to_jsonb(r)||jsonb_build_object('id',gen_random_uuid(),'upstream_model','catalog-limit-'||n,'priority',r.priority+1000+n))).* FROM routes r CROSS JOIN generate_series(1,128) n WHERE r.id=${original} RETURNING id,priority`;
    const crowded = await read(db, setup.model);
    expect(crowded).toHaveLength(128);
    expect(crowded.some((c) => (c.ref as Record<string, unknown>).route_id === original)).toBe(
      false,
    );
    const highest = [...added].sort((a, b) => b.priority - a.priority)[0]!;
    await db`DELETE FROM routes WHERE id=${highest.id}`;
    const restored = await read(db, setup.model);
    expect(restored).toHaveLength(128);
    expect(restored.some((c) => (c.ref as Record<string, unknown>).route_id === original)).toBe(
      true,
    );
    expect(restored.some((c) => (c.ref as Record<string, unknown>).route_id === highest.id)).toBe(
      false,
    );
    await consistent(db);
  }));

test("provider tariff insertion overrides a warm default and deletion restores that exact default", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap();
    const first = await read(db, setup.model),
      before = await epoch(db);
    const common = await f.request("/tariffs", {
      model: setup.model,
      operation: "chat",
      pricing: { currency: "USD", rates: { requests: { numerator: "3", denominator: "1" } } },
      ceilings: { requests: "1" },
      basis: "REQUEST_COMPLETION",
    });
    expect(common.status).toBe(201);
    expect(await epoch(db)).toBeGreaterThan(before);
    expect(await read(db, setup.model)).toEqual(first);
    await db`DELETE FROM billing_tariffs WHERE model=${setup.model} AND provider_id=${setup.provider.id}`;
    const fallback = await read(db, setup.model);
    expect(fallback[0]!.customer_rates).toEqual({ requests: { numerator: "3", denominator: "1" } });
    const specific = await f.request("/tariffs", {
      model: setup.model,
      operation: "chat",
      provider_id: setup.provider.id,
      pricing: { currency: "USD", rates: { requests: { numerator: "7", denominator: "1" } } },
      ceilings: { requests: "1" },
      basis: "REQUEST_COMPLETION",
    });
    expect(specific.status).toBe(201);
    expect((await read(db, setup.model))[0]!.customer_rates).toEqual({
      requests: { numerator: "7", denominator: "1" },
    });
    await db`DELETE FROM billing_tariffs WHERE model=${setup.model} AND provider_id=${setup.provider.id}`;
    expect(await read(db, setup.model)).toEqual(fallback);
    await consistent(db);
  }));

test("a controlled binding replacement invalidates cached config and its captured version identity", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap(),
      first = await read(db, setup.model),
      before = await epoch(db);
    await f.catalog.application.bind(await f.catalog.actor(), setup.provider.id, {
      ...f.catalog.binding(setup.publication.version_id),
      expected_revision: (
        await db`SELECT config_revision::text value FROM channels WHERE id=${setup.provider.id}`
      )[0].value,
      account_id: setup.provider.account_id,
      brand_id: setup.provider.brand_id,
      secret_reference: `channel:${setup.provider.id}`,
      config: {
        model_map: { [setup.model]: "synthetic" },
        max_output_tokens: 101,
        max_input_tokens: 1000,
        auth: "bearer",
      },
    });
    const changed = await read(db, setup.model);
    expect(await epoch(db)).toBeGreaterThan(before);
    expect((changed[0]!.ref as Record<string, unknown>).binding_id).not.toBe(
      (first[0]!.ref as Record<string, unknown>).binding_id,
    );
    expect((changed[0]!.config as Record<string, unknown>).max_output_tokens).toBe(101);
    await consistent(db);
  }));

test("installing an unbound signed release invalidates conservatively without changing an existing candidate", () =>
  withFinancial(async (f, db) => {
    const { anthropicMessagesReleaseId } = await import("@gateway/plugin-sdk");
    const setup = await f.bootstrap(),
      first = await read(db, setup.model),
      before = await epoch(db);
    await f.catalog.application.installBundled(await f.catalog.actor(), {
      command_id: crypto.randomUUID(),
      release_id: anthropicMessagesReleaseId,
      reason: "Catalog cache publication coverage",
    });
    expect(await epoch(db)).toBeGreaterThan(before);
    expect(await read(db, setup.model)).toEqual(first);
    expect(
      (
        await db`SELECT count(*)::int n FROM core_catalog_cache.candidates WHERE catalog_revision<>(SELECT revision FROM core_catalog_cache.epoch)`
      )[0].n,
    ).toBe(0);
    await consistent(db);
  }));

test("a full bounded cache still returns a legal uncached candidate without growing its row or byte accounting", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap();
    const prefix = `cache-boundary-${crypto.randomUUID()}-`;
    await db`INSERT INTO models SELECT (jsonb_populate_record(NULL::models,to_jsonb(m)||jsonb_build_object('name',${prefix}||n))).* FROM models m CROSS JOIN generate_series(1,4096) n WHERE m.name=${setup.model}`;
    await db`INSERT INTO billing_tariffs(id,model,operation,provider_id,currency,pricing_id,enabled) SELECT gen_random_uuid(),${prefix}||n,'chat',NULL,'USD',t.pricing_id,true FROM billing_tariffs t CROSS JOIN generate_series(1,4096) n WHERE t.model=${setup.model} AND t.operation='chat' AND t.currency='USD' LIMIT 4096`;
    await db.begin(async (tx) => {
      await tx`INSERT INTO core_catalog_cache.candidates(catalog_revision,scope_digest,model,operation,currency,candidates,payload_bytes) SELECT e.revision,sha256(convert_to(jsonb_build_array(t.model,t.operation,t.currency)::text,'UTF8')),t.model,t.operation,t.currency,'[]'::jsonb,pg_column_size('[]'::jsonb) FROM billing_tariffs t CROSS JOIN core_catalog_cache.epoch e WHERE t.model LIKE ${prefix + "%"} AND t.operation='chat' AND t.currency='USD'`;
      await tx`UPDATE core_catalog_cache.usage SET byte_count=(SELECT sum(payload_bytes) FROM core_catalog_cache.candidates),entry_count=(SELECT count(*) FROM core_catalog_cache.candidates)`;
    });
    const result = await read(db, setup.model);
    expect(result).toHaveLength(1);
    expect((result[0]!.ref as Record<string, unknown>).channel_id).toBe(setup.provider.id);
    expect((await db`SELECT count(*)::int n FROM core_catalog_cache.candidates`)[0].n).toBe(4096);
    expect(
      (
        await db`SELECT count(*)::int n FROM core_catalog_cache.candidates WHERE model=${setup.model}`
      )[0].n,
    ).toBe(0);
    await consistent(db);
  }));

test("the 64 MiB cache byte budget returns oversized remaining scopes without retaining their candidates", () =>
  withFinancial(async (f, db) => {
    const setup = await f.bootstrap();
    const prefix = `cache-bytes-${crypto.randomUUID()}-`;
    const names = Array.from({ length: 10 }, (_, n) => `${prefix}${n}`);
    const padding = Object.fromEntries(
      Array.from({ length: 280 }, (_, n) => [`padding-${n}`, "x".repeat(200)]),
    );
    await f.catalog.application.bind(await f.catalog.actor(), setup.provider.id, {
      ...f.catalog.binding(setup.publication.version_id),
      expected_revision: "1",
      account_id: setup.provider.account_id,
      brand_id: setup.provider.brand_id,
      secret_reference: `channel:${setup.provider.id}`,
      config: {
        model_map: { ...padding, ...Object.fromEntries(names.map((name) => [name, "synthetic"])) },
        max_output_tokens: 100,
        max_input_tokens: 1000,
        auth: "bearer",
      },
    });
    await db`INSERT INTO models SELECT (jsonb_populate_record(NULL::models,to_jsonb(m)||jsonb_build_object('name',scope.name))).* FROM models m CROSS JOIN unnest(${names}::text[]) scope(name) WHERE m.name=${setup.model}`;
    await db`INSERT INTO billing_tariffs SELECT (jsonb_populate_record(NULL::billing_tariffs,to_jsonb(t)||jsonb_build_object('id',gen_random_uuid(),'model',scope.name))).* FROM billing_tariffs t CROSS JOIN unnest(${names}::text[]) scope(name) WHERE t.model=${setup.model}`;
    await db`INSERT INTO routes SELECT (jsonb_populate_record(NULL::routes,to_jsonb(r)||jsonb_build_object('id',gen_random_uuid(),'model_name',scope.name,'upstream_model','cache-bytes-'||n,'priority',n))).* FROM routes r CROSS JOIN unnest(${names}::text[]) scope(name) CROSS JOIN generate_series(1,128) n WHERE r.model_name=${setup.model}`;
    for (const name of names) {
      expect(
        (await db`SELECT jsonb_array_length(core_candidates_v1(${name},'chat','USD')) n`)[0].n,
      ).toBe(128);
    }
    const before = (await db`SELECT byte_count,entry_count FROM core_catalog_cache.usage`)[0];
    expect(before.byte_count).toBeLessThanOrEqual(64 * 1024 * 1024);
    expect(before.entry_count).toBeGreaterThan(0);
    expect(before.entry_count).toBeLessThan(10);
    const missing = (
      await db`SELECT name FROM unnest(${names}::text[]) name WHERE NOT EXISTS(SELECT 1 FROM core_catalog_cache.candidates c WHERE c.model=name) LIMIT 1`
    )[0].name;
    const result = (
      await db`SELECT jsonb_array_length(value) n,pg_column_size(value) bytes FROM (SELECT core_candidates_v1(${missing},'chat','USD') value) candidate`
    )[0];
    expect(result.n).toBe(128);
    expect(result.bytes).toBeGreaterThan(64 * 1024 * 1024 - before.byte_count);
    expect((await db`SELECT byte_count,entry_count FROM core_catalog_cache.usage`)[0]).toEqual(
      before,
    );
    await consistent(db);
  }));
