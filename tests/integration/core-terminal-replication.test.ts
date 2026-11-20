import { SQL } from "bun";
import { expect, test } from "bun:test";
import { executionReplySchema, type CoreEvidenceGroupV1 } from "@gateway/contracts";
import { CoreRpcError } from "@gateway/core-client";
import {
  UsageProducer,
  streamTerminalRecoveryChannel,
  terminalRecoveryChannel,
} from "@gateway/runtime";
import { encrypt } from "@gateway/runtime/crypto";
import {
  createReplicatedTerminalSettlementStore,
  createSettlementStore,
} from "@gateway/storage/settlement";
import { createMockApp } from "../../apps/mock-providers/src/app.ts";
import { DatabaseCommitUnknown } from "../../packages/storage/src/transaction.ts";
import { event, granted, supplier } from "../support/core-settlement.ts";
import {
  financialKey,
  prepareFinancialAdmission,
  withFinancial,
} from "../support/financial-execution.ts";
import {
  boundedSupplier,
  chatFrames,
  chatReplies,
  eventually,
  slowStream,
  startCoreWorker,
  startGateway,
  startTlsUpstream,
  withSettlementRole,
} from "../support/gateway-process.ts";
import { createNativeFixture } from "../support/native-fixture.ts";

test("combined Chat JSON and SSE grants require physical replay, deduplicate sends and settle exact usage", async () => {
  const fixture = await createNativeFixture();
  const previousDatabase = process.env.DATABASE_URL;
  const previousRedis = process.env.REDIS_URL;
  try {
    const primary = await fixture.startPostgres(true);
    const replica = await fixture.startPostgresStandby(primary);
    const admission = await fixture.startRedis("terminal-admission");
    const recoveryRedis = await fixture.startRedis("terminal-recovery");
    const recoveryReplica = await fixture.startRedis("terminal-recovery-replica", recoveryRedis);
    await fixture.waitFor(async () =>
      (await recoveryReplica.command("INFO", "replication")).includes("master_link_status:up"),
    );
    const recoveryNamespace = "terminal-replica-test";
    process.env.DATABASE_URL = primary.url;
    process.env.REDIS_URL = admission.url;
    let terminalConfig: { database_url: string; standby_url: string };
    await withFinancial(
      async (f, db) => {
        const setup = await f.bootstrap();
        const client = f.client();
        await client.startup();
        const [{ name }] = await db`SELECT current_database() AS name`;
        const standbyAdminUrl = new URL(replica.url);
        standbyAdminUrl.pathname = `/${name}`;
        const writer = new SQL(terminalConfig.database_url, { max: 1 });
        const witness = new SQL(terminalConfig.standby_url, { max: 1 });
        const standby = new SQL(standbyAdminUrl.toString(), { max: 1 });
        const recoveryProbe = new UsageProducer({
          channel: terminalRecoveryChannel,
          url: recoveryRedis.url,
          namespace: recoveryNamespace,
          replicas: 1,
          timeoutMs: 1000,
          requireAllReplicas: true,
        });
        const deadline = () => ({ deadline: performance.now() + 8000 });
        const rpcDeadline = () => new Date(Date.now() + 8000).toISOString();

        async function withBlockedGrant<T>(
          attemptId: string | null,
          send: () => Promise<T>,
          observe: (
            backend: number,
            release: () => Promise<void>,
            pending: Promise<T>,
          ) => Promise<void>,
        ) {
          const blocker = await db.reserve();
          let held = false;
          let pending: Promise<T> | undefined;
          async function release() {
            if (held) {
              await blocker`SELECT pg_advisory_unlock(63719529::bigint)`;
              held = false;
            }
          }
          try {
            await blocker.unsafe(`
              CREATE FUNCTION block_test_send_grant() RETURNS trigger LANGUAGE plpgsql AS $$
              BEGIN PERFORM pg_advisory_xact_lock(63719529::bigint); RETURN NEW; END $$
            `);
            const [definition] = await blocker<{ ddl: string }[]>`SELECT format($ddl$
              CREATE TRIGGER block_test_send_grant BEFORE INSERT ON execution_permits
              FOR EACH ROW WHEN (NEW.state='SEND_GRANTED' AND
                (%1$L::uuid IS NULL OR NEW.attempt_id=%1$L::uuid))
              EXECUTE FUNCTION block_test_send_grant()
            $ddl$, ${attemptId}::uuid) AS ddl`;
            await blocker.unsafe(definition!.ddl);
            await blocker`SELECT pg_advisory_lock(63719529::bigint)`;
            held = true;
            pending = send();
            let backend: number | undefined;
            await fixture.waitFor(async () => {
              const [row] = await db<{ pid: number }[]>`
                SELECT a.pid FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid
                WHERE a.datname=current_database() AND a.application_name='core-internal-identity'
                  AND l.locktype='advisory' AND l.classid=0::oid AND l.objid=63719529::oid
                  AND l.objsubid=1 AND NOT l.granted
              `;
              backend = row?.pid;
              return backend !== undefined;
            }, 2500);
            await observe(backend!, release, pending);
          } finally {
            try {
              await release();
              try {
                await pending;
              } finally {
                await blocker.unsafe(`
                  DROP TRIGGER IF EXISTS block_test_send_grant ON execution_permits;
                  DROP FUNCTION IF EXISTS block_test_send_grant()
                `);
              }
            } finally {
              await blocker.release();
            }
          }
        }

        async function attempt(stream = false) {
          const { prepared, owner } = await granted(db, setup, client, { stream });
          const usage = { input_tokens: "31", output_tokens: "2" };
          const final = {
            usage,
            finality: "FINAL",
            final_dimensions: ["input_tokens", "output_tokens"],
          } as const;
          const group: CoreEvidenceGroupV1 = {
            events: [
              event(owner, prepared.attemptId, {
                ...final,
                final_dimensions: [...final.final_dimensions],
              }),
              event(owner, prepared.attemptId, {
                ...final,
                final_dimensions: [...final.final_dimensions],
                source_revision: "2",
                attempt_state: "SUCCEEDED",
                request_state: "SUCCEEDED",
              }),
            ],
          };
          return { id: prepared.requestId, group, attemptId: prepared.attemptId, owner };
        }

        try {
          const first = await attempt();
          expect(await client.settleTerminal(first.group, rpcDeadline())).toEqual({
            requestId: first.id,
          });
          const other = f.client(f.identity.a.port, "gatewayB");
          await other.startup();
          const denied = await other
            .settleTerminal(first.group, rpcDeadline())
            .catch((error: unknown) => error);
          expect(denied).toMatchObject({
            detail: { code: "internal_owner_mismatch" },
          });
          expect(
            await standby`SELECT amount_atoms::text FROM billing_charges WHERE request_id=${first.id}`,
          ).toEqual([{ amount_atoms: "100" }]);
          await expect(witness`SELECT reserved_atoms FROM wallets`).rejects.toMatchObject({
            errno: "42501",
          });
          await expect(
            createReplicatedTerminalSettlementStore(writer, db).settle(first.group, deadline()),
          ).rejects.toBeInstanceOf(DatabaseCommitUnknown);
          const late = event(first.owner, first.attemptId, {
            source_revision: "3",
            finality: "FINAL",
            usage: { input_tokens: "31", output_tokens: "2" },
            final_dimensions: ["input_tokens", "output_tokens"],
          });
          expect(
            (await createSettlementStore(writer).persist([{ events: [late] }]))[0],
          ).toMatchObject({
            status: "COMMITTED",
            receipts: [{ outcome: "ACCEPTED" }],
          });
          await fixture.waitFor(
            async () =>
              (await standby`SELECT 1 FROM core_evidence_receipts WHERE event_id=${late.event_id}`)
                .length === 1,
          );
          expect(await client.settleTerminal(first.group, rpcDeadline())).toEqual({
            requestId: first.id,
          });

          const second = await attempt();
          const replayConsumer = await f.request("/consumers", {
            name: "replication-fence",
            replay: true,
          });
          expect(replayConsumer.status).toBe(201);
          await standby`SELECT pg_wal_replay_pause()`;
          await fixture.waitFor(
            async () =>
              (await standby`SELECT pg_get_wal_replay_pause_state() AS state`)[0].state ===
              "paused",
          );
          const pending = client.settleTerminal(second.group, rpcDeadline()).then(
            (value) => ({ value }),
            (error: unknown) => ({ error }),
          );
          let pid: number | undefined;
          try {
            await fixture.waitFor(async () => {
              const [waiter] = await db<{ pid: number }[]>`
                SELECT pid FROM pg_stat_activity WHERE datname=current_database()
                  AND application_name='core-terminal-writer' AND wait_event='SyncRep'
              `;
              pid = waiter?.pid;
              return pid !== undefined;
            }, 4000);
            expect(
              (await db`SELECT pg_cancel_backend(${pid}::int) AS cancelled`)[0].cancelled,
            ).toBe(true);
            expect(await pending).toMatchObject({
              error: { status: 503, detail: { command_state: "UNKNOWN" } },
            });
            expect(
              await db`SELECT amount_atoms::text FROM billing_charges WHERE request_id=${second.id}`,
            ).toEqual([{ amount_atoms: "100" }]);
            expect(
              await standby`SELECT amount_atoms::text FROM billing_charges WHERE request_id=${second.id}`,
            ).toEqual([]);
            const earlyClaim = await f.request(`/consumers/${replayConsumer.value.id}/claim`, {
              limit: 100,
            });
            expect(earlyClaim).toMatchObject({ status: 200, value: { events: [] } });
            await fixture.waitFor(async () => (await recoveryProbe.stats()).delayed === 1);
            expect((await recoveryProbe.stats()).failed).toBe(0);
          } finally {
            await standby`SELECT pg_wal_replay_resume()`;
            await pending;
          }
          await fixture.waitFor(
            async () =>
              (await standby`SELECT 1 FROM billing_charges WHERE request_id=${second.id}`)
                .length === 1,
          );
          await fixture.waitFor(async () =>
            Object.values(await recoveryProbe.stats()).every((count) => count === 0),
          );
          expect(await client.settleTerminal(second.group, rpcDeadline())).toEqual({
            requestId: second.id,
          });
          expect(
            await standby`SELECT amount_atoms::text FROM billing_charges WHERE request_id=${second.id}`,
          ).toEqual([{ amount_atoms: "100" }]);
          expect(await supplier(db, setup.provider.account_id)).toEqual({
            balance: "978",
            reserved: "0",
            spent: "22",
            state: "ACTIVE",
          });
          const consumer = await f.request("/consumers", {
            name: "replicated-terminal-events",
            replay: true,
          });
          expect(consumer.status).toBe(201);
          const consumerPath = `/consumers/${consumer.value.id}`;
          const claim = await f.request(`${consumerPath}/claim`, { limit: 100 });
          expect(claim.status).toBe(200);
          const events = claim.value.events as { id: string; request_id: string }[];
          expect(new Set(events.map((item) => item.request_id))).toEqual(
            new Set([first.id, second.id]),
          );
          const ack = await f.request(`${consumerPath}/ack`, {
            lease_token: claim.value.lease_token,
            event_ids: events.map((item) => item.id),
          });
          expect(ack.value).toEqual({ acknowledged: events.length });
          const replayed = await f.request(`/consumers/${replayConsumer.value.id}/claim`, {
            limit: 100,
          });
          expect((replayed.value.events as unknown[]).length).toBe(events.length);
          const stream = await attempt(true);
          const unsupported = await client
            .settleTerminal(stream.group, rpcDeadline())
            .catch((error: unknown) => error);
          expect(unsupported).toMatchObject({
            status: 400,
            detail: { command_state: "NOT_ATTEMPTED" },
          });
          expect(
            await db`SELECT 1 FROM core_evidence_receipts WHERE attempt_id=${stream.attemptId}`,
          ).toEqual([]);
          expect((await createSettlementStore(writer).persist([stream.group]))[0]).toMatchObject({
            status: "COMMITTED",
          });

          async function prepareCombined(rpc = client, stream = false) {
            const proposal = await prepareFinancialAdmission(setup, rpc, {
              routeProjection: true,
              stream,
            });
            const send = () =>
              rpc.execution(
                "execution_admit_v1",
                { ...proposal.data, commit_send: true },
                {
                  commandId: proposal.command,
                  deadlineAt: proposal.deadline,
                  routeTicket: proposal.context.route_ticket!,
                },
              );
            return { ...proposal, send };
          }
          const combined = await prepareCombined();
          expect(await combined.send()).toMatchObject({
            receipt: { state: "SEND_GRANTED", request_id: combined.requestId },
            transient_send: { attempt_id: combined.attemptId, permit_epoch: "1" },
          });
          const shortGroup = Number.parseInt(setup.project.id.at(-1)!, 16);
          expect(
            await standby`
            SELECT p.state,h.funding_cell,b.amount_atoms::text,
              (SELECT bool_and(l.state='ACTIVE' AND l.revision=1) FROM execution_quota_leases l WHERE l.attempt_id=p.attempt_id) AS active
            FROM execution_permits p JOIN upstream_holds h ON h.attempt_id=p.attempt_id
              JOIN billing_holds b ON b.request_id=${combined.requestId}::uuid
            WHERE p.attempt_id=${combined.attemptId}::uuid
          `,
          ).toEqual([
            { state: "SEND_GRANTED", funding_cell: shortGroup, amount_atoms: "100", active: true },
          ]);
          expect(await combined.send()).not.toHaveProperty("transient_send");

          await db`UPDATE wallets SET manual_frozen=true WHERE project_id=${setup.project.id}::uuid`;
          for (let i = 0; i < 11; i++) {
            const rejected = await prepareCombined(other);
            const failure = await rejected.send().catch((error: unknown) => error);
            expect(failure).toBeInstanceOf(CoreRpcError);
            expect(failure).toMatchObject({
              status: 402,
              detail: {
                code: "insufficient_funds",
                command_state: "REJECTED",
                retryability: "NONE",
              },
            });
          }
          await db`UPDATE wallets SET manual_frozen=false WHERE project_id=${setup.project.id}::uuid`;

          const ambiguous = await prepareCombined();
          await withBlockedGrant(
            ambiguous.attemptId,
            () =>
              ambiguous.send().then(
                (value) => ({ value }),
                (error: unknown) => ({ error }),
              ),
            async (grantBackend, release, grantResult) => {
              try {
                await standby`SELECT pg_wal_replay_pause()`;
                await fixture.waitFor(
                  async () =>
                    (await standby`SELECT pg_get_wal_replay_pause_state() AS state`)[0].state ===
                    "paused",
                  2000,
                );
                await release();
                let cancelled = false;
                await fixture.waitFor(async () => {
                  const [row] = await db<{ cancelled: boolean }[]>`
                    SELECT pg_cancel_backend(a.pid) AS cancelled FROM pg_stat_activity a
                    WHERE a.pid=${grantBackend} AND a.datname=current_database()
                      AND a.query='COMMIT' AND a.wait_event='SyncRep'
                      AND EXISTS (SELECT FROM pg_locks l WHERE l.pid=a.pid AND l.locktype='advisory'
                        AND l.classid=0::oid AND l.objid=63719529::oid AND l.objsubid=1 AND l.granted)
                  `;
                  cancelled = row?.cancelled === true;
                  return cancelled;
                }, 2500);
                expect(cancelled).toBe(true);
                expect(await grantResult).toMatchObject({
                  error: { detail: { retryability: "STATUS_ONLY", command_state: "UNKNOWN" } },
                });
                expect(
                  await db`SELECT state FROM execution_permits WHERE attempt_id=${ambiguous.attemptId}::uuid`,
                ).toEqual([{ state: "SEND_GRANTED" }]);
                expect(
                  await standby`SELECT state FROM execution_permits WHERE attempt_id=${ambiguous.attemptId}::uuid`,
                ).toEqual([]);
              } finally {
                await standby`SELECT pg_wal_replay_resume()`;
              }
            },
          );
          expect(await ambiguous.send()).not.toHaveProperty("transient_send");

          const combinedStream = await prepareCombined(client, true);
          const freshStreamGrant = executionReplySchema.parse(await combinedStream.send());
          expect(freshStreamGrant).toMatchObject({
            receipt: { state: "SEND_GRANTED", request_id: combinedStream.requestId },
            transient_send: { attempt_id: combinedStream.attemptId, permit_epoch: "1" },
          });
          expect(await combinedStream.send()).toEqual({ receipt: freshStreamGrant.receipt });
          expect(
            await client.execution(
              "execution_query_v1",
              {
                kind: "execution_admit_v1",
                command_id: combinedStream.command,
              },
              { deadlineAt: rpcDeadline() },
            ),
          ).toEqual({ receipt: freshStreamGrant.receipt });
          expect(
            await standby`
              SELECT p.state,p.manifest->'stream' AS stream,h.funding_cell,b.amount_atoms::text,
                c.receipt?'authorization_plan' AS duplicated_plan
              FROM execution_permits p JOIN upstream_holds h ON h.attempt_id=p.attempt_id
              JOIN execution_commands c ON c.id=p.command_id
              JOIN billing_holds b ON b.request_id=${combinedStream.requestId}::uuid
              WHERE p.attempt_id=${combinedStream.attemptId}::uuid
            `,
          ).toEqual([
            {
              state: "SEND_GRANTED",
              stream: true,
              funding_cell: shortGroup,
              amount_atoms: "100",
              duplicated_plan: false,
            },
          ]);
          const combinedStreamGroup = {
            events: [
              event({ client, permit_epoch: "1" }, combinedStream.attemptId, {
                finality: "FINAL",
                usage: { input_tokens: "31", output_tokens: "2" },
                final_dimensions: ["input_tokens", "output_tokens"],
                attempt_state: "SUCCEEDED",
                request_state: "SUCCEEDED",
              }),
            ],
          };
          const streamingSettlement = createSettlementStore(writer);
          expect((await streamingSettlement.persist([combinedStreamGroup]))[0]).toMatchObject({
            status: "COMMITTED",
          });
          expect((await streamingSettlement.persist([combinedStreamGroup]))[0]).toMatchObject({
            status: "COMMITTED",
          });
          expect(
            await db`
              SELECT c.amount_atoms::text AS billed,h.final_cost_atoms::text AS cost,
                h.current_reserved_atoms::text AS reserved
              FROM billing_charges c JOIN upstream_holds h ON h.attempt_id=${combinedStream.attemptId}::uuid
              WHERE c.request_id=${combinedStream.requestId}::uuid
            `,
          ).toEqual([{ billed: "100", cost: "11", reserved: "0" }]);

          const tracked = await prepareCombined(client, true);
          await tracked.send();
          const trackedOwner = { client, permit_epoch: "1" };
          const metered = {
            finality: "FINAL" as const,
            usage: { input_tokens: "31", output_tokens: "2" },
            final_dimensions: ["input_tokens", "output_tokens"] as (
              "input_tokens" | "output_tokens"
            )[],
          };
          const trackedGroup = {
            events: [
              event(trackedOwner, tracked.attemptId, metered),
              event(trackedOwner, tracked.attemptId, {
                ...metered,
                source_revision: "2",
                attempt_state: "SUCCEEDED",
                request_state: "SUCCEEDED",
              }),
            ],
          };
          await standby`SELECT pg_wal_replay_pause()`;
          try {
            await fixture.waitFor(
              async () =>
                (await standby`SELECT pg_get_wal_replay_pause_state() AS state`)[0].state ===
                "paused",
            );
            const wait = client.settleTerminal(trackedGroup, rpcDeadline(), undefined, true).then(
              (value) => ({ value }),
              (error: unknown) => ({ error }),
            );
            let pid: number | undefined;
            await fixture.waitFor(async () => {
              const [row] = await db<
                { pid: number }[]
              >`SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND application_name='core-terminal-writer' AND wait_event='SyncRep'`;
              pid = row?.pid;
              return pid !== undefined;
            });
            expect(
              (await db`SELECT pg_cancel_backend(${pid!}::int) AS cancelled`)[0].cancelled,
            ).toBe(true);
            expect(await wait).toMatchObject({
              error: { status: 503, detail: { command_state: "UNKNOWN" } },
            });
            expect(
              await standby`SELECT 1 FROM billing_charges WHERE request_id=${tracked.requestId}::uuid`,
            ).toEqual([]);
          } finally {
            await standby`SELECT pg_wal_replay_resume()`;
          }
          await fixture.waitFor(
            async () =>
              (
                await standby`SELECT 1 FROM billing_charges WHERE request_id=${tracked.requestId}::uuid`
              ).length === 1,
          );
          expect(await client.settleTerminal(trackedGroup, rpcDeadline(), undefined, true)).toEqual(
            {
              requestId: tracked.requestId,
            },
          );
          expect(
            await standby`SELECT count(*)::int AS signed FROM core_evidence_receipts WHERE attempt_id=${tracked.attemptId}::uuid AND outcome='ACCEPTED' AND authentication_disposition='VERIFIED'`,
          ).toEqual([{ signed: 2 }]);
          expect(
            await standby`SELECT c.amount_atoms::text AS billed,h.final_cost_atoms::text AS cost,h.current_reserved_atoms::text AS held FROM billing_charges c JOIN upstream_holds h ON h.attempt_id=${tracked.attemptId}::uuid WHERE c.request_id=${tracked.requestId}::uuid`,
          ).toEqual([{ billed: "100", cost: "11", held: "0" }]);

          const readerOnly = f.client(f.identity.b.port, "gatewayB");
          await readerOnly.startup();
          const restored = await prepareCombined(readerOnly, true);
          await restored.send();
          const recoveredOwner = { client: readerOnly, permit_epoch: "1" };
          const signedPair = (
            qty: typeof metered.usage,
            dimensions: ("input_tokens" | "output_tokens")[],
            owner = recoveredOwner,
          ) => [
            event(owner, restored.attemptId, {
              finality: "FINAL",
              usage: qty,
              final_dimensions: dimensions,
            }),
            event(owner, restored.attemptId, {
              finality: "FINAL",
              usage: qty,
              final_dimensions: dimensions,
              source_revision: "2",
              attempt_state: "SUCCEEDED",
              request_state: "SUCCEEDED",
            }),
          ];
          const eligible = { events: signedPair(metered.usage, metered.final_dimensions) };
          const badSignature = {
            events: eligible.events.map((e) => ({ ...e, signature: "A".repeat(86) })),
          };
          const underpriced = { events: signedPair(metered.usage, ["input_tokens"]) };
          const rejected = async (g: typeof eligible) =>
            readerOnly
              .settleTerminal(g, rpcDeadline(), undefined, true)
              .catch((error: unknown) => error);
          const hugeEpoch = {
            events: signedPair(metered.usage, metered.final_dimensions, {
              client: readerOnly,
              permit_epoch: "9".repeat(38),
            }),
          };
          expect(await rejected(underpriced)).toMatchObject({
            status: 400,
            detail: { code: "internal_request_invalid", command_state: "NOT_ATTEMPTED" },
          });
          expect(await rejected(badSignature)).toMatchObject({
            status: 400,
            detail: { code: "internal_request_invalid", command_state: "NOT_ATTEMPTED" },
          });
          expect(await rejected(eligible)).toMatchObject({
            status: 409,
            detail: { code: "internal_stream_terminal_disabled", command_state: "NOT_ATTEMPTED" },
          });
          expect(await rejected(hugeEpoch)).toMatchObject({
            status: 400,
            detail: { code: "internal_request_invalid", command_state: "NOT_ATTEMPTED" },
          });
          expect(
            await db`SELECT 1 FROM core_evidence_receipts WHERE attempt_id=${restored.attemptId}::uuid`,
          ).toEqual([]);
          const restoreQueue = new UsageProducer({
            channel: streamTerminalRecoveryChannel,
            url: recoveryRedis.url,
            namespace: recoveryNamespace + "-recover-only",
            replicas: 1,
            timeoutMs: 1000,
            requireAllReplicas: true,
            batchSize: 1,
          });
          try {
            expect(Object.values(await restoreQueue.stats()).every((n) => n === 0)).toBe(true);
            await restoreQueue.publish(eligible);
            await fixture.waitFor(
              async () =>
                (
                  await standby`SELECT count(*)::int AS n FROM core_evidence_receipts WHERE attempt_id=${restored.attemptId}::uuid AND authentication_disposition='VERIFIED' AND outcome='ACCEPTED'`
                )[0].n === 2,
            );
            await fixture.waitFor(async () =>
              Object.values(await restoreQueue.stats()).every((n) => n === 0),
            );
          } finally {
            restoreQueue.close();
          }
          await withSettlementRole(db, async (databaseUrl) => {
            let sent = 0;
            let holdOpen = false;
            let upstreamCancelled = false;
            let stopProjectCore = false;
            let syncSent = 0;
            const standardMock = createMockApp(crypto.randomUUID());
            const upstream = await startTlsUpstream(async (request) => {
              const body = (await request.json()) as {
                model: string;
                stream?: boolean;
                messages?: { content?: string }[];
              };
              if (body.stream && body.messages?.[0]?.content === "standard fixed-length SSE") {
                standardMock.scenarios.set(body.model, { echoRequestChallenge: true });
                return standardMock.app.fetch(
                  new Request(request.url, {
                    method: request.method,
                    headers: request.headers,
                    body: JSON.stringify(body),
                    signal: request.signal,
                  }),
                );
              }
              if (body.stream && body.messages?.[0]?.content === "replicated SSE proof") {
                syncSent++;
                const bytes = new TextEncoder().encode(await chatReplies.sse().text());
                return new Response(
                  new ReadableStream<Uint8Array>({
                    start(s) {
                      s.enqueue(bytes);
                    },
                  }),
                  { headers: { "content-type": "text/event-stream" } },
                );
              }
              sent++;
              if (stopProjectCore) await f.identity.b.stop();
              if (body.stream && holdOpen)
                return slowStream(chatFrames().slice(0, 1), 50, () => {
                  upstreamCancelled = true;
                });
              return body.stream ? chatReplies.sse() : chatReplies.json();
            });
            const queue = {
              url: recoveryRedis.url,
              namespace: recoveryNamespace,
              required_replicas: 1,
            };
            let worker: Awaited<ReturnType<typeof startCoreWorker>> | undefined;
            let gateway: Awaited<ReturnType<typeof startGateway>> | undefined;
            try {
              const replicatedChat = await f.bootstrap(
                "USD",
                undefined,
                undefined,
                undefined,
                boundedSupplier,
                { baseUrl: upstream.baseUrl, maxInputTokens: 1000 },
              );
              gateway = await startGateway(f, {
                namespace: recoveryNamespace,
                upstreamCertificate: upstream.certificate,
                instance: "gatewayB",
                overrides: {
                  queue,
                  commit_short_chat: true,
                  commit_short_chat_sse: true,
                  terminal_recovery_queue: queue,
                  total_timeout_ms: 10000,
                },
              });
              const sendReplicatedStream = (origin: string) =>
                fetch(`${origin}/v1/chat/completions`, {
                  method: "POST",
                  headers: {
                    authorization: `Bearer ${replicatedChat.key.key}`,
                    "content-type": "application/json",
                  },
                  body: JSON.stringify({
                    model: replicatedChat.model,
                    messages: [{ role: "user", content: "replicated SSE proof" }],
                    max_completion_tokens: 30,
                    stream: true,
                  }),
                });
              const replicatedReply = await sendReplicatedStream(gateway.origin);
              expect(replicatedReply.status).toBe(200);
              expect((await replicatedReply.text()).trimEnd().endsWith("data: [DONE]")).toBe(true);
              const replicatedId = replicatedReply.headers.get("x-request-id")!;
              expect(syncSent).toBe(1);
              expect(
                await standby`SELECT q.state,c.amount_atoms::text AS billed,h.final_cost_atoms::text AS cost,
                 (SELECT count(*)::int FROM core_evidence_receipts r WHERE r.attempt_id=q.selected_attempt_id AND r.authentication_disposition='VERIFIED' AND r.outcome='ACCEPTED') AS accepted
                FROM gateway_requests q JOIN billing_charges c ON c.request_id=q.id
                JOIN upstream_holds h ON h.attempt_id=q.selected_attempt_id WHERE q.id=${replicatedId}::uuid`,
              ).toEqual([{ state: "SUCCEEDED", billed: "100", cost: "15", accepted: 2 }]);
              const fixedRate = await f.bootstrap(
                "USD",
                "1000000000000000000",
                undefined,
                "REQUEST_COMPLETION",
                {
                  rates: {
                    input_tokens: { numerator: "3", denominator: "1" },
                    cached_input_tokens: { numerator: "1", denominator: "1" },
                    output_tokens: { numerator: "7", denominator: "1" },
                  },
                  ceilings: {
                    input_tokens: "4096",
                    cached_input_tokens: "4096",
                    output_tokens: "4096",
                  },
                },
                { baseUrl: upstream.baseUrl, maxInputTokens: 4096 },
              );
              const challenge = crypto.randomUUID() + "00000001";
              const fixedReply = await fetch(gateway.origin + "/v1/chat/completions", {
                method: "POST",
                headers: {
                  authorization: "Bearer " + fixedRate.key.key,
                  "content-type": "application/json",
                },
                body: JSON.stringify({
                  model: fixedRate.model,
                  messages: [{ role: "user", content: "standard fixed-length SSE" }],
                  max_completion_tokens: 30,
                  stream: true,
                  stream_options: { include_usage: true },
                  user: challenge,
                }),
              });
              const fixedText = await fixedReply.text();
              expect({
                status: fixedReply.status,
                error: fixedReply.ok ? undefined : fixedText,
              }).toEqual({ status: 200, error: undefined });
              expect(fixedText).toContain(challenge);
              expect(fixedText.trimEnd().endsWith("data: [DONE]")).toBe(true);
              expect(standardMock.stats.requests).toBe(1);
              const fixedId = fixedReply.headers.get("x-request-id")!;
              expect(
                await standby`SELECT q.state,c.amount_atoms::text AS billed,h.final_cost_atoms::text AS cost,
                 (SELECT count(*)::int FROM core_evidence_receipts r WHERE r.attempt_id=q.selected_attempt_id AND r.authentication_disposition='VERIFIED' AND r.outcome='ACCEPTED') AS accepted
                FROM gateway_requests q JOIN billing_charges c ON c.request_id=q.id
                JOIN upstream_holds h ON h.attempt_id=q.selected_attempt_id WHERE q.id=${fixedId}::uuid`,
              ).toEqual([{ state: "SUCCEEDED", billed: "100", cost: "59", accepted: 2 }]);
              await gateway.stop();
              gateway = undefined;
              gateway = await startGateway(f, {
                namespace: recoveryNamespace,
                upstreamCertificate: upstream.certificate,
                instance: "gatewayB",
                core: "coreB",
                coreConnections: 4,
                shortChatPeerParity: /[13579bdf]$/i.test(replicatedChat.project.id) ? 1 : 0,
                overrides: {
                  queue,
                  commit_short_chat: true,
                  commit_short_chat_sse: true,
                  terminal_recovery_queue: queue,
                  total_timeout_ms: 10000,
                },
              });
              const pinnedStream = await sendReplicatedStream(gateway.origin);
              expect(pinnedStream.status).toBe(200);
              expect((await pinnedStream.text()).trimEnd().endsWith("data: [DONE]")).toBe(true);
              expect(syncSent).toBe(2);
              expect(
                await standby`SELECT c.amount_atoms::text AS billed,h.final_cost_atoms::text AS cost
                  FROM gateway_requests q JOIN billing_charges c ON c.request_id=q.id
                  JOIN upstream_holds h ON h.attempt_id=q.selected_attempt_id
                  WHERE q.id=${pinnedStream.headers.get("x-request-id")}::uuid`,
                "a project pinned from a Core without synchronous SSE must settle on its capable peer",
              ).toEqual([{ billed: "100", cost: "15" }]);
              await gateway.stop();
              gateway = undefined;
              const chat = await f.bootstrap(
                "USD",
                undefined,
                undefined,
                undefined,
                boundedSupplier,
                { baseUrl: upstream.baseUrl, maxInputTokens: 1000 },
              );
              const invalidCredential = encrypt("invalid bearer", financialKey, chat.provider.id);
              await db`UPDATE channels SET secret=${invalidCredential} WHERE id=${chat.provider.id}`;
              worker = await startCoreWorker(recoveryNamespace, {
                databaseUrl,
                identity: f.identity,
                env: { QUEUE_REDIS_URL: recoveryRedis.url },
              });
              gateway = await startGateway(f, {
                namespace: recoveryNamespace,
                upstreamCertificate: upstream.certificate,
                instance: "gatewayB",
                coreConnections: 4,
                shortChatPeerParity: /[13579bdf]$/i.test(chat.project.id) ? 1 : 0,
                overrides: {
                  queue,
                  commit_short_chat: true,
                  terminal_recovery_queue: queue,
                  total_timeout_ms: 10000,
                },
              });
              const chatUrl = `${gateway.origin}/v1/chat/completions`;
              const send = (signal?: AbortSignal, stream = false) =>
                fetch(chatUrl, {
                  method: "POST",
                  ...(signal ? { signal } : {}),
                  headers: {
                    authorization: `Bearer ${chat.key.key}`,
                    "content-type": "application/json",
                  },
                  body: JSON.stringify({
                    model: chat.model,
                    messages: [{ role: "user", content: "hello" }],
                    max_completion_tokens: 30,
                    stream,
                  }),
                });
              const response = await send();
              expect(await response.json()).toMatchObject({ error: { code: "core_unavailable" } });
              expect(response.status).toBe(503);
              expect(sent).toBe(0);
              const requestId = response.headers.get("x-request-id")!;
              async function released(id: string) {
                await eventually(
                  async () =>
                    (
                      await db`SELECT a.state='NOT_EXECUTED' AND r.execution_closed_at IS NOT NULL AS ready
                    FROM attempts a JOIN gateway_requests r ON r.id=a.request_id WHERE r.id=${id}::uuid`
                    )[0]?.ready === true,
                  "known unsent short Chat settlement and logical closure",
                  6000,
                );
                expect(
                  await db`
                  SELECT r.execution_closed_at IS NOT NULL AS closed,
                    bool_and(l.state IN ('RELEASE_PENDING','RELEASED')) AS release_committed,
                    (SELECT reserved_atoms::text FROM wallets WHERE project_id=${chat.project.id}::uuid) AS customer_reserved,
                    h.current_reserved_atoms::text AS supplier_reserved
                  FROM gateway_requests r JOIN execution_quota_leases l ON l.request_id=r.id
                  JOIN attempts a ON a.request_id=r.id JOIN upstream_holds h ON h.attempt_id=a.id
                  WHERE r.id=${id}::uuid GROUP BY r.id,h.attempt_id
                `,
                ).toEqual([
                  {
                    closed: true,
                    release_committed: true,
                    customer_reserved: "0",
                    supplier_reserved: "0",
                  },
                ]);
              }
              await released(requestId);

              const disconnected = new AbortController();
              await withBlockedGrant(
                null,
                () => send(disconnected.signal).catch((error: unknown) => error),
                async (_backend, _release, pending) => {
                  disconnected.abort();
                  expect(await pending).toBeInstanceOf(Error);
                },
              );
              let abortedId: string | undefined;
              await fixture.waitFor(async () => {
                const [row] = await db<{ id: string }[]>`SELECT id::text
                  FROM gateway_requests WHERE project_id=${chat.project.id}::uuid AND id<>${requestId}::uuid`;
                abortedId = row?.id;
                return abortedId !== undefined;
              });
              await released(abortedId!);
              expect(sent).toBe(0);

              const validCredential = encrypt(
                "synthetic-upstream-secret",
                financialKey,
                chat.provider.id,
              );
              await db`UPDATE channels SET secret=${validCredential} WHERE id=${chat.provider.id}`;
              const streamed = await send(undefined, true);
              expect(streamed.status).toBe(200);
              const streamedBody = await streamed.text();
              expect(streamedBody).toContain('"content":"o"');
              expect(streamedBody.trimEnd().endsWith("data: [DONE]")).toBe(true);
              expect(sent).toBe(1);
              const streamId = streamed.headers.get("x-request-id")!;
              await eventually(
                async () =>
                  (
                    await db`SELECT count(*)::int n FROM billing_charges WHERE request_id=${streamId}::uuid`
                  )[0]!.n === 1,
                "settlement for the replicated SSE grant",
              );
              expect(
                await standby`
                  SELECT c.kind,c.receipt->>'state' AS state,p.manifest->'stream' AS stream
                  FROM execution_commands c JOIN execution_permits p ON p.command_id=c.id
                  WHERE c.request_id=${streamId}::uuid
                `,
              ).toEqual([{ kind: "execution_admit_v1", state: "SEND_GRANTED", stream: true }]);
              expect(
                await db`
                  SELECT c.amount_atoms::text AS billed,h.final_cost_atoms::text AS cost
                  FROM billing_charges c JOIN gateway_requests r ON r.id=c.request_id
                  JOIN upstream_holds h ON h.attempt_id=r.selected_attempt_id
                  WHERE c.request_id=${streamId}::uuid
                `,
              ).toEqual([{ billed: "100", cost: "15" }]);

              const sseClaim = await f.request(`${consumerPath}/claim`, { limit: 100 });
              expect(sseClaim.status).toBe(200);
              const deliveries = sseClaim.value.events as { id: string; request_id: string }[];
              expect(deliveries.some((delivery) => delivery.request_id === streamId)).toBe(true);
              expect(
                (
                  await f.request(`${consumerPath}/ack`, {
                    lease_token: sseClaim.value.lease_token,
                    event_ids: deliveries.map((delivery) => delivery.id),
                  })
                ).value,
              ).toEqual({ acknowledged: deliveries.length });

              holdOpen = true;
              const clientCancellation = new AbortController();
              const interrupted = await send(clientCancellation.signal, true);
              const interruptedId = interrupted.headers.get("x-request-id")!;
              const reader = interrupted.body!.getReader();
              try {
                const frame = await reader.read();
                expect(new TextDecoder().decode(frame.value)).toContain('"content":"o"');
              } finally {
                clientCancellation.abort();
                reader.releaseLock();
              }
              await eventually(async () => upstreamCancelled, "combined SSE upstream cancellation");
              await eventually(
                async () =>
                  (
                    await db`
                  SELECT a.state='PARTIAL' AND a.error_code='client_cancelled' AND h.current_reserved_atoms>0 AS pending
                  FROM attempts a JOIN upstream_holds h ON h.attempt_id=a.id
                  WHERE a.request_id=${interruptedId}::uuid
                `
                  )[0]?.pending === true,
                "cancelled sent SSE keeps supplier exposure",
              );
              expect(
                await db`SELECT 1 FROM billing_charges WHERE request_id=${interruptedId}::uuid`,
              ).toEqual([]);
              expect(sent).toBe(2);

              holdOpen = false;
              const routed = await send();
              expect(routed.status).toBe(200);
              expect(await routed.json()).toMatchObject({ choices: expect.any(Array) });
              const routedId = routed.headers.get("x-request-id")!;
              expect(
                await standby`SELECT amount_atoms::text AS amount FROM billing_charges WHERE request_id=${routedId}::uuid`,
              ).toEqual([{ amount: "100" }]);
              const beforeStop = sent;
              stopProjectCore = true;
              const lostTerminal = await send();
              expect(lostTerminal.status).toBe(503);
              await lostTerminal.text();
              expect(sent).toBe(beforeStop + 1);
              const recoveryId = lostTerminal.headers.get("x-request-id")!;
              await eventually(
                async () =>
                  (
                    await standby`SELECT amount_atoms::text AS amount FROM billing_charges WHERE request_id=${recoveryId}::uuid`
                  )[0]?.amount === "100",
                "short Chat terminal recovery on the surviving Core",
              );
              expect(
                await standby`SELECT c.amount_atoms::text AS amount, h.current_reserved_atoms::text AS reserved, h.final_cost_atoms::text AS cost FROM billing_charges c JOIN attempts a ON a.request_id=c.request_id JOIN upstream_holds h ON h.attempt_id=a.id WHERE c.request_id=${recoveryId}::uuid`,
              ).toEqual([{ amount: "100", reserved: "0", cost: "15" }]);
              const lostGrant = await send();
              expect(lostGrant.status).toBe(503);
              await lostGrant.text();
              expect(sent).toBe(beforeStop + 1);
              const lostId = lostGrant.headers.get("x-request-id")!;
              expect(await db`SELECT 1 FROM gateway_requests WHERE id=${lostId}::uuid`).toEqual([]);
              stopProjectCore = false;
              await recoveryReplica.command("REPLICAOF", "NO", "ONE");
              await fixture.waitFor(async () =>
                (await recoveryRedis.command("INFO", "replication")).includes("connected_slaves:0"),
              );
              const unwitnessed = await send(undefined, true);
              expect(await unwitnessed.text()).not.toContain("data: [DONE]");
              expect(sent).toBe(beforeStop + 2);
            } finally {
              await Promise.all([gateway?.stop(), worker?.stop(), upstream.stop()]);
            }
          });

          await db`UPDATE core_funding.writer_groups SET epoch=2 WHERE cell=${shortGroup}`;
          const fenced = await prepareCombined();
          const fencedReply = await fenced.send().catch((error: unknown) => error);
          expect(fencedReply).toBeInstanceOf(CoreRpcError);
          expect(fencedReply).toMatchObject({ detail: { retryability: "STATUS_ONLY" } });
          expect(
            await db`SELECT id FROM gateway_requests WHERE id=${fenced.requestId}::uuid`,
          ).toEqual([]);
        } finally {
          recoveryProbe.close();
          await Promise.all([
            writer.close({ timeout: 1 }),
            witness.close({ timeout: 1 }),
            standby.close({ timeout: 1 }),
          ]);
        }
      },
      {
        redisUrl: admission.url,
        managementStandby: () => new SQL(terminalConfig.standby_url, { max: 1 }),
        async terminalSettlement(db) {
          const [{ name }] = await db`SELECT current_database() AS name`;
          await db.unsafe(await Bun.file("infra/core-terminal-settlement-roles.sql").text());
          const password = crypto.randomUUID().replaceAll("-", "");
          await db.unsafe(`CREATE ROLE terminal_writer LOGIN PASSWORD '${password}'`);
          await db.unsafe(`CREATE ROLE terminal_reader LOGIN PASSWORD '${password}'`);
          await db`GRANT core_terminal_writer TO terminal_writer`;
          await db`GRANT core_terminal_confirmation TO terminal_reader`;
          const writer = new URL(primary.url);
          writer.pathname = `/${name}`;
          writer.username = "terminal_writer";
          writer.password = password;
          const witness = new URL(replica.url);
          witness.pathname = writer.pathname;
          witness.username = "terminal_reader";
          witness.password = password;
          terminalConfig = { database_url: writer.toString(), standby_url: witness.toString() };
          const config = {
            ...terminalConfig,
            short_chat_funding_epoch: 1,
            recovery_queue: {
              url: recoveryRedis.url,
              namespace: recoveryNamespace,
              required_replicas: 1,
              ack_timeout_ms: 1000,
            },
          };
          return {
            coreA: { ...config, commit_short_chat_sse: true },
            coreB: {
              ...config,
              commit_short_chat_sse: false,
              recovery_queue: {
                ...config.recovery_queue,
                namespace: recoveryNamespace + "-recover-only",
              },
            },
          };
        },
      },
    );
  } finally {
    if (previousDatabase === undefined) delete process.env.DATABASE_URL;
    else process.env.DATABASE_URL = previousDatabase;
    if (previousRedis === undefined) delete process.env.REDIS_URL;
    else process.env.REDIS_URL = previousRedis;
    await fixture.close();
  }
}, 45000);
