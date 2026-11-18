import { expect, test } from "bun:test";
import type { SQL } from "bun";
import { canonical } from "@gateway/kernel";
import { sha256, verifier } from "@gateway/runtime/crypto";
import {
  createQuotaLeases,
  type QuotaDescriptor,
  type QuotaScope,
} from "@gateway/runtime/quota-leases";
import { migrate, recordRoute } from "@gateway/storage";
import { createInternalIdentityStore } from "@gateway/storage/internal-identity";
import { createSettlementStore } from "@gateway/storage/settlement";
import type { CoreVerifiedCallerProjection, ExecutionKind } from "@gateway/contracts";
import manifest from "../../packages/storage/src/migrations/manifest.json";
import { withFinancial } from "../support/financial-execution.ts";
import { event, supplier } from "../support/core-settlement.ts";
import { syntheticPepper } from "../support/catalog.ts";
import { seedHistoricalRecharge } from "../support/historical-funding.ts";

async function installPrevious(db: SQL) {
  const stop = manifest.migrations.findIndex((entry) => entry.version === 2026111704);
  if (stop < 0) throw new Error("FROZEN_SCHEMA_MISSING");
  for (const migration of manifest.migrations.slice(0, stop + 1)) {
    for (const entry of migration.files) {
      const source = await Bun.file(
        new URL(`../../packages/storage/src/${entry.path}`, import.meta.url),
      ).text();
      if (sha256(source) !== entry.checksum) throw new Error("FROZEN_SCHEMA_CHECKSUM");
      await db.unsafe(source);
    }
    await db`INSERT INTO schema_migrations(version,checksum,manifest) VALUES(${migration.version},${sha256(JSON.stringify(migration))},${migration}::jsonb)`;
  }
}

test(
  "actual V1 reserved and granted permits survive the appended migration without new sends or fabricated identities",
  () =>
    withFinancial(
      async (f, db) => {
        expect(
          (await db`SELECT to_regprocedure('public.core_catalog_write_v1()') function`)[0].function,
        ).toBeNull();
        const setup = await f.bootstrap();
        const client = f.client();
        const owner = await client.startup();
        const identities = createInternalIdentityStore(db);
        const peer = (await identities.readPeerDirectory(owner.certificate_sha256))!;
        const session = (await identities.readStartupSession(owner.session_id))!;
        const deadline = new Date(Date.now() + 25000).toISOString();
        function command(kind: ExecutionKind, path: string, data: unknown) {
          const projection: CoreVerifiedCallerProjection = {
            service: "gateway",
            instance_id: session.instance_id,
            certificate_sha256: session.certificate_sha256,
            instance_revision: peer.instance_revision,
            certificate_revision: peer.certificate_revision,
            raw_body_sha256: sha256("historical-core-fixture"),
            session_id: session.session_id,
            incarnation_id: session.incarnation_id,
            key_sha256: session.public_key_sha256,
            session_revision: session.session_revision,
            command_type: kind,
            command_id: crypto.randomUUID(),
            method: "POST",
            normalized_path: `/_internal/core/v1/${path}`,
            deadline_at: deadline,
            trace_id: "historical-v1",
          };
          const envelope = {
            command_id: projection.command_id,
            caller_incarnation: owner.incarnation_id,
            deadline_at: deadline,
            trace_id: projection.trace_id,
            contract_version: 1,
            body_digest: sha256(
              canonical({
                kind,
                contract_version: 1,
                owner: {
                  service: "gateway",
                  instance_id: owner.instance_id,
                  session_id: owner.session_id,
                  incarnation_id: owner.incarnation_id,
                },
                deadline_at: deadline,
                data,
              }),
            ),
          };
          return { projection, envelope, data };
        }
        const quota = createQuotaLeases({
          urls: [process.env.REDIS_URL!],
          namespace: f.namespace,
          generation: "v1",
        });
        const historical = [];
        try {
          for (let index = 0; index < 3; index++) {
            const contextCaller = command("execution_context_v1", "context", {}).projection;
            const input = {
              verifier: verifier(setup.key.key, syntheticPepper),
              ip: "127.0.0.1",
              operation: "chat",
              model: setup.model,
              fingerprint: sha256(`historical_${index}`),
              max_attempts: 3,
            };
            const context = (
              await db`SELECT core_context_v1(${contextCaller}::jsonb,${crypto.randomUUID()}::uuid,${input}::jsonb) value`
            )[0].value;
            expect(context.execution_runtimes).toBeUndefined();
            const candidate = context.candidates[0];
            const billable = {
              version: 1,
              operation: "chat",
              model: setup.model,
              upstream_model: "synthetic",
              stream: false,
              input_bytes: 100,
              quantities: {},
              resource_ids: [],
              ceilings: { input_tokens: "60", output_tokens: "30" },
            };
            const dispatch = {
              version: 1,
              proposed_request_id: crypto.randomUUID(),
              proposed_attempt_id: crypto.randomUUID(),
              origin: candidate.origin,
              method: "POST",
              operation: "chat",
              request_target_sha256: sha256("/v1/chat/completions"),
              final_headers_digest: sha256("headers"),
              final_body_sha256: sha256("body"),
              candidate_ref: candidate.ref,
              auth: { type: "bearer" },
              credential_ciphertext_sha256: candidate.credential_ciphertext_sha256,
              billable_manifest_sha256: sha256(
                canonical(["stg.internal.billable-manifest", "1", billable]),
              ),
              deadline_at: deadline,
            };
            const prepId = crypto.randomUUID();
            const binding = {
              session: {
                id: prepId,
                tenant: setup.project.id,
                attempt: dispatch.proposed_attempt_id,
                incarnation: owner.incarnation_id,
                plugin: candidate.ref.package_digest,
                config: sha256(canonical(candidate.config)),
              },
              owner: {
                service: "gateway",
                instance_id: owner.instance_id,
                session_id: owner.session_id,
              },
              request_id: dispatch.proposed_request_id,
              operation: "chat",
              binding_id: candidate.ref.binding_id,
              config_revision: candidate.ref.config_revision,
              config_digest: candidate.ref.config_digest,
            };
            const data = {
              context_id: context.context_id,
              dispatch_projection: dispatch,
              dispatched_digest: sha256(
                canonical(["stg.internal.dispatch-projection", "1", dispatch]),
              ),
              manifest: billable,
              prep_session_id: prepId,
              prep_binding_digest: sha256(canonical(["stg.internal.prep-binding", "1", binding])),
              project_id: setup.project.id,
              key_id: setup.key.id,
              intent_digest: null,
              fingerprint: input.fingerprint,
              customer_currency: "USD",
            };
            const item = command("execution_admit_v1", "admissions", data);
            const scopes: QuotaScope[] = [
              ...context.scopes,
              {
                kind: "channel",
                id: candidate.ref.channel_id,
                rpm: 100,
                tpm: Number.MAX_SAFE_INTEGER,
                concurrency: 10,
              },
            ];
            const leases: QuotaDescriptor[] = [];
            for (const scope of scopes)
              leases.push(
                (
                  await quota.acquire({
                    scope,
                    caller: `gateway:${owner.instance_id}`,
                    kind: "execution_admit_v1",
                    command_id: item.projection.command_id,
                    body_digest: item.envelope.body_digest,
                    owner_session_id: owner.session_id,
                    deadline_ms: Date.parse(deadline),
                    tokens: scope.kind === "channel" ? 90 : 270,
                  })
                ).descriptor,
              );
            const computed = {
              quote: {
                currency: "USD",
                amount_atoms: "100",
                input_characters: "0",
                prices: { [candidate.ref.channel_id]: candidate.ref.customer_price_id },
              },
              estimated_quantities: { input_tokens: "60", output_tokens: "30" },
              supplier_atoms: "30",
              token_count: "90",
              leases,
            };
            const admitted = (
              await db`SELECT core_execution_batch_v1(${[{ ...item, computed }]}::jsonb) value`
            )[0].value[0];
            expect(admitted.receipt).toMatchObject({
              command_state: "COMMITTED",
              state: "RESERVED",
              customer_atoms: "100",
              supplier_atoms: "30",
              authorization_plan: { version: 1 },
            });
            const begin = {
              attempt_id: dispatch.proposed_attempt_id,
              permit_epoch: "1",
              authorization_plan_digest: admitted.receipt.authorization_plan_digest,
              dispatched_digest: data.dispatched_digest,
              prep_session_id: prepId,
              prep_binding_digest: data.prep_binding_digest,
            };
            if (index > 0) {
              const beginItem = command("execution_begin_v1", "begin", begin);
              const adopted = await Promise.all(
                leases.map((lease) =>
                  quota.adopt(
                    lease,
                    dispatch.proposed_attempt_id,
                    "1",
                    sha256(JSON.stringify([beginItem.projection.command_id, lease.lease_id])),
                  ),
                ),
              );
              const descriptors = adopted.map((result) => result.descriptor);
              await db`SELECT core_quota_v1(${beginItem.projection}::jsonb,${dispatch.proposed_attempt_id}::uuid,'ACTIVE',${descriptors}::jsonb)`;
              const started = (
                await db`SELECT core_execution_batch_v1(${[{ ...beginItem, computed: { proofs: descriptors.map((lease) => ({ lease_id: lease.lease_id, revision: lease.revision })), proof_remaining_ms: Math.floor(Math.min(...adopted.map((result) => result.proofDeadline)) - performance.now()) } }]}::jsonb) value`
              )[0].value[0];
              expect(started).toMatchObject({
                fresh_transition: true,
                receipt: { state: "SEND_GRANTED" },
              });
            }
            historical.push({
              item,
              admitted,
              begin,
              attempt: dispatch.proposed_attempt_id,
              request: dispatch.proposed_request_id,
            });
          }
          const signer = { client, permit_epoch: "1" };
          const [unknown] = await createSettlementStore(db).persist([
            {
              events: [
                event(signer, historical[2]!.attempt, {
                  source_revision: "1",
                  finality: "UNKNOWN",
                  attempt_state: "UNKNOWN",
                  request_state: "UNKNOWN",
                }),
              ],
            },
          ]);
          expect(unknown!.status).toBe("COMMITTED");
          await db`UPDATE execution_permits SET state='UNKNOWN' WHERE attempt_id=${historical[2]!.attempt} AND state='SEND_GRANTED'`;
          await db`UPDATE upstream_holds SET state='UNKNOWN' WHERE attempt_id=${historical[2]!.attempt} AND state='HELD'`;
          const before =
            await db`SELECT attempt_id,snapshot,authorization_plan_digest,state FROM execution_permits ORDER BY created_at`;
          expect(before.map((row: { state: string }) => row.state)).toEqual([
            "RESERVED",
            "SEND_GRANTED",
            "UNKNOWN",
          ]);
          await migrate(db);
          expect(
            (
              await f.catalog
                .appDb`SELECT has_function_privilege(current_user,'core_catalog_write_v1()','EXECUTE') allowed`
            )[0].allowed,
          ).toBe(true);
          expect(
            await db`SELECT attempt_id,snapshot,authorization_plan_digest,state FROM execution_permits ORDER BY created_at`,
          ).toEqual(before);
          for (const previous of historical) {
            const query = await client.execution("execution_query_v1", {
              kind: "execution_admit_v1",
              command_id: previous.item.projection.command_id,
            });
            expect(query).toMatchObject({
              receipt: {
                authorization_plan: previous.admitted.receipt.authorization_plan,
                authorization_plan_digest: previous.admitted.receipt.authorization_plan_digest,
              },
            });
            expect(query).not.toHaveProperty("transient_send");
          }
          const reserved = historical[0]!;
          const rejectedItem = command("execution_begin_v1", "begin", reserved.begin);
          const rejected = (
            await db`SELECT core_execution_batch_v1(${[{ ...rejectedItem, computed: {} }]}::jsonb) value`
          )[0].value[0];
          expect(rejected).toMatchObject({
            fresh_transition: false,
            receipt: { command_state: "REJECTED", code: "candidate_stale" },
          });
          expect(rejected).not.toHaveProperty("credential_ciphertext");
          const aborted = await client.execution("execution_abort_v1", {
            attempt_id: reserved.attempt,
            permit_epoch: "1",
            reason: "PREPARE_DISPOSED",
            close_request: true,
          });
          expect(aborted).toMatchObject({ receipt: { state: "ABORTED" } });
          for (const [index, previous] of historical.entries())
            if (index > 0) {
              const [settled] = await createSettlementStore(db).persist([
                {
                  events: [
                    event(signer, previous.attempt, {
                      source_revision: index === 2 ? "2" : "1",
                      finality: "FINAL",
                      usage: { input_tokens: "31", output_tokens: "2" },
                      final_dimensions: ["input_tokens", "output_tokens"],
                      attempt_state: "SUCCEEDED",
                      request_state: "SUCCEEDED",
                    }),
                  ],
                },
              ]);
              expect(settled!.status).toBe("COMMITTED");
            }
          expect(
            (await db`SELECT sum(amount_atoms)::text charged FROM billing_charges`)[0].charged,
          ).toBe("200");
          expect(await supplier(db, setup.provider.account_id)).toMatchObject({
            reserved: "0",
            spent: "22",
          });
        } finally {
          quota.close();
        }
      },
      {
        initializeDatabase: installPrevious,
        managementRoleFile: "tests/fixtures/core-management-2026111704.sql",
        executionRoleFile: "tests/fixtures/core-execution-2026111704.sql",
        seedRecharge: seedHistoricalRecharge,
        seedRoute: (db, model, input) => db.begin((tx) => recordRoute(tx, model, input)),
      },
    ),
  30000,
);
