import { expect, test } from "bun:test";
import type { CoreEvidenceGroupResult, CoreEvidenceGroupV1 } from "@gateway/contracts";
import { createSettlementProcessor } from "../settlement/processor.ts";

function group(): CoreEvidenceGroupV1 {
  return {
    events: [
      {
        event_id: crypto.randomUUID(),
        attempt_id: crypto.randomUUID(),
        origin_identity: "gateway:processor-test",
        origin_incarnation: crypto.randomUUID(),
        origin_session_id: crypto.randomUUID(),
        permit_epoch: "1",
        source: "execution",
        source_revision: "1",
        finality: "FINAL",
        usage: { input_tokens: "1" },
        final_dimensions: ["input_tokens"],
        signature: "a".repeat(86),
      },
    ],
  };
}

function committed(input: CoreEvidenceGroupV1, quarantined = false): CoreEvidenceGroupResult {
  return {
    status: "COMMITTED",
    billing: {},
    receipts: input.events.map((event) => ({
      event_id: event.event_id,
      receipt_id: crypto.randomUUID(),
      outcome: quarantined ? "QUARANTINED" : "ACCEPTED",
      duplicate: false,
    })),
  };
}

test("one job's lock retry does not delay other committed or rejected job acknowledgements", async () => {
  const inputs = [group(), group(), group()];
  const retry = Promise.withResolvers<CoreEvidenceGroupV1[]>();
  const release = Promise.withResolvers<CoreEvidenceGroupResult[]>();
  const alerts = { rejected: [] as number[], quarantined: [] as number[] };
  const accepted = committed(inputs[1]!, true);
  let calls = 0;
  const processor = createSettlementProcessor({
    persist: async (groups) => {
      if (++calls === 1)
        return [
          { status: "REJECTED", error: "evidence_signature_invalid" },
          accepted,
          { status: "ERROR", error: "55P03" },
        ];
      retry.resolve(groups);
      return release.promise;
    },
    onRejected: (count) => alerts.rejected.push(count),
    onQuarantined: (count) => alerts.quarantined.push(count),
  });
  let retriedSettled = false;
  const first = processor.process([inputs[0]!]);
  const second = processor.process([inputs[1]!]);
  const third = processor.process([inputs[2]!]).then((result) => {
    retriedSettled = true;
    return result;
  });
  const resolved = committed(inputs[2]!);
  try {
    expect(await retry.promise).toEqual([inputs[2]!]);
    expect(await first).toEqual([{ status: "REJECTED", error: "evidence_signature_invalid" }]);
    expect(await second).toEqual([accepted]);
    expect(retriedSettled).toBe(false);
    expect(alerts).toEqual({ rejected: [1], quarantined: [1] });
  } finally {
    release.resolve([resolved]);
    await processor.close();
  }
  expect(await third).toEqual([resolved]);
  expect(processor.stats()).toEqual({
    calls: 2,
    jobs: 4,
    groups: 4,
    events: 4,
    max_jobs: 3,
    max_groups: 3,
    lock_conflicts: 1,
    errors: 0,
    active_jobs: 0,
  });
});

test("unknown shared commit fails every participating job and shutdown waits for its outcome", async () => {
  const submitted = Promise.withResolvers<CoreEvidenceGroupV1[]>();
  const release = Promise.withResolvers<CoreEvidenceGroupResult[]>();
  let calls = 0;
  const processor = createSettlementProcessor({
    persist: (groups) => {
      calls++;
      submitted.resolve(groups);
      return release.promise;
    },
  });
  const firstGroup = group(),
    secondGroup = group();
  const jobs = Promise.allSettled([
    processor.process([firstGroup]),
    processor.process([secondGroup]),
  ]);
  expect(await submitted.promise).toEqual([firstGroup, secondGroup]);
  let closed = false;
  const closing = processor.close().then(() => {
    closed = true;
  });
  const unknown = new Error("DATABASE_COMMIT_OUTCOME_UNKNOWN");
  try {
    await expect(processor.process([group()])).rejects.toThrow("evidence_processor_stopping");
    expect(closed).toBe(false);
  } finally {
    release.reject(unknown);
    await closing;
  }
  expect(await jobs).toEqual([
    { status: "rejected", reason: unknown },
    { status: "rejected", reason: unknown },
  ]);
  expect(calls).toBe(1);
  expect(closed).toBe(true);
  expect(processor.stats().errors).toBe(1);
});

test("a short persistence result cannot acknowledge another job's evidence", async () => {
  const first = group(),
    second = group();
  const processor = createSettlementProcessor({
    persist: async () => [committed(first)],
  });
  try {
    const results = await Promise.allSettled([
      processor.process([first]),
      processor.process([second]),
    ]);
    expect(results.map((result) => result.status)).toEqual(["rejected", "rejected"]);
    for (const result of results)
      if (result.status === "rejected")
        expect(result.reason.message).toBe("evidence_batch_result_mismatch");
  } finally {
    await processor.close();
  }
});

test("four busy writers can drain queued jobs without one call taking every waiting job", async () => {
  const releases = Array.from({ length: 6 }, () => Promise.withResolvers<void>());
  const submissions = Array.from({ length: 6 }, () =>
    Promise.withResolvers<CoreEvidenceGroupV1[]>(),
  );
  const full = Array.from({ length: 4 }, () => Array.from({ length: 128 }, () => group()));
  const waiting = Array.from({ length: 5 }, () => [group()]);
  let calls = 0,
    active = 0,
    peak = 0;
  const processor = createSettlementProcessor({
    persist: async (groups) => {
      const index = calls++;
      active++;
      peak = Math.max(peak, active);
      submissions[index]!.resolve(groups);
      await releases[index]!.promise;
      active--;
      return groups.map((input) => committed(input));
    },
  });
  const inputs = [...full, ...waiting];
  const jobs = inputs.map((input) => processor.process(input));
  let closed = false;
  const closing = processor.close().then(() => {
    closed = true;
  });
  try {
    expect(calls).toBe(4);
    for (let index = 0; index < full.length; index++)
      expect(await submissions[index]!.promise).toEqual(full[index]!);
    expect(closed).toBe(false);
    releases[0]!.resolve();
    expect(await submissions[4]!.promise).toEqual(waiting.slice(0, 4).flat());
    releases[1]!.resolve();
    expect(await submissions[5]!.promise).toEqual(waiting[4]!);
    expect(closed).toBe(false);
  } finally {
    releases.forEach((release) => release.resolve());
    await closing;
  }
  const results = await Promise.all(jobs);
  expect(peak).toBe(4);
  expect(calls).toBe(6);
  for (let index = 0; index < results.length; index++)
    expect(
      results[index]!.flatMap((result) =>
        result.status === "COMMITTED" ? result.receipts.map((receipt) => receipt.event_id) : [],
      ),
    ).toEqual(inputs[index]!.flatMap((input) => input.events.map((event) => event.event_id)));
});
