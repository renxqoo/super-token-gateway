import { expect, test } from "bun:test";
import { mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createCapacityAckIndex } from "../capacity-ack-index.ts";
import { auditCapacityIdentities, createIdentityWriter } from "../capacity-evidence.ts";

test("an ACK repeated across claim pages keeps its first external timestamp and counts once", async () => {
  const directory = await mkdtemp(join(tmpdir(), "capacity-ack-pages-"));
  const ackPath = join(directory, "ack.tsv");
  const httpPath = join(directory, "http.tsv");
  try {
    const index = await createCapacityAckIndex(directory);
    const ack = await createIdentityWriter(ackPath);
    try {
      expect(index.unseen(["alpha", "alpha", "beta"])).toEqual(["alpha", "beta"]);
      await ack.write(index.add(["alpha", "alpha", "beta"], 1800), 1800);
      const pages = 5;
      for (let i = 0; i < pages; i++) {
        const page = Array.from(
          { length: 48 },
          (_, j) => "request-" + (i * 48 + j).toString(16).padStart(28, "0"),
        );
        const at = 2000 + i;
        await ack.write(index.add(page, at), at);
      }
      expect(index.unseen(["alpha", "gamma", "beta", "gamma"])).toEqual(["gamma"]);
      await ack.write(index.add(["gamma", "alpha", "beta"], 99000), 99000);
      expect(index.size).toBe(3 + 48 * pages);
      expect(index.add(["alpha", "gamma"], 110000)).toEqual([]);
    } finally {
      await ack.close();
      await index.close();
    }
    const http = await createIdentityWriter(httpPath);
    await http.write(["alpha", "beta", "gamma"], 2000);
    await http.close();
    const audit = await auditCapacityIdentities(httpPath, ackPath, 1000, 60);
    expect(audit.matchedRequests).toBe(3);
    expect(audit.fullyCompletedByWindow).toBe(2);
    expect((await readdir(directory)).sort()).toEqual(["ack.tsv", "http.tsv"]);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("a broken ACK storage path fails closed and an invalid page cannot partially advance online counts", async () => {
  const directory = await mkdtemp(join(tmpdir(), "capacity-ack-failure-"));
  try {
    const blocked = join(directory, "occupied");
    await writeFile(blocked, "preserve");
    await expect(createCapacityAckIndex(blocked)).rejects.toThrow();
    const index = await createCapacityAckIndex(directory);
    try {
      expect(index.add(["first"], 9)).toEqual(["first"]);
      expect(() => index.add(["second", "bad id"], 10)).toThrow();
      expect(() => index.add(["second"], NaN)).toThrow();
      expect(index.unseen(["first", "second"])).toEqual(["second"]);
      expect(index.size).toBe(1);
    } finally {
      await index.close();
    }
    expect(() => index.add(["new"], 11)).toThrow();
    await index.close();
    expect(await readdir(directory)).toEqual(["occupied"]);
    expect(await Bun.file(blocked).text()).toBe("preserve");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
