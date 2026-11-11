import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, rm, statfs } from "node:fs/promises";
import { dirname } from "node:path";
import { once } from "node:events";
import { finished } from "node:stream/promises";
import { createInterface } from "node:readline";

export async function createIdentityWriter(path: string) {
  await mkdir(dirname(path), { recursive: true });
  const stream = createWriteStream(path, { flags: "wx", highWaterMark: 65536 });
  let failure: Error | undefined;
  let draining: Promise<unknown> | undefined;
  let closed = false;
  stream.on("error", (error) => {
    failure = error;
  });
  return {
    async write(ids: string[], at: number): Promise<void> {
      if (failure) throw failure;
      if (closed || !Number.isSafeInteger(at) || at < 0 || ids.length > 8192)
        throw new Error("Invalid identity evidence write");
      for (const id of ids) {
        if (!/^[A-Za-z0-9_-]{1,100}$/.test(id)) throw new Error("Invalid request identity");
        stream.write(`${id}\t${at}\n`);
      }
      if (stream.writableNeedDrain) {
        draining ??= once(stream, "drain").finally(() => {
          draining = undefined;
        });
        await draining;
      }
      if (failure) throw failure;
    },
    async close(): Promise<void> {
      if (closed) return;
      closed = true;
      stream.end();
      await finished(stream);
    },
  };
}

async function* identities(path: string) {
  const input = createReadStream(path, { encoding: "utf8", highWaterMark: 65536 });
  const lines = createInterface({ input, crlfDelay: Infinity });
  let previous: string | undefined;
  try {
    for await (const line of lines) {
      const match = /^([A-Za-z0-9_-]{1,100})\t([0-9]{1,16})$/.exec(line);
      if (!match || !Number.isSafeInteger(Number(match[2])))
        throw new Error("Malformed identity evidence");
      const id = match[1]!;
      if (previous !== undefined && id <= previous)
        throw new Error("Duplicate or unordered request identity");
      previous = id;
      yield { id, at: Number(match[2]) };
    }
  } finally {
    lines.close();
    input.destroy();
  }
}

export async function auditCapacityIdentities(
  httpPath: string,
  ackPath: string,
  startedAt: number,
  seconds: number,
): Promise<{
  fullyCompletedByWindow: number;
  minuteCompletions: number[];
  matchedRequests: number;
  httpOnly: number;
  ackOnly: number;
}> {
  if (
    !Number.isSafeInteger(startedAt) ||
    !Number.isSafeInteger(seconds) ||
    seconds < 1 ||
    seconds > 3600
  )
    throw new Error("Invalid capacity audit window");
  const sorted = [httpPath, ackPath].map((path) => `${path}.${crypto.randomUUID()}.sorted`);
  try {
    for (const [index, path] of [httpPath, ackPath].entries()) {
      const process = Bun.spawn(
        ["sort", "-S", "64M", "-T", dirname(path), "-o", sorted[index]!, path],
        {
          env: { ...Bun.env, LC_ALL: "C" },
          stdout: "ignore",
          stderr: "pipe",
        },
      );
      const error = await new Response(process.stderr).text();
      if ((await process.exited) !== 0)
        throw new Error(`Identity sort failed: ${error.slice(0, 300)}`);
    }
    const http = identities(sorted[0]!);
    const ack = identities(sorted[1]!);
    const minuteCompletions = Array.from({ length: Math.ceil(seconds / 60) }, () => 0);
    let matchedRequests = 0,
      httpOnly = 0,
      ackOnly = 0;
    try {
      let left = await http.next(),
        right = await ack.next();
      while (!left.done || !right.done) {
        if (right.done || (!left.done && left.value.id < right.value.id)) {
          httpOnly++;
          left = await http.next();
        } else if (left.done || right.value.id < left.value.id) {
          ackOnly++;
          right = await ack.next();
        } else {
          matchedRequests++;
          const at = Math.max(left.value.at, right.value.at) - startedAt;
          if (at >= 0 && at < seconds * 1000) {
            const index = Math.floor(at / 60000);
            minuteCompletions[index] = (minuteCompletions[index] ?? 0) + 1;
          }
          left = await http.next();
          right = await ack.next();
        }
      }
    } finally {
      await http.return();
      await ack.return();
    }
    return {
      fullyCompletedByWindow: minuteCompletions.reduce((sum, n) => sum + n, 0),
      minuteCompletions,
      matchedRequests,
      httpOnly,
      ackOnly,
    };
  } finally {
    await Promise.all(sorted.map((path) => rm(path, { force: true })));
  }
}

export async function capacityStorageBudget(
  path: string,
  requests: number,
  bytesPerRequest = 5000,
) {
  if (
    !Number.isSafeInteger(requests) ||
    requests < 0 ||
    !Number.isSafeInteger(bytesPerRequest) ||
    bytesPerRequest < 1
  )
    throw new Error("Invalid capacity storage estimate");
  const disk = await statfs(path);
  const available = Number(disk.bavail) * Number(disk.bsize);
  const reserve = 4 * 1024 ** 3;
  const estimated = requests * bytesPerRequest + 4 * 1024 ** 3;
  return {
    available,
    estimated,
    reserve,
    sufficient: available >= estimated + reserve,
    bytesPerRequest,
  };
}
