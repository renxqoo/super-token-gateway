import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, rm, statfs } from "node:fs/promises";
import { dirname } from "node:path";
import { once } from "node:events";
import { finished } from "node:stream/promises";
import { createInterface } from "node:readline";
import { CONTRACT_MAX_DRAIN_MS, CONTRACT_MAX_P99_MS, LatencyHistogram } from "./benchmark-lib.ts";

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

async function* httpIdentities(path: string, dispatchPath?: string) {
  const dispatches = dispatchPath ? identities(dispatchPath) : undefined;
  try {
    for await (const http of identities(path)) {
      const start = await dispatches?.next();
      if (start && (start.done || start.value.id !== http.id || start.value.at > http.at))
        throw new Error("Invalid stream dispatch evidence for HTTP identity");
      yield { ...http, issuedAt: start?.value?.at };
    }
    if (dispatches && !(await dispatches.next()).done)
      throw new Error("Extra stream dispatch evidence");
  } finally {
    await dispatches?.return();
  }
}

export async function auditCapacityIdentities(
  httpPath: string,
  ackPath: string,
  startedAt: number,
  seconds: number,
  dispatchPath?: string,
  maxP99Ms = CONTRACT_MAX_P99_MS,
  streamFill?: { arrivalsStartedAt: number; minimumStreamMs: number },
) {
  const arrivalsStartedAt = streamFill?.arrivalsStartedAt ?? startedAt;
  const minimumStreamMs = streamFill?.minimumStreamMs ?? 0;
  if (
    !Number.isSafeInteger(startedAt) ||
    !Number.isSafeInteger(seconds) ||
    seconds < 1 ||
    seconds > 3600 ||
    !Number.isFinite(maxP99Ms) ||
    maxP99Ms <= 0 ||
    maxP99Ms > CONTRACT_MAX_P99_MS ||
    (streamFill &&
      (!dispatchPath ||
        !Number.isSafeInteger(arrivalsStartedAt) ||
        !Number.isSafeInteger(minimumStreamMs) ||
        minimumStreamMs <= 0 ||
        startedAt - arrivalsStartedAt < minimumStreamMs))
  )
    throw new Error("Invalid capacity audit window");
  const paths = dispatchPath ? [httpPath, ackPath, dispatchPath] : [httpPath, ackPath];
  const sorted = paths.map((path) => `${path}.${crypto.randomUUID()}.sorted`);
  try {
    for (const [index, path] of paths.entries()) {
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
    const http = httpIdentities(sorted[0]!, sorted[2]);
    const ack = identities(sorted[1]!);
    const latency = new LatencyHistogram();
    const minuteCompletions = Array.from({ length: Math.ceil(seconds / 60) }, () => 0);
    const issuedMinutes = minuteCompletions.map(() => ({ samples: 0, aboveBudget: 0 }));
    const longStream = streamFill
      ? { minuteStarts: minuteCompletions.map(() => 0), inflightAtWindowStart: 0, shortStreams: 0 }
      : undefined;
    let dispatchesPastDrain = 0;
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
          const finishedAt = Math.max(left.value.at, right.value.at);
          if (left.value.issuedAt !== undefined) {
            const issuedAt = left.value.issuedAt;
            if (right.value.at < issuedAt) throw new Error("ACK predates stream dispatch");
            if (issuedAt < arrivalsStartedAt) throw new Error("Stream dispatch predates arrivals");
            const elapsed = finishedAt - issuedAt;
            latency.add(elapsed);
            const sinceWindowStart = issuedAt - startedAt;
            const index = Math.floor(sinceWindowStart / 60000);
            if (longStream) {
              if (left.value.at - issuedAt < minimumStreamMs) longStream.shortStreams++;
              if (issuedAt < startedAt && left.value.at >= startedAt)
                longStream.inflightAtWindowStart++;
              if (sinceWindowStart >= 0 && sinceWindowStart < seconds * 1000)
                longStream.minuteStarts[index]!++;
            }
            if (sinceWindowStart > seconds * 1000 + CONTRACT_MAX_DRAIN_MS) {
              dispatchesPastDrain++;
            } else if (index >= 0) {
              while (issuedMinutes.length <= index)
                issuedMinutes.push({ samples: 0, aboveBudget: 0 });
              const minute = issuedMinutes[index]!;
              minute.samples++;
              if (elapsed > maxP99Ms) minute.aboveBudget++;
            }
          }
          const at = finishedAt - startedAt;
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
      ...(longStream ? { longStream } : {}),
      ...(dispatchPath
        ? {
            completionLatencyMs: {
              samples: latency.count,
              maxP99Ms,
              issuedMinutes,
              dispatchesPastDrain,
              p50: latency.count ? latency.quantile(0.5) : null,
              p95: latency.count ? latency.quantile(0.95) : null,
              p99: latency.count ? latency.quantile(0.99) : null,
              p999: latency.count ? latency.quantile(0.999) : null,
            },
          }
        : {}),
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
