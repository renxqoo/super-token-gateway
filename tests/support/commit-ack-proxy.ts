import { createServer, connect, type Socket } from "node:net";
import { appendFile } from "node:fs/promises";
import type { SQL } from "bun";

function relayUntilMarkedCommit(client: Socket, notice: string, dropCommit: () => boolean) {
  const message = Buffer.from("M" + notice + "\0"),
    commit = Buffer.from("COMMIT\0");
  let pending = Buffer.alloc(0),
    marked = false,
    startup = true;
  return (chunk: Buffer) => {
    if (startup && chunk.length === 1 && chunk[0] === 78) {
      client.write(chunk);
      return;
    }
    startup = false;
    const bytes = pending.length ? Buffer.concat([pending, chunk]) : chunk;
    let offset = 0;
    while (bytes.length - offset >= 5) {
      const length = bytes.readUInt32BE(offset + 1);
      if (length < 4) {
        client.destroy();
        return;
      }
      if (bytes.length - offset < length + 1) break;
      const tag = bytes[offset],
        body = bytes.subarray(offset + 5, offset + length + 1);
      if (tag === 78 && body.includes(message)) marked = true;
      if (marked && tag === 67 && body.equals(commit) && dropCommit()) return;
      if (tag === 69 || tag === 90) marked = false;
      client.write(bytes.subarray(offset, offset + length + 1));
      offset += length + 1;
    }
    pending = Buffer.from(bytes.subarray(offset));
  };
}

async function createProxy(notice?: string) {
  const target = new URL(process.env.DATABASE_URL!),
    sockets = new Set<Socket>();
  let armed = false,
    hold: "commit" | "acknowledgement" | undefined,
    triggers: string[] = [],
    finished: ((error?: Error) => void) | undefined,
    timer: ReturnType<typeof setTimeout> | undefined;
  const server = createServer((client) => {
    const upstream = connect({ host: target.hostname, port: Number(target.port) });
    sockets.add(client);
    sockets.add(upstream);
    let drop = false,
      withhold = false,
      primed = false,
      tail = Buffer.alloc(0);
    const close = () => {
      tail.fill(0);
      tail = Buffer.alloc(0);
      client.destroy();
      upstream.destroy();
    };
    client.on("error", close);
    upstream.on("error", close);
    client.on("close", () => {
      sockets.delete(client);
      upstream.destroy();
    });
    upstream.on("close", () => {
      sockets.delete(upstream);
      client.destroy();
    });
    if (notice) {
      client.on("data", (chunk) => upstream.write(chunk));
      const relay = relayUntilMarkedCommit(client, notice, () => {
        if (!armed) return false;
        armed = false;
        close();
        settle();
        return true;
      });
      upstream.on("data", (chunk) => relay(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
      return;
    }
    client.on("data", (chunk) => {
      const part = typeof chunk === "string" ? Buffer.from(chunk) : chunk;
      const bytes = Buffer.concat([tail, part]);
      const text = bytes.toString("latin1");
      if (armed && triggers.length && triggers.every((trigger) => text.includes(trigger)))
        primed = true;
      if (armed && (!triggers.length || primed) && /COMMIT(?:\s+TRANSACTION)?\0/.test(text)) {
        drop = true;
        armed = false;
        if (hold === "commit") {
          withhold = true;
          settle();
        }
      }
      tail.fill(0);
      tail = Buffer.from(bytes.subarray(Math.max(0, bytes.length - 128)));
      bytes.fill(0);
      if (!withhold) upstream.write(chunk);
    });
    upstream.on("data", (chunk) => {
      const part = typeof chunk === "string" ? Buffer.from(chunk) : chunk;
      if (drop) {
        if (part.includes(Buffer.from("COMMIT\0"))) {
          settle();
          if (!hold) close();
        }
      } else client.write(chunk);
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  function settle() {
    const complete = finished;
    finished = undefined;
    clearTimeout(timer);
    timer = undefined;
    complete?.();
  }
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing proxy port");
  await appendFile(
    ".local/T04b2a-resources.jsonl",
    JSON.stringify({ event: "commit_proxy_created", port: address.port }) + "\n",
    { mode: 0o600 },
  );
  return {
    port: address.port,
    arm(trigger: string[] = [], timeoutMs = 3000, mode?: "commit" | "acknowledgement") {
      if (armed || finished) throw new Error("Proxy already armed");
      armed = true;
      hold = mode;
      triggers = trigger;
      return new Promise<void>((resolve, reject) => {
        finished = (error) => (error ? reject(error) : resolve());
        timer = setTimeout(() => {
          armed = false;
          finished = undefined;
          timer = undefined;
          reject(new Error("COMMIT acknowledgement was not observed"));
        }, timeoutMs);
      });
    },
    async close() {
      armed = false;
      clearTimeout(timer);
      timer = undefined;
      finished?.(new Error("Proxy closed"));
      finished = undefined;
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await appendFile(
        ".local/T04b2a-resources.jsonl",
        JSON.stringify({
          event: "commit_proxy_closed",
          port: address.port,
          sockets: sockets.size,
          timers: timer ? 1 : 0,
        }) + "\n",
        { mode: 0o600 },
      );
      if (sockets.size) throw new Error("Proxy sockets retained");
    },
  };
}

export function createCommitAckProxy() {
  return createProxy();
}

export async function createPermitCommitAckProxy(state: "RESERVED" | "SEND_GRANTED") {
  const notice = "permit_commit_" + crypto.randomUUID().replaceAll("-", "");
  const proxy = await createProxy(notice);
  return {
    port: proxy.port,
    close: proxy.close,
    async arm(db: SQL, timeoutMs: number) {
      const event = state === "RESERVED" ? "INSERT" : "UPDATE";
      const condition =
        state === "RESERVED"
          ? "NEW.state='RESERVED'"
          : "OLD.state='RESERVED' AND NEW.state='SEND_GRANTED'";
      await db.unsafe(`
        CREATE FUNCTION test_permit_commit_notice() RETURNS trigger LANGUAGE plpgsql AS $$
          BEGIN RAISE NOTICE '${notice}'; RETURN NULL; END $$;
        CREATE CONSTRAINT TRIGGER test_permit_commit_notice AFTER ${event} ON execution_permits
          DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (${condition})
          EXECUTE FUNCTION test_permit_commit_notice()
      `);
      return { observed: proxy.arm([], timeoutMs) };
    },
  };
}
