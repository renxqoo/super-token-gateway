import { createServer, connect, type Socket } from "node:net";
import { appendFile } from "node:fs/promises";
export async function createCommitAckProxy() {
  const target = new URL(process.env.DATABASE_URL!),
    sockets = new Set<Socket>();
  let armed = false,
    finished: ((error?: Error) => void) | undefined,
    timer: ReturnType<typeof setTimeout> | undefined;
  const server = createServer((client) => {
    const upstream = connect({ host: target.hostname, port: Number(target.port) });
    sockets.add(client);
    sockets.add(upstream);
    let drop = false,
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
    client.on("data", (chunk) => {
      const part = typeof chunk === "string" ? Buffer.from(chunk) : chunk;
      const bytes = Buffer.concat([tail, part]);
      if (armed && /COMMIT(?:\s+TRANSACTION)?\0/.test(bytes.toString("latin1"))) {
        drop = true;
        armed = false;
      }
      tail.fill(0);
      tail = Buffer.from(bytes.subarray(Math.max(0, bytes.length - 128)));
      bytes.fill(0);
      upstream.write(chunk);
    });
    upstream.on("data", (chunk) => {
      const part = typeof chunk === "string" ? Buffer.from(chunk) : chunk;
      if (drop) {
        if (part.includes(Buffer.from("COMMIT\0"))) {
          const complete = finished;
          finished = undefined;
          clearTimeout(timer);
          timer = undefined;
          complete?.();
          close();
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
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing proxy port");
  await appendFile(
    ".local/T04b2a-resources.jsonl",
    JSON.stringify({ event: "commit_proxy_created", port: address.port }) + "\n",
    { mode: 0o600 },
  );
  return {
    port: address.port,
    arm() {
      if (armed || finished) throw new Error("Proxy already armed");
      armed = true;
      return new Promise<void>((resolve, reject) => {
        finished = (error) => (error ? reject(error) : resolve());
        timer = setTimeout(() => {
          armed = false;
          finished = undefined;
          timer = undefined;
          reject(new Error("COMMIT acknowledgement was not observed"));
        }, 3000);
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
