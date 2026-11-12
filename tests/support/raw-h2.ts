import { connect, type TLSSocket } from "node:tls";
export function h2Frame(
  type: number,
  flags: number,
  stream: number,
  payload: Uint8Array = Buffer.alloc(0),
) {
  const header = Buffer.alloc(9);
  header.writeUIntBE(payload.length, 0, 3);
  header[3] = type;
  header[4] = flags;
  header.writeUInt32BE(stream, 5);
  return Buffer.concat([header, payload]);
}
function string(value: string) {
  const bytes = Buffer.from(value);
  if (bytes.length >= 127) throw new Error("Synthetic header too long");
  return Buffer.concat([Buffer.from([bytes.length]), bytes]);
}
export function identityHeaders(port: number) {
  return Buffer.concat([
    Buffer.from([0x83, 0x87, 0x04]),
    string("/_internal/core/v1/identity/challenges"),
    Buffer.from([0x01]),
    string(`localhost:${port}`),
    Buffer.from([0x0f, 0x10]),
    string("application/json"),
  ]);
}
export async function openRawH2(options: { port: number; ca: string; key: string; cert: string }) {
  const socket = connect({
    host: "127.0.0.1",
    port: options.port,
    servername: "localhost",
    ca: options.ca,
    key: options.key,
    cert: options.cert,
    ALPNProtocols: ["h2"],
    rejectUnauthorized: true,
  });
  socket.on("error", () => {});
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      socket.destroy();
      reject(new Error("Raw TLS timeout"));
    }, 2000);
    socket.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    socket.once("secureConnect", () => {
      clearTimeout(timer);
      resolve();
    });
  });
  let buffered = Buffer.alloc(0),
    data = Buffer.alloc(0),
    finished = false,
    resolveResponse!: (value: unknown) => void,
    rejectResponse!: (error: Error) => void;
  const response = new Promise<unknown>((resolve, reject) => {
    resolveResponse = resolve;
    rejectResponse = reject;
  });
  response.catch(() => {});
  const frames: {
    type: number;
    flags: number;
    stream: number;
    length: number;
    errorCode?: number;
  }[] = [];
  socket.on("data", (chunk) => {
    buffered = Buffer.concat([buffered, typeof chunk === "string" ? Buffer.from(chunk) : chunk]);
    while (buffered.length >= 9) {
      const length = buffered.readUIntBE(0, 3);
      if (buffered.length < 9 + length) break;
      const type = buffered[3]!,
        flags = buffered[4]!,
        stream = buffered.readUInt32BE(5) & 0x7fffffff,
        payload = buffered.subarray(9, 9 + length);
      frames.push({
        type,
        flags,
        stream,
        length,
        ...(type === 7 && length >= 8
          ? { errorCode: payload.readUInt32BE(4) }
          : type === 3 && length >= 4
            ? { errorCode: payload.readUInt32BE(0) }
            : {}),
      });
      if (type === 4 && flags === 0) socket.write(h2Frame(4, 1, 0));
      if (type === 0 && stream === 1) {
        data = Buffer.concat([data, payload]);
        if (flags & 1) {
          finished = true;
          try {
            resolveResponse(JSON.parse(data.toString()));
          } catch {
            rejectResponse(new Error("Invalid raw response"));
          }
        }
      }
      buffered = buffered.subarray(9 + length);
    }
  });
  const closed = new Promise<void>((resolve) =>
    socket.once("close", () => {
      if (!finished) rejectResponse(new Error("Raw stream closed"));
      resolve();
    }),
  );
  socket.write(Buffer.concat([Buffer.from("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"), h2Frame(4, 0, 0)]));
  return { socket, response, closed, frames };
}
export async function sendRaw(socket: TLSSocket, bytes: Buffer) {
  if (socket.destroyed) return false;
  return new Promise<boolean>((resolve) => {
    const done = (error?: Error | null) => resolve(!error && !socket.destroyed);
    socket.write(bytes, done);
  });
}

export async function boundedClose(closed: Promise<void>, ms: number) {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await new Promise<void>((resolve, reject) => {
      timer = setTimeout(() => reject(new Error("Raw connection not bounded")), ms);
      closed.then(resolve, reject);
    });
  } finally {
    clearTimeout(timer);
  }
}
export function hpackValueLength(length: number) {
  const bytes = [127];
  let remaining = length - 127;
  while (remaining >= 128) {
    bytes.push((remaining % 128) | 128);
    remaining = Math.floor(remaining / 128);
  }
  bytes.push(remaining);
  return Buffer.from(bytes);
}
