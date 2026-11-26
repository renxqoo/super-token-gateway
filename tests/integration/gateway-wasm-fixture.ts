function encodeLebU(n: number): number[] {
  const out: number[] = [];
  let v = n >>> 0;
  do {
    let b = v & 0x7f;
    v >>>= 7;
    if (v !== 0) b |= 0x80;
    out.push(b);
  } while (v !== 0);
  return out;
}

function encodeLebS64(v: bigint): number[] {
  const out: number[] = [];
  let cur = v;
  let more = true;
  while (more) {
    let b = Number(cur & 0x7fn);
    cur >>= 7n;
    if ((cur === 0n && (b & 0x40) === 0) || (cur === -1n && (b & 0x40) !== 0)) {
      more = false;
    } else {
      b |= 0x80;
    }
    out.push(b);
  }
  return out;
}

function encodeWasmSec(id: number, body: number[]): number[] {
  return [id, ...encodeLebU(body.length), ...body];
}

function encodeWasmStr(s: string): number[] {
  const bytes = [...new TextEncoder().encode(s)];
  return [...encodeLebU(bytes.length), ...bytes];
}

export function buildRealMatchWasm(
  hooks: readonly string[],
  needle: string,
  matchReply: Record<string, unknown>,
  defaultReply: Record<string, unknown> = { action: "PASS" },
): Uint8Array {
  const enc = new TextEncoder();
  const needleBytes = [...enc.encode(needle)];
  const matchBytes = [...enc.encode(JSON.stringify(matchReply))];
  const passBytes = [...enc.encode(JSON.stringify(defaultReply))];
  const needleOff = 64;
  const matchOff = 512;
  const passOff = 1536;
  const matchPacked = (BigInt(matchOff) << 32n) | BigInt(matchBytes.length);
  const passPacked = (BigInt(passOff) << 32n) | BigInt(passBytes.length);

  const types = encodeWasmSec(1, [2, 0x60, 1, 0x7f, 1, 0x7f, 0x60, 2, 0x7f, 0x7f, 1, 0x7e]);
  const funcs = encodeWasmSec(3, [2, 0, 1]);
  const mem = encodeWasmSec(5, [1, 0x00, 1]);
  const exportEntries: number[] = [
    ...encodeWasmStr("memory"),
    2,
    0,
    ...encodeWasmStr("stg_alloc"),
    0,
    0,
  ];
  for (const h of hooks) {
    exportEntries.push(...encodeWasmStr(h), 0, 1);
  }
  const exps = encodeWasmSec(7, [2 + hooks.length, ...exportEntries]);
  const allocBody = [0, 0x41, ...encodeLebS64(4096n), 0x0b];
  const nLen = needleBytes.length;
  const hookBody = [
    1,
    2,
    0x7f,
    0x41,
    0x00,
    0x21,
    0x02,
    0x03,
    0x40,
    0x20,
    0x02,
    0x41,
    ...encodeLebS64(BigInt(nLen)),
    0x6a,
    0x20,
    0x01,
    0x4b,
    0x04,
    0x40,
    0x42,
    ...encodeLebS64(passPacked),
    0x0f,
    0x0b,
    0x41,
    0x00,
    0x21,
    0x03,
    0x02,
    0x40,
    0x03,
    0x40,
    0x20,
    0x03,
    0x41,
    ...encodeLebS64(BigInt(nLen)),
    0x4f,
    0x04,
    0x40,
    0x42,
    ...encodeLebS64(matchPacked),
    0x0f,
    0x0b,
    0x20,
    0x00,
    0x20,
    0x02,
    0x6a,
    0x20,
    0x03,
    0x6a,
    0x2d,
    0x00,
    0x00,
    0x41,
    ...encodeLebS64(BigInt(needleOff)),
    0x20,
    0x03,
    0x6a,
    0x2d,
    0x00,
    0x00,
    0x47,
    0x0d,
    0x01,
    0x20,
    0x03,
    0x41,
    0x01,
    0x6a,
    0x21,
    0x03,
    0x0c,
    0x00,
    0x0b,
    0x0b,
    0x20,
    0x02,
    0x41,
    0x01,
    0x6a,
    0x21,
    0x02,
    0x0c,
    0x00,
    0x0b,
    0x42,
    ...encodeLebS64(passPacked),
    0x0b,
  ];
  const code = encodeWasmSec(10, [
    2,
    ...encodeLebU(allocBody.length),
    ...allocBody,
    ...encodeLebU(hookBody.length),
    ...hookBody,
  ]);
  const data = encodeWasmSec(11, [
    3,
    0,
    0x41,
    ...encodeLebS64(BigInt(needleOff)),
    0x0b,
    ...encodeLebU(needleBytes.length),
    ...needleBytes,
    0,
    0x41,
    ...encodeLebS64(BigInt(matchOff)),
    0x0b,
    ...encodeLebU(matchBytes.length),
    ...matchBytes,
    0,
    0x41,
    ...encodeLebS64(BigInt(passOff)),
    0x0b,
    ...encodeLebU(passBytes.length),
    ...passBytes,
  ]);
  return new Uint8Array([
    0x00,
    0x61,
    0x73,
    0x6d,
    1,
    0,
    0,
    0,
    ...types,
    ...funcs,
    ...mem,
    ...exps,
    ...code,
    ...data,
  ]);
}

export function buildRealLoopWasm(): Uint8Array {
  const types = encodeWasmSec(1, [2, 0x60, 1, 0x7f, 1, 0x7f, 0x60, 2, 0x7f, 0x7f, 1, 0x7e]);
  const funcs = encodeWasmSec(3, [2, 0, 1]);
  const mem = encodeWasmSec(5, [1, 0x00, 1]);
  const exps = encodeWasmSec(7, [
    3,
    ...encodeWasmStr("memory"),
    2,
    0,
    ...encodeWasmStr("stg_alloc"),
    0,
    0,
    ...encodeWasmStr("on_response"),
    0,
    1,
  ]);
  const b0 = [0, 0x41, ...encodeLebS64(1024n), 0x0b];
  const b1 = [0, 0x03, 0x40, 0x0c, 0x00, 0x0b, 0x42, 0x00, 0x0b];
  const code = encodeWasmSec(10, [
    2,
    ...encodeLebU(b0.length),
    ...b0,
    ...encodeLebU(b1.length),
    ...b1,
  ]);
  return new Uint8Array([
    0x00,
    0x61,
    0x73,
    0x6d,
    1,
    0,
    0,
    0,
    ...types,
    ...funcs,
    ...mem,
    ...exps,
    ...code,
  ]);
}
