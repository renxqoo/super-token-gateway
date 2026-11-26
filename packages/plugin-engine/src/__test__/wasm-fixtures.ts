import type { PluginHook } from "../index.ts";

function lebU(n: number): number[] {
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

function lebS64(v: bigint): number[] {
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

function lebS32(v: number): number[] {
  return lebS64(BigInt(v));
}

function wasmSection(id: number, body: number[]): number[] {
  return [id, ...lebU(body.length), ...body];
}

function wasmStr(s: string): number[] {
  const bytes = [...new TextEncoder().encode(s)];
  return [...lebU(bytes.length), ...bytes];
}

export function buildSubstringWasm(options: {
  hooks: readonly PluginHook[];
  needle: string;
  matchReply: Record<string, unknown>;
  passReply?: Record<string, unknown>;
}): Uint8Array {
  const enc = new TextEncoder();
  const needleBytes = [...enc.encode(options.needle)];
  const matchBytes = [...enc.encode(JSON.stringify(options.matchReply))];
  const passBytes = [...enc.encode(JSON.stringify(options.passReply ?? { action: "PASS" }))];
  const needleOff = 64;
  const matchOff = 512;
  const passOff = 1536;
  const matchPacked = (BigInt(matchOff) << 32n) | BigInt(matchBytes.length);
  const passPacked = (BigInt(passOff) << 32n) | BigInt(passBytes.length);

  const types = wasmSection(1, [2, 0x60, 1, 0x7f, 1, 0x7f, 0x60, 2, 0x7f, 0x7f, 1, 0x7e]);
  const funcs = wasmSection(3, [2, 0, 1]);
  const mem = wasmSection(5, [1, 0x00, 1]);
  const exportEntries: number[] = [...wasmStr("memory"), 2, 0, ...wasmStr("stg_alloc"), 0, 0];
  for (const h of options.hooks) {
    exportEntries.push(...wasmStr(h), 0, 1);
  }
  const exps = wasmSection(7, [2 + options.hooks.length, ...exportEntries]);

  const allocBody = [0, 0x41, ...lebS32(4096), 0x0b];
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
    ...lebS32(nLen),
    0x6a,
    0x20,
    0x01,
    0x4b,
    0x04,
    0x40,
    0x42,
    ...lebS64(passPacked),
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
    ...lebS32(nLen),
    0x4f,
    0x04,
    0x40,
    0x42,
    ...lebS64(matchPacked),
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
    ...lebS32(needleOff),
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
    ...lebS64(passPacked),
    0x0b,
  ];
  const code = wasmSection(10, [
    2,
    ...lebU(allocBody.length),
    ...allocBody,
    ...lebU(hookBody.length),
    ...hookBody,
  ]);
  const data = wasmSection(11, [
    3,
    0,
    0x41,
    ...lebS32(needleOff),
    0x0b,
    ...lebU(needleBytes.length),
    ...needleBytes,
    0,
    0x41,
    ...lebS32(matchOff),
    0x0b,
    ...lebU(matchBytes.length),
    ...matchBytes,
    0,
    0x41,
    ...lebS32(passOff),
    0x0b,
    ...lebU(passBytes.length),
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

export function buildInfiniteLoopWasm(hook: PluginHook): Uint8Array {
  const types = wasmSection(1, [2, 0x60, 1, 0x7f, 1, 0x7f, 0x60, 2, 0x7f, 0x7f, 1, 0x7e]);
  const funcs = wasmSection(3, [2, 0, 1]);
  const mem = wasmSection(5, [1, 0x00, 1]);
  const exps = wasmSection(7, [
    3,
    ...wasmStr("memory"),
    2,
    0,
    ...wasmStr("stg_alloc"),
    0,
    0,
    ...wasmStr(hook),
    0,
    1,
  ]);
  const b0 = [0, 0x41, ...lebS32(1024), 0x0b];
  const b1 = [0, 0x03, 0x40, 0x0c, 0x00, 0x0b, 0x42, 0x00, 0x0b];
  const code = wasmSection(10, [2, ...lebU(b0.length), ...b0, ...lebU(b1.length), ...b1]);
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

export function buildMemoryBombWasm(initialPages: number, growPages: number): Uint8Array {
  const passBytes = [...new TextEncoder().encode(JSON.stringify({ action: "PASS" }))];
  const passOff = 512;
  const passPacked = (BigInt(passOff) << 32n) | BigInt(passBytes.length);
  const types = wasmSection(1, [2, 0x60, 1, 0x7f, 1, 0x7f, 0x60, 2, 0x7f, 0x7f, 1, 0x7e]);
  const funcs = wasmSection(3, [2, 0, 1]);
  const mem = wasmSection(5, [1, 0x00, ...lebU(initialPages)]);
  const exps = wasmSection(7, [
    3,
    ...wasmStr("memory"),
    2,
    0,
    ...wasmStr("stg_alloc"),
    0,
    0,
    ...wasmStr("on_response"),
    0,
    1,
  ]);
  const b0 = [0, 0x41, ...lebS32(1024), 0x0b];
  const b1 = [
    0,
    ...(growPages > 0 ? [0x41, ...lebS32(growPages), 0x40, 0x00, 0x1a] : []),
    0x42,
    ...lebS64(passPacked),
    0x0b,
  ];
  const code = wasmSection(10, [2, ...lebU(b0.length), ...b0, ...lebU(b1.length), ...b1]);
  const data = wasmSection(11, [
    1,
    0,
    0x41,
    ...lebS32(passOff),
    0x0b,
    ...lebU(passBytes.length),
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

export function buildWasiImportWasm(): Uint8Array {
  const types = wasmSection(
    1,
    [3, 0x60, 0, 0, 0x60, 1, 0x7f, 1, 0x7f, 0x60, 2, 0x7f, 0x7f, 1, 0x7e],
  );
  const imports = wasmSection(2, [
    1,
    ...wasmStr("wasi_snapshot_preview1"),
    ...wasmStr("proc_exit"),
    0x00,
    0,
  ]);
  const funcs = wasmSection(3, [2, 1, 2]);
  const mem = wasmSection(5, [1, 0x00, 1]);
  const exps = wasmSection(7, [
    3,
    ...wasmStr("memory"),
    2,
    0,
    ...wasmStr("stg_alloc"),
    0,
    1,
    ...wasmStr("on_response"),
    0,
    2,
  ]);
  const b0 = [0, 0x41, ...lebS32(1024), 0x0b];
  const b1 = [0, 0x42, 0x00, 0x0b];
  const code = wasmSection(10, [2, ...lebU(b0.length), ...b0, ...lebU(b1.length), ...b1]);
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
    ...imports,
    ...funcs,
    ...mem,
    ...exps,
    ...code,
  ]);
}

export function buildCrossTenantProbeWasm(): Uint8Array {
  const enc = new TextEncoder();
  const passBytes = [...enc.encode(JSON.stringify({ action: "PASS" }))];
  const leakBytes = [
    ...enc.encode(JSON.stringify({ action: "BLOCK", rule_code: "MEMORY_LEAKED" })),
  ];
  const passOff = 512;
  const leakOff = 1024;
  const sentinelOff = 2048;
  const passPacked = (BigInt(passOff) << 32n) | BigInt(passBytes.length);
  const leakPacked = (BigInt(leakOff) << 32n) | BigInt(leakBytes.length);
  const types = wasmSection(1, [2, 0x60, 1, 0x7f, 1, 0x7f, 0x60, 2, 0x7f, 0x7f, 1, 0x7e]);
  const funcs = wasmSection(3, [2, 0, 1]);
  const mem = wasmSection(5, [1, 0x00, 1]);
  const exps = wasmSection(7, [
    3,
    ...wasmStr("memory"),
    2,
    0,
    ...wasmStr("stg_alloc"),
    0,
    0,
    ...wasmStr("on_response"),
    0,
    1,
  ]);
  const b0 = [0, 0x41, ...lebS32(4096), 0x0b];
  const b1 = [
    0,
    0x41,
    ...lebS32(sentinelOff),
    0x2d,
    0x00,
    0x00,
    0x41,
    0x00,
    0x47,
    0x04,
    0x40,
    0x42,
    ...lebS64(leakPacked),
    0x0f,
    0x0b,
    0x41,
    ...lebS32(sentinelOff),
    0x41,
    ...lebS32(0xab),
    0x3a,
    0x00,
    0x00,
    0x42,
    ...lebS64(passPacked),
    0x0b,
  ];
  const code = wasmSection(10, [2, ...lebU(b0.length), ...b0, ...lebU(b1.length), ...b1]);
  const data = wasmSection(11, [
    2,
    0,
    0x41,
    ...lebS32(passOff),
    0x0b,
    ...lebU(passBytes.length),
    ...passBytes,
    0,
    0x41,
    ...lebS32(leakOff),
    0x0b,
    ...lebU(leakBytes.length),
    ...leakBytes,
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
