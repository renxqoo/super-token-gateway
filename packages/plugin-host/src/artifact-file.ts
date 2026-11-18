import { open } from "node:fs/promises";
import { constants } from "node:fs";

export async function readPluginArtifact(path: string, maxBytes: number, signal?: AbortSignal) {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1 || maxBytes > 256 << 20)
    throw new Error("PLUGIN_ARTIFACT_BUDGET_INVALID");
  signal?.throwIfAborted();
  const file = await open(path, constants.O_RDONLY | constants.O_NONBLOCK);
  try {
    const state = await file.stat();
    if (!state.isFile() || state.size < 1 || state.size > maxBytes)
      throw new Error("PLUGIN_ARTIFACT_BUDGET");
    const bytes = new Uint8Array(state.size);
    let at = 0;
    while (at < bytes.length) {
      signal?.throwIfAborted();
      const { bytesRead } = await file.read(bytes, at, Math.min(65536, bytes.length - at), at);
      if (bytesRead === 0) throw new Error("PLUGIN_ARTIFACT_CHANGED");
      at += bytesRead;
    }
    if ((await file.read(new Uint8Array(1), 0, 1, at)).bytesRead)
      throw new Error("PLUGIN_ARTIFACT_CHANGED");
    signal?.throwIfAborted();
    return bytes;
  } finally {
    await file.close();
  }
}
