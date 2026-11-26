const encoder = new TextEncoder();

export function sliceTailForMaxBytes(
  text: string,
  maxBytes: number,
): { head: string; tail: string } {
  if (maxBytes <= 0 || text.length === 0) return { head: text, tail: "" };
  let bytes = 0;
  let cut = text.length;
  while (cut > 0) {
    let prev = cut - 1;
    const code = text.charCodeAt(prev);
    if (code >= 0xdc00 && code <= 0xdfff && prev > 0) {
      const high = text.charCodeAt(prev - 1);
      if (high >= 0xd800 && high <= 0xdbff) prev--;
    }
    const charBytes = encoder.encode(text.slice(prev, cut)).byteLength;
    if (bytes + charBytes > maxBytes) break;
    bytes += charBytes;
    cut = prev;
  }
  const safeCut = safeSliceBoundary(text, cut);
  return { head: text.slice(0, safeCut), tail: text.slice(safeCut) };
}

export function safeSliceBoundary(text: string, targetIndex: number): number {
  let idx = Math.max(0, Math.min(text.length, targetIndex));
  if (idx > 0 && idx < text.length) {
    const code = text.charCodeAt(idx - 1);
    if (code >= 0xd800 && code <= 0xdbff) idx--;
  }
  return idx;
}

export function splitChunkForMaxBytes(text: string, start: number, maxBytes: number): number {
  const approx = Math.min(text.length, start + maxBytes);
  let end = safeSliceBoundary(text, approx);
  if (end <= start) end = Math.min(text.length, start + 1);
  while (end > start + 1 && encoder.encode(text.slice(start, end)).byteLength > maxBytes) {
    const excess = Math.max(1, Math.floor((end - start) / 4));
    end = safeSliceBoundary(text, end - excess);
  }
  return Math.max(start + 1, end);
}
