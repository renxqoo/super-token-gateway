import { GatewayError } from "@gateway/contracts";

const MAX_MULTIPART_PARTS = 1024;
const MAX_PART_HEADER_BYTES = 16 * 1024;
const textDecoder = new TextDecoder("utf-8");

function hasHeaderControlChar(value: string): boolean {
  for (let i = 0; i < value.length; i++) {
    const code = value.charCodeAt(i);
    if (code < 0x20 || code === 0x7f) return true;
  }
  return false;
}

function invalidMultipart(): never {
  throw new GatewayError(400, "invalid_request", "Invalid multipart body");
}

function extractBoundary(contentType: string): string {
  const semi = contentType.indexOf(";");
  const mediaType = (semi === -1 ? contentType : contentType.slice(0, semi)).trim().toLowerCase();
  if (mediaType !== "multipart/form-data" || semi === -1) invalidMultipart();
  const params = contentType.slice(semi + 1);
  const match = /(?:^|;)\s*boundary=(?:"([^"\r\n]+)"|([^;\s\r\n]+))/i.exec(params);
  const boundary = (match?.[1] ?? match?.[2] ?? "").trim();
  if (!boundary || boundary.length > 70 || hasHeaderControlChar(boundary)) {
    invalidMultipart();
  }
  return boundary;
}

function unescapeQuotedParam(value: string): string {
  return value.replace(/\\(.)/g, "$1");
}

function parseContentDisposition(value: string): { name: string; filename?: string } {
  const semi = value.indexOf(";");
  const disposition = (semi === -1 ? value : value.slice(0, semi)).trim().toLowerCase();
  if (disposition !== "form-data" || semi === -1) invalidMultipart();
  const params = value.slice(semi + 1);
  let name: string | undefined;
  let filename: string | undefined;
  let extFilename: string | undefined;
  const paramPattern = /;\s*([a-zA-Z0-9_-]+\*?)\s*=\s*(?:"((?:\\.|[^"\\])*)"|([^;\r\n]*))/g;
  const input = `;${params}`;
  for (const match of input.matchAll(paramPattern)) {
    const key = match[1]!.toLowerCase();
    const rawVal = match[2] !== undefined ? unescapeQuotedParam(match[2]) : match[3]!.trim();
    if (key === "name") {
      name = rawVal;
    } else if (key === "filename") {
      filename = rawVal;
    } else if (key === "filename*") {
      const encoded = /^[^']*'[^']*'(.*)$/.exec(rawVal)?.[1];
      if (encoded !== undefined) {
        try {
          extFilename = decodeURIComponent(encoded);
        } catch {
          invalidMultipart();
        }
      }
    }
  }
  if (name === undefined) invalidMultipart();
  const resolvedFilename = extFilename ?? filename;
  return resolvedFilename !== undefined ? { name, filename: resolvedFilename } : { name };
}

export function parseMultipartFormData(
  body: Uint8Array,
  contentType: string,
): [string, string | File][] {
  const boundary = extractBoundary(contentType);
  const buf = Buffer.from(body.buffer, body.byteOffset, body.byteLength);
  const firstDelim = Buffer.from(`--${boundary}`);
  const nextDelim = Buffer.from(`\r\n--${boundary}`);
  const headerSep = Buffer.from("\r\n\r\n");

  let cursor: number;
  if (buf.length >= firstDelim.length && buf.subarray(0, firstDelim.length).equals(firstDelim)) {
    cursor = firstDelim.length;
  } else {
    const found = buf.indexOf(nextDelim);
    if (found === -1) invalidMultipart();
    cursor = found + nextDelim.length;
  }

  const entries: [string, string | File][] = [];
  let closed = false;

  while (cursor <= buf.length) {
    if (cursor + 1 < buf.length && buf[cursor] === 45 && buf[cursor + 1] === 45) {
      closed = true;
      break;
    }
    while (cursor < buf.length && (buf[cursor] === 32 || buf[cursor] === 9)) cursor++;
    if (cursor + 1 >= buf.length || buf[cursor] !== 13 || buf[cursor + 1] !== 10) {
      invalidMultipart();
    }
    cursor += 2;

    if (entries.length >= MAX_MULTIPART_PARTS) invalidMultipart();

    const headerEnd = buf.indexOf(headerSep, cursor);
    if (headerEnd === -1 || headerEnd - cursor > MAX_PART_HEADER_BYTES) invalidMultipart();

    const headerBlock = buf.subarray(cursor, headerEnd).toString("utf8");
    const bodyStart = headerEnd + 4;
    const partEnd = buf.indexOf(nextDelim, bodyStart);
    if (partEnd === -1) invalidMultipart();

    let disposition: { name: string; filename?: string } | undefined;
    let partContentType: string | undefined;
    for (const line of headerBlock.split("\r\n")) {
      const colon = line.indexOf(":");
      if (colon <= 0) invalidMultipart();
      const headerName = line.slice(0, colon).trim().toLowerCase();
      const headerValue = line.slice(colon + 1).trim();
      if (hasHeaderControlChar(headerValue)) invalidMultipart();
      if (headerName === "content-disposition") {
        disposition = parseContentDisposition(headerValue);
      } else if (headerName === "content-type") {
        partContentType = headerValue;
      }
    }
    if (!disposition) invalidMultipart();

    if (disposition.filename !== undefined) {
      const fileBytes = body.slice(bodyStart, partEnd);
      const file = new File(
        [fileBytes],
        disposition.filename,
        partContentType ? { type: partContentType } : undefined,
      );
      entries.push([disposition.name, file]);
    } else {
      entries.push([disposition.name, textDecoder.decode(body.subarray(bodyStart, partEnd))]);
    }

    cursor = partEnd + nextDelim.length;
  }

  if (!closed) invalidMultipart();
  return entries;
}
