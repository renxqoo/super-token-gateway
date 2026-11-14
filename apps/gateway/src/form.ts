import { GatewayError } from "@gateway/contracts";
import { canonical } from "@gateway/kernel";
import { sha256 } from "@gateway/runtime";

export type FormFiles = Map<string, Uint8Array[]>;
type FileDescriptor = { file: { name: string; type: string; bytes: number; sha256: string } };

const singular = new Set(["model", "prompt", "input", "n", "stream"]);
const safeName = /^[^"\r\n\0]{1,200}$/;
const mediaType = /^[A-Za-z0-9!#$&^_.+-]{1,64}\/[A-Za-z0-9!#$&^_.+-]{1,64}$/;

export async function readForm(body: Uint8Array, contentType: string) {
  let form: FormData;
  try {
    form = await new Response(body.slice(), {
      headers: { "content-type": contentType },
    }).formData();
  } catch {
    throw new GatewayError(400, "invalid_request", "Multipart body is malformed");
  }
  const fields = new Map<string, (string | FileDescriptor)[]>();
  const files: FormFiles = new Map();
  for (const [name, value] of form.entries() as Iterable<[string, string | File]>) {
    if (!safeName.test(name))
      throw new GatewayError(400, "invalid_request", "Multipart field name is not supported");
    let entry: string | FileDescriptor;
    if (typeof value === "string") entry = value;
    else {
      if (value.name && !safeName.test(value.name))
        throw new GatewayError(400, "invalid_request", "Multipart file name is not supported");
      const data = new Uint8Array(await value.arrayBuffer());
      entry = {
        file: {
          name: value.name,
          type: mediaType.test(value.type) ? value.type : "application/octet-stream",
          bytes: data.length,
          sha256: sha256(data),
        },
      };
      const key = canonical(entry);
      files.set(key, [...(files.get(key) ?? []), data]);
    }
    const existing = fields.get(name);
    if (existing && singular.has(name))
      throw new GatewayError(400, "invalid_request", `Multipart ${name} must appear once`);
    fields.set(name, [...(existing ?? []), entry]);
  }
  const view = Object.fromEntries(
    [...fields].map(([name, values]) => [name, values.length === 1 ? values[0] : values]),
  );
  return { view, files };
}

export function encodeForm(edited: unknown, files: FormFiles, boundary: string): Uint8Array {
  if (typeof edited !== "object" || edited === null || Array.isArray(edited))
    throw new Error("form_view");
  const remaining = new Map([...files].map(([key, list]) => [key, [...list]]));
  const encoder = new TextEncoder();
  const parts: Uint8Array[] = [];
  for (const [name, value] of Object.entries(edited)) {
    if (!safeName.test(name)) throw new Error("form_name");
    for (const item of Array.isArray(value) ? value : [value]) {
      let head: string, data: Uint8Array;
      if (typeof item === "string") {
        head = `Content-Disposition: form-data; name="${name}"`;
        data = encoder.encode(item);
      } else {
        const key = canonical(item);
        const available = remaining.get(key);
        if (!available?.length) throw new Error("form_file");
        data = available.shift()!;
        const file = (item as FileDescriptor).file;
        head = `Content-Disposition: form-data; name="${name}"; filename="${file.name}"\r\nContent-Type: ${file.type}`;
      }
      parts.push(encoder.encode(`--${boundary}\r\n${head}\r\n\r\n`), data, encoder.encode("\r\n"));
    }
  }
  if ([...remaining.values()].some((list) => list.length)) throw new Error("form_file");
  parts.push(encoder.encode(`--${boundary}--\r\n`));
  return Buffer.concat(parts);
}
