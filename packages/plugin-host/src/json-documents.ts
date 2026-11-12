import { sha256, type JSONDocumentDescriptor, type JSONNodeFact } from "@gateway/plugin-sdk";
import { PluginHostError } from "./supervisor.ts";
type Node = {
  kind: "object" | "array" | "string" | "number" | "boolean" | "null";
  start: number;
  end: number;
  memberStart?: number;
  children?: Map<string, Node>;
  items?: Node[];
};
export type JSONDocument = JSONDocumentDescriptor;
export type JSONFact = JSONNodeFact;
export type JSONEdit = { op: "set" | "remove"; pointer: string; value?: string };
export type JSONPlan = { document: string; edits: JSONEdit[] };
type Document = { owner: string; bytes: Buffer; root: Node; charge: number; consumed: boolean };
export function createJSONDocuments(maxBytes = 256 << 20) {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1024 || maxBytes > 1024 ** 3)
    throw new PluginHostError("CONFIG_INVALID");
  const documents = new Map<string, Document>(),
    outputs = new Map<string, { owner: string; bytes: Uint8Array }>();
  const external = new Map<string, number>();
  let retained = 0,
    peakBytes = 0;
  function reserve(bytes: number) {
    if (retained + bytes > maxBytes) throw new PluginHostError("JSON_MEMORY_BUDGET");
    retained += bytes;
    peakBytes = Math.max(peakBytes, retained);
  }
  function pointer(value: string): string[] {
    if (value.length > 2048 || (value !== "" && !value.startsWith("/")) || /~(?![01])/.test(value))
      throw new PluginHostError("JSON_POINTER");
    return value === ""
      ? []
      : value
          .slice(1)
          .split("/")
          .map((part) => part.replace(/~1/g, "/").replace(/~0/g, "~"));
  }
  function lookup(root: Node, parts: string[]): Node | undefined {
    let node: Node | undefined = root;
    for (const key of parts) {
      if (!node) return;
      if (node.kind === "object") node = node.children!.get(key);
      else if (node.kind === "array") {
        if (!/^(?:0|[1-9]\d*)$/.test(key) || key.length > 10)
          throw new PluginHostError("JSON_POINTER");
        node = node.items![Number(key)];
      } else return;
    }
    return node;
  }
  function index(bytes: Buffer, charge: (n: number) => void): Node {
    let i = 0,
      nodes = 0;
    new TextDecoder("utf-8", { fatal: true }).decode(bytes);
    function space() {
      while (i < bytes.length && [32, 9, 10, 13].includes(bytes[i]!)) i++;
    }
    function string() {
      if (bytes[i++] !== 34) throw new PluginHostError("JSON_INVALID");
      while (i < bytes.length) {
        const ch = bytes[i++]!;
        if (ch === 34) return;
        if (ch < 32) throw new PluginHostError("JSON_INVALID");
        if (ch === 92) {
          const escape = bytes[i++];
          if (escape === 117) {
            for (let n = 0; n < 4; n++) {
              const hex = bytes[i++];
              if (hex === undefined || !/[0-9a-fA-F]/.test(String.fromCharCode(hex)))
                throw new PluginHostError("JSON_INVALID");
            }
          } else if (escape === undefined || ![34, 92, 47, 98, 102, 110, 114, 116].includes(escape))
            throw new PluginHostError("JSON_INVALID");
        }
      }
      throw new PluginHostError("JSON_INVALID");
    }
    function value(depth: number): Node {
      if (depth > 64 || ++nodes > 100000) throw new PluginHostError("JSON_LIMIT");
      charge(1024);
      space();
      const start = i,
        ch = bytes[i];
      if (ch === 123 || ch === 91) {
        const array = ch === 91,
          end = array ? 93 : 125;
        const node: Node = {
          kind: array ? "array" : "object",
          start,
          end: 0,
          ...(array ? { items: [] } : { children: new Map() }),
        };
        i++;
        space();
        if (bytes[i] === end) {
          node.end = ++i;
          return node;
        }
        for (;;) {
          space();
          if (array) node.items!.push(value(depth + 1));
          else {
            const keyStart = i;
            string();
            charge((i - keyStart) * 2);
            const key = JSON.parse(bytes.subarray(keyStart, i).toString("utf8")) as string;
            if (node.children!.has(key)) throw new PluginHostError("JSON_DUPLICATE_KEY");
            space();
            if (bytes[i++] !== 58) throw new PluginHostError("JSON_INVALID");
            const child = value(depth + 1);
            child.memberStart = keyStart;
            node.children!.set(key, child);
          }
          space();
          const next = bytes[i++];
          if (next === end) {
            node.end = i;
            return node;
          }
          if (next !== 44) throw new PluginHostError("JSON_INVALID");
        }
      }
      if (ch === 34) {
        string();
        return { kind: "string", start, end: i };
      }
      for (const [keyword, kind] of [
        ["true", "boolean"],
        ["false", "boolean"],
        ["null", "null"],
      ] as const) {
        if (bytes.subarray(i, i + keyword.length).toString("utf8") === keyword) {
          i += keyword.length;
          return { kind, start, end: i };
        }
      }
      let end = i;
      while (end < bytes.length && ![32, 9, 10, 13, 44, 93, 125].includes(bytes[end]!)) end++;
      if (end - i > 200) throw new PluginHostError("JSON_NUMERIC_LIMIT");
      const literal = bytes.subarray(i, end).toString("utf8");
      if (!/^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/.test(literal))
        throw new PluginHostError("JSON_INVALID");
      i = end;
      return { kind: "number", start, end };
    }
    const root = value(0);
    space();
    if (i !== bytes.length) throw new PluginHostError("JSON_INVALID");
    return root;
  }
  function get(owner: string, handle: string): Document {
    const document = documents.get(handle);
    if (!document || document.owner !== owner || document.consumed)
      throw new PluginHostError("JSON_HANDLE");
    return document;
  }
  function drop(handle: string, doc: Document) {
    if (documents.delete(handle)) retained -= doc.charge;
  }
  return {
    register(owner: string, source: Uint8Array, selectedHandle?: string): JSONDocument {
      if (source.length > 16 << 20) throw new PluginHostError("JSON_BODY_BUDGET");
      let charge = source.length * 7 + (128 << 10);
      reserve(charge);
      try {
        const bytes = Buffer.from(source),
          root = index(bytes, (amount) => {
            reserve(amount);
            charge += amount;
          });
        const handle = selectedHandle ?? crypto.randomUUID();
        if (documents.has(handle)) throw new PluginHostError("JSON_HANDLE");
        documents.set(handle, { owner, bytes, root, charge, consumed: false });
        return { handle, bytes: bytes.length };
      } catch (error) {
        retained -= charge;
        throw error;
      }
    },
    inspect(owner: string, handle: string, paths: string[]): Record<string, JSONFact> {
      const doc = get(owner, handle);
      if (paths.length > 16 || new Set(paths).size !== paths.length)
        throw new PluginHostError("JSON_INSPECTION_BUDGET");
      const facts: Record<string, JSONFact> = Object.create(null) as Record<string, JSONFact>;
      for (const path of paths) {
        const node = lookup(doc.root, pointer(path));
        if (!node) {
          facts[path] = { kind: "missing", bytes: 0 };
          continue;
        }
        const bytes = node.end - node.start;
        let stringInfo: { utf16_units: number; digest: string } | undefined;
        if (node.kind === "string") {
          const guard = bytes * 6 + 512;
          reserve(guard);
          try {
            const text = JSON.parse(
              doc.bytes.subarray(node.start, node.end).toString("utf8"),
            ) as string;
            stringInfo = { utf16_units: text.length, digest: sha256(Buffer.from(text, "utf16le")) };
          } finally {
            retained -= guard;
          }
        }
        facts[path] = {
          kind: node.kind,
          bytes,
          ...(stringInfo ? { string_info: stringInfo } : {}),
          ...(node.kind === "object"
            ? { count: node.children!.size }
            : node.kind === "array"
              ? { count: node.items!.length }
              : {}),
          ...(node.kind === "string" ? { empty: bytes === 2 } : {}),
          ...(!["object", "array"].includes(node.kind) && bytes <= 4096
            ? { literal: doc.bytes.subarray(node.start, node.end).toString("utf8") }
            : {}),
        };
      }
      if (Buffer.byteLength(JSON.stringify(facts)) > 128 << 10)
        throw new PluginHostError("JSON_INSPECTION_BUDGET");
      return facts;
    },
    materialize(owner: string, plan: JSONPlan) {
      const doc = get(owner, plan.document);
      if (plan.edits.length > 32) throw new PluginHostError("JSON_PLAN_BUDGET");
      let replacementBytes = 0;
      const edits = plan.edits.map((edit) => {
        const parts = pointer(edit.pointer);
        if (
          !parts.length ||
          (edit.op !== "set" && edit.op !== "remove") ||
          (edit.op === "remove" && edit.value !== undefined) ||
          (edit.op === "set" && edit.value === undefined)
        )
          throw new PluginHostError("JSON_PLAN");
        const parent = lookup(doc.root, parts.slice(0, -1)),
          key = parts.at(-1)!;
        if (!parent || !["object", "array"].includes(parent.kind))
          throw new PluginHostError("JSON_PLAN");
        if (edit.op === "set") {
          const bytes = Buffer.from(edit.value!);
          replacementBytes += bytes.length;
          if (bytes.length > 4096 || replacementBytes > 64 << 10)
            throw new PluginHostError("JSON_PLAN_BUDGET");
          index(bytes, (amount) => {
            reserve(amount);
            doc.charge += amount;
          });
        }
        const existing = lookup(doc.root, parts);
        if ((parent.kind === "array" && !existing) || (edit.op === "remove" && !existing))
          throw new PluginHostError("JSON_PLAN");
        return { ...edit, parts, parent, key, existing };
      });
      for (let i = 0; i < edits.length; i++)
        for (let j = i + 1; j < edits.length; j++) {
          const a = edits[i]!.parts,
            b = edits[j]!.parts;
          if (a.slice(0, Math.min(a.length, b.length)).every((part, n) => part === b[n]))
            throw new PluginHostError("JSON_PLAN_OVERLAP");
        }
      const patches: { start: number; end: number; bytes: Buffer }[] = [];
      const insertions = new Map<Node, typeof edits>();
      for (const edit of edits) {
        if (!edit.existing) {
          const items = insertions.get(edit.parent) ?? [];
          items.push(edit);
          insertions.set(edit.parent, items);
          continue;
        }
        if (edit.op === "set") {
          patches.push({
            start: edit.existing.start,
            end: edit.existing.end,
            bytes: Buffer.from(edit.value!),
          });
          continue;
        }
        const siblings =
          edit.parent.kind === "array" ? edit.parent.items! : [...edit.parent.children!.values()];
        const position = siblings.indexOf(edit.existing);
        if (siblings.length === 1)
          patches.push({
            start: edit.parent.start + 1,
            end: edit.parent.end - 1,
            bytes: Buffer.alloc(0),
          });
        else if (position === 0)
          patches.push({
            start: edit.parent.start + 1,
            end: siblings[1]!.memberStart ?? siblings[1]!.start,
            bytes: Buffer.alloc(0),
          });
        else {
          const previous = siblings[position - 1]!;
          let start = previous.end;
          while (doc.bytes[start] !== 44 && start < edit.existing.start) start++;
          patches.push({ start, end: edit.existing.end, bytes: Buffer.alloc(0) });
        }
      }
      for (const [parent, items] of insertions) {
        if (parent.kind !== "object") throw new PluginHostError("JSON_PLAN");
        const bytes = Buffer.from(
          (parent.children!.size ? "," : "") +
            items.map((item) => `${JSON.stringify(item.key)}:${item.value}`).join(","),
        );
        patches.push({ start: parent.end - 1, end: parent.end - 1, bytes });
      }
      patches.sort((a, b) => a.start - b.start || a.end - b.end);
      for (let i = 1; i < patches.length; i++)
        if (patches[i]!.start < patches[i - 1]!.end || patches[i]!.start === patches[i - 1]!.start)
          throw new PluginHostError("JSON_PLAN_OVERLAP");
      const size = patches.reduce(
        (n, p) => n + p.bytes.length - (p.end - p.start),
        doc.bytes.length,
      );
      if (size > (16 << 20) + (64 << 10) || size > doc.bytes.length + (64 << 10))
        throw new PluginHostError("JSON_OUTPUT_BUDGET");
      const parts: Buffer[] = [];
      let cursor = 0;
      for (const patch of patches) {
        parts.push(doc.bytes.subarray(cursor, patch.start), patch.bytes);
        cursor = patch.end;
      }
      parts.push(doc.bytes.subarray(cursor));
      const bytes = new Uint8Array(Buffer.concat(parts, size));
      index(Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength), () => {});
      const digest = sha256(bytes),
        output = crypto.randomUUID();
      doc.consumed = true;
      drop(plan.document, doc);
      reserve(bytes.length);
      outputs.set(output, { owner, bytes });
      return {
        bytes,
        digest,
        release() {
          if (outputs.delete(output)) retained -= bytes.length;
        },
      };
    },
    discard(owner: string, handle: string) {
      const doc = documents.get(handle);
      if (doc && doc.owner === owner) drop(handle, doc);
    },
    stringValue(owner: string, handle: string, path: string) {
      const doc = get(owner, handle),
        node = lookup(doc.root, pointer(path));
      if (!node || node.kind !== "string") throw new PluginHostError("STRING_POINTER");
      try {
        return JSON.parse(doc.bytes.subarray(node.start, node.end).toString("utf8")) as string;
      } catch {
        throw new PluginHostError("STRING_POINTER");
      }
    },
    adjust(owner: string, delta: number) {
      const value = external.get(owner) ?? 0;
      if (!Number.isSafeInteger(delta) || value + delta < 0)
        throw new PluginHostError("JSON_MEMORY_ACCOUNTING");
      if (delta > 0) reserve(delta);
      else retained += delta;
      if (value + delta) external.set(owner, value + delta);
      else external.delete(owner);
    },
    dispose(owner: string) {
      for (const [handle, doc] of documents) if (doc.owner === owner) drop(handle, doc);
      for (const [id, output] of outputs)
        if (output.owner === owner) {
          retained -= output.bytes.length;
          outputs.delete(id);
        }
      retained -= external.get(owner) ?? 0;
      external.delete(owner);
    },
    snapshot: () => ({
      documents: documents.size,
      outputs: outputs.size,
      bytes: retained,
      peakBytes,
      maxBytes,
    }),
  };
}
