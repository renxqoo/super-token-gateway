import { GatewayError } from "@gateway/contracts";

export type Reference = { pointer: string; resourceId: string };

const unmanagedKeys = new Set([
  "conversation",
  "container_id",
  "vector_store_ids",
  "vector_store_id",
  "thread_id",
  "assistant_id",
  "cachedContent",
  "cached_content",
  "fileData",
  "file_id",
  "file_ids",
  "input_file_id",
]);
const opaqueKeys = new Set(["parameters", "input_schema", "schema", "properties"]);

function opaque(key: string, node: Record<string, unknown>, parent: string) {
  return (
    opaqueKeys.has(key) ||
    (key === "input" && node.type === "tool_use") ||
    (key === "args" && parent === "functionCall") ||
    (key === "response" && parent === "functionResponse")
  );
}

function unmanaged(key: string, entry: unknown) {
  return (
    (unmanagedKeys.has(key) && entry !== null && entry !== undefined) ||
    (key === "container" && typeof entry === "string") ||
    (key === "prompt" &&
      typeof entry === "object" &&
      entry !== null &&
      typeof (entry as Record<string, unknown>).id === "string") ||
    (key === "file_data" && typeof entry === "object" && entry !== null)
  );
}

function refused(): never {
  throw new GatewayError(
    422,
    "unmanaged_resource",
    "Stored provider resources must be accessed through gateway-owned identities",
  );
}

function escape(key: string) {
  return key.replaceAll("~", "~0").replaceAll("/", "~1");
}

export function collectReferences(value: unknown, resolvable: boolean): Reference[] {
  const references: Reference[] = [];
  const pending: [unknown, string, string][] = [[value, "", ""]];
  while (pending.length) {
    const [node, parent, path] = pending.pop()!;
    if (Array.isArray(node)) {
      node.forEach((entry, index) => pending.push([entry, parent, `${path}/${index}`]));
      continue;
    }
    if (typeof node !== "object" || node === null) continue;
    const record = node as Record<string, unknown>;
    for (const [key, entry] of Object.entries(record)) {
      if (opaque(key, record, parent)) continue;
      const pointer = `${path}/${escape(key)}`;
      if (key === "previous_response_id" && entry !== null && entry !== undefined) {
        if (
          !resolvable ||
          path !== "" ||
          typeof entry !== "string" ||
          !/^response_[0-9a-f]{32}$/.test(entry)
        )
          refused();
        references.push({ pointer, resourceId: entry });
        continue;
      }
      if (unmanaged(key, entry)) refused();
      pending.push([entry, key, pointer]);
    }
  }
  return references;
}
