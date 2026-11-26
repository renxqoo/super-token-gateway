const SAFE_PATH_RE = /^[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+){0,7}$/;

const FORBIDDEN_SEGMENTS = new Set(["__proto__", "constructor", "prototype"]);

const PROTECTED_HEADER_NAMES = new Set([
  "host",
  "content-length",
  "transfer-encoding",
  "connection",
  "authorization",
  "x-api-key",
  "api-key",
  "x-goog-api-key",
]);

const ALLOWED_TEMPLATE_VARS = new Set(["model", "encoded_model", "operation", "stream_action"]);

export function assertSafeFieldPath(path: string): string[] {
  if (!SAFE_PATH_RE.test(path)) {
    throw new Error(`Invalid field path: ${path}`);
  }
  const parts = path.split(".");
  for (const seg of parts) {
    if (FORBIDDEN_SEGMENTS.has(seg)) {
      throw new Error(`Forbidden field path segment: ${seg}`);
    }
  }
  return parts;
}

export function assertSafeRelativePath(path: string): string {
  if (
    !path.startsWith("/") ||
    path.includes("://") ||
    path.includes("..") ||
    path.includes("\r") ||
    path.includes("\n") ||
    path.includes("\0") ||
    path.startsWith("//")
  ) {
    throw new Error(`Unsafe provider relative path: ${path}`);
  }
  return path;
}

export function assertSafeEndpointTemplate(tpl: string): void {
  assertSafeRelativePath(tpl.replace(/\$\{[a-z_]+\}/g, "x"));
  for (const match of tpl.matchAll(/\$\{([^}]+)\}/g)) {
    if (!ALLOWED_TEMPLATE_VARS.has(match[1] ?? "")) {
      throw new Error(`Unsupported endpoint template variable: ${match[1]}`);
    }
  }
}

export function sanitizeProviderHeaders(
  headers: Record<string, string> | undefined,
): Record<string, string> {
  const out: Record<string, string> = {};
  if (!headers) return out;
  for (const [k, v] of Object.entries(headers)) {
    const lower = k.toLowerCase().trim();
    if (!lower || PROTECTED_HEADER_NAMES.has(lower)) continue;
    if (lower.includes("\r") || lower.includes("\n") || v.includes("\r") || v.includes("\n")) {
      continue;
    }
    out[k] = v;
  }
  return out;
}
