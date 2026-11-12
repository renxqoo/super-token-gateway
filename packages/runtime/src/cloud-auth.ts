import { createHmac, createPrivateKey, sign } from "node:crypto";
import { GatewayError, providerObject, text } from "@gateway/contracts";
import { sha256 } from "./crypto.ts";
import type { ProviderKind } from "@gateway/kernel";
import { readBounded, safeFetch } from "./network.ts";

export interface AwsCredentials {
  accessKeyId: string;
  secretAccessKey: string;
  region: string;
  sessionToken?: string;
}
const invalid = () =>
  new GatewayError(422, "invalid_cloud_credentials", "Cloud credential configuration is invalid");
export function cloudCredentials(secret: string): Record<string, unknown> {
  try {
    return providerObject(JSON.parse(secret));
  } catch {
    throw invalid();
  }
}
export function awsCredentials(secret: string): AwsCredentials {
  const value = cloudCredentials(secret);
  if (
    !/^[A-Za-z0-9]{1,128}$/.test(text(value.accessKeyId)) ||
    !/^[A-Za-z0-9/+=]{1,256}$/.test(text(value.secretAccessKey)) ||
    !/^[a-z]{2}(?:-[a-z]+)+-\d$/.test(text(value.region)) ||
    (value.sessionToken !== undefined && !/^[\x21-\x7e]{1,16384}$/.test(text(value.sessionToken)))
  )
    throw invalid();
  return {
    accessKeyId: text(value.accessKeyId),
    secretAccessKey: text(value.secretAccessKey),
    region: text(value.region),
    ...(value.sessionToken ? { sessionToken: text(value.sessionToken) } : {}),
  };
}
export function azureCredentials(secret: string): { key: string; version: string } {
  const value = secret.startsWith("{") ? cloudCredentials(secret) : { apiKey: secret };
  const key = text(value.apiKey);
  const version = text(value.apiVersion, "2024-10-21");
  if (!/^[\x21-\x7e]{1,4096}$/.test(key) || !/^\d{4}-\d{2}-\d{2}(?:-preview)?$/.test(version))
    throw invalid();
  return { key, version };
}
export function validateCloudCredentials(kind: ProviderKind, secret: string): void {
  if (kind === "azure") azureCredentials(secret);
  if (kind === "bedrock") awsCredentials(secret);
  if (kind === "vertex") {
    const value = cloudCredentials(secret);
    if (value.type === "service_account") {
      if (
        !/^[^\s@]+@[^\s@]+$/.test(text(value.client_email)) ||
        (value.token_uri !== undefined && value.token_uri !== "https://oauth2.googleapis.com/token")
      )
        throw invalid();
      try {
        if (createPrivateKey(text(value.private_key)).asymmetricKeyType !== "rsa") throw invalid();
      } catch {
        throw invalid();
      }
    } else if (
      !text(value.access_token) ||
      !Number.isFinite(Date.parse(text(value.expires_at))) ||
      Date.parse(text(value.expires_at)) <= Date.now()
    )
      throw invalid();
  }
}
const uriEncode = (value: string) =>
  encodeURIComponent(value).replace(
    /[!'()*]/g,
    (char) => `%${char.charCodeAt(0).toString(16).toUpperCase()}`,
  );
export function signBedrock(
  url: URL,
  body: string,
  headers: Headers,
  credentials: AwsCredentials,
  now = new Date(),
): void {
  const date = now.toISOString().replace(/[:-]|\.\d{3}/g, "");
  const day = date.slice(0, 8);
  headers.set("host", url.host);
  headers.set("x-amz-date", date);
  headers.set("x-amz-content-sha256", sha256(body));
  if (credentials.sessionToken) headers.set("x-amz-security-token", credentials.sessionToken);
  const names = [...headers.keys()]
    .filter((key) => key !== "authorization" && key !== "accept")
    .sort();
  const canonicalHeaders = names
    .map((key) => `${key}:${headers.get(key)!.trim().replace(/\s+/g, " ")}\n`)
    .join("");
  const query = [...url.searchParams]
    .map(([key, value]) => [uriEncode(key), uriEncode(value)])
    .sort((a, b) =>
      a[0]! < b[0]! ? -1 : a[0]! > b[0]! ? 1 : a[1]! < b[1]! ? -1 : a[1]! > b[1]! ? 1 : 0,
    )
    .map((pair) => pair.join("="))
    .join("&");
  const path = url.pathname
    .split("/")
    .map((part) => uriEncode(part))
    .join("/");
  const canonical = ["POST", path, query, canonicalHeaders, names.join(";"), sha256(body)].join(
    "\n",
  );
  const scope = `${day}/${credentials.region}/bedrock/aws4_request`;
  const toSign = `AWS4-HMAC-SHA256\n${date}\n${scope}\n${sha256(canonical)}`;
  const hmac = (key: string | Uint8Array, value: string) =>
    createHmac("sha256", key).update(value).digest();
  const signing = hmac(
    hmac(hmac(hmac(`AWS4${credentials.secretAccessKey}`, day), credentials.region), "bedrock"),
    "aws4_request",
  );
  headers.set(
    "authorization",
    `AWS4-HMAC-SHA256 Credential=${credentials.accessKeyId}/${scope}, SignedHeaders=${names.join(";")}, Signature=${createHmac("sha256", signing).update(toSign).digest("hex")}`,
  );
}
type TokenFetch = (url: URL, init: RequestInit) => Promise<Response>;
export class VertexTokens {
  private readonly entries = new Map<
    string,
    { token?: string; expires?: number; pending?: Promise<string> }
  >();
  constructor(
    private readonly fetchToken: TokenFetch = (url, init) => safeFetch(url, init, false),
    private readonly clock = Date.now,
  ) {}
  async get(secret: string, signal: AbortSignal): Promise<string> {
    validateCloudCredentials("vertex", secret);
    const value = cloudCredentials(secret);
    if (value.type !== "service_account") {
      if (Date.parse(text(value.expires_at)) <= this.clock() + 1000) throw invalid();
      return text(value.access_token);
    }
    const key = sha256(secret);
    let entry = this.entries.get(key);
    if (!entry) {
      if (this.entries.size >= 1000) {
        for (const [id, candidate] of this.entries)
          if (!candidate.pending) {
            this.entries.delete(id);
            break;
          }
        if (this.entries.size >= 1000)
          throw new GatewayError(
            503,
            "cloud_auth_overloaded",
            "Cloud authentication capacity reached",
          );
      }
      entry = {};
      this.entries.set(key, entry);
    }
    if (entry.token && entry.expires! > this.clock() + 60000) return entry.token;
    if (!entry.pending) {
      const target = entry;
      entry.pending = (async () => {
        const issued = Math.floor(this.clock() / 1000);
        const header = Buffer.from(JSON.stringify({ alg: "RS256", typ: "JWT" })).toString(
          "base64url",
        );
        const claims = Buffer.from(
          JSON.stringify({
            iss: value.client_email,
            scope: "https://www.googleapis.com/auth/cloud-platform",
            aud: "https://oauth2.googleapis.com/token",
            iat: issued,
            exp: issued + 3600,
          }),
        ).toString("base64url");
        const input = `${header}.${claims}`;
        const assertion = `${input}.${sign("RSA-SHA256", Buffer.from(input), text(value.private_key)).toString("base64url")}`;
        const timeout = AbortSignal.timeout(5000);
        const response = await this.fetchToken(new URL("https://oauth2.googleapis.com/token"), {
          method: "POST",
          headers: { "content-type": "application/x-www-form-urlencoded" },
          body: new URLSearchParams({
            grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
            assertion,
          }),
          signal: timeout,
        });
        const bytes = await readBounded(response.body, 16384, timeout);
        if (!response.ok)
          throw new GatewayError(503, "cloud_auth_failed", "Cloud authentication failed");
        const data = providerObject(JSON.parse(new TextDecoder().decode(bytes)));
        const expires = Number(data.expires_in);
        if (
          !text(data.access_token) ||
          data.token_type !== "Bearer" ||
          !Number.isSafeInteger(expires) ||
          expires <= 60 ||
          expires > 3600
        )
          throw invalid();
        target.token = text(data.access_token);
        target.expires = this.clock() + expires * 1000;
        return target.token;
      })().finally(() => {
        delete target.pending;
      });
    }
    signal.throwIfAborted();
    return new Promise<string>((resolve, reject) => {
      const abort = () => reject(signal.reason);
      signal.addEventListener("abort", abort, { once: true });
      entry!
        .pending!.then(resolve, reject)
        .finally(() => signal.removeEventListener("abort", abort));
    });
  }
}
