import { expect, test } from "bun:test";
import { getBuiltinProvider } from "@gateway/plugin-engine";
import { createKey } from "@gateway/runtime";
import { parseMultipartFormData } from "../inference/multipart.ts";
import type { ProxyDependencies } from "../inference/types.ts";
import { handleInferenceRequest } from "../proxy.ts";
import {
  makeApiKey,
  makeBillingAccount,
  makeConfig,
  makeFakeDeps,
  makeSnapshot,
} from "./proxy-fixture.ts";

function inspectRawMultipartParts(body: Uint8Array, contentType: string) {
  const match = /boundary=(?:"([^"]+)"|([^;\s]+))/i.exec(contentType);
  if (!match) throw new Error(`Missing boundary in ${contentType}`);
  const boundary = match[1] ?? match[2]!;
  const buf = Buffer.from(body.buffer, body.byteOffset, body.byteLength);
  const delim = Buffer.from(`--${boundary}`);
  const parts: Array<{ name: string; filename?: string; contentType?: string; bytes: Uint8Array }> =
    [];
  let cursor = buf.indexOf(delim);
  while (cursor !== -1) {
    cursor += delim.length;
    if (buf[cursor] === 45 && buf[cursor + 1] === 45) break;
    if (buf[cursor] === 13 && buf[cursor + 1] === 10) cursor += 2;
    const headerEnd = buf.indexOf("\r\n\r\n", cursor);
    if (headerEnd === -1) throw new Error("Missing part header terminator");
    const headerText = buf.subarray(cursor, headerEnd).toString("utf8");
    const bodyStart = headerEnd + 4;
    const nextDelim = buf.indexOf(Buffer.from(`\r\n--${boundary}`), bodyStart);
    if (nextDelim === -1) throw new Error("Missing next boundary");
    const partBytes = new Uint8Array(buf.subarray(bodyStart, nextDelim));
    let name = "";
    let filename: string | undefined;
    let partContentType: string | undefined;
    for (const line of headerText.split("\r\n")) {
      const colon = line.indexOf(":");
      if (colon === -1) continue;
      const key = line.slice(0, colon).trim().toLowerCase();
      const value = line.slice(colon + 1).trim();
      if (key === "content-disposition") {
        const nameMatch = /\bname="([^"]*)"/.exec(value);
        if (nameMatch) name = nameMatch[1]!;
        const fileMatch = /\bfilename="([^"]*)"/.exec(value);
        if (fileMatch) filename = fileMatch[1]!;
      } else if (key === "content-type") {
        partContentType = value;
      }
    }
    parts.push({
      name,
      ...(filename !== undefined ? { filename } : {}),
      ...(partContentType !== undefined ? { contentType: partContentType } : {}),
      bytes: partBytes,
    });
    cursor = nextDelim + 2;
  }
  return parts;
}

test("parseMultipartFormData preserves exact MIME, quoted boundary, UTF-8 filename*, binary bytes, and rejects malformed payloads", async () => {
  const binary = new Uint8Array([0x00, 0x0d, 0x0a, 0xff, 0x80, 0x2d, 0x2d]);
  const raw = Buffer.concat([
    Buffer.from(
      'preamble ignored\r\n--b-123\r\nContent-Disposition: form-data; name="field"\r\n\r\nhello\r\n' +
        '--b-123\r\nContent-Disposition: form-data; name="file"; filename="raw"; filename*=UTF-8\'\'%E9%9F%B3%E9%A2%91\r\nContent-Type: audio/wav\r\n\r\n',
    ),
    binary,
    Buffer.from("\r\n--b-123--\r\n"),
  ]);
  const entries = parseMultipartFormData(raw, 'multipart/form-data; boundary="b-123"');
  expect(entries).toHaveLength(2);
  expect(entries[0]).toEqual(["field", "hello"]);
  const file = entries[1]![1];
  expect(file instanceof File).toBe(true);
  if (file instanceof File) {
    expect(file.name).toBe("音频");
    expect(file.type).toBe("audio/wav");
    expect(new Uint8Array(await file.arrayBuffer())).toEqual(binary);
  }

  for (const [payload, ct] of [
    [new TextEncoder().encode("plain"), "multipart/form-data"],
    [new TextEncoder().encode("--b\r\n"), "multipart/form-data; boundary=b"],
    [
      new TextEncoder().encode("--b\r\nContent-Type: text/plain\r\n\r\nno-disp\r\n--b--\r\n"),
      "multipart/form-data; boundary=b",
    ],
    [
      new TextEncoder().encode(
        '--b\r\nContent-Disposition: form-data; name="a"\r\n\r\nunterminated',
      ),
      "multipart/form-data; boundary=b",
    ],
  ] as const) {
    expect(() => parseMultipartFormData(payload, ct)).toThrow(
      expect.objectContaining({ status: 400, code: "invalid_request" }),
    );
  }
});

test("symptom: multipart forwarding drops extensionless file Content-Type and rewrites audio/wav to audio/x-wav", async () => {
  const proj = makeBillingAccount("11111111-1111-4111-8111-111111111153", 10);
  const rawKey = createKey();
  const snap = makeSnapshot(1, [proj], [makeApiKey(rawKey, proj.id)]);
  const preset = getBuiltinProvider("openai")!;
  const pluginId = "88888888-8888-4888-8888-888888888853";
  snap.plugins.set(pluginId, {
    id: pluginId,
    name: preset.name,
    version: preset.version,
    capability: "PROVIDER",
    kind: "DECLARATIVE",
    digest: preset.digest,
    wasm_bytes: null,
    webhook_config: null,
    provider_config: preset.provider_config,
    signature: null,
    signing_key_id: null,
    manifest: {},
    enabled: true,
    builtin: true,
  });

  const capturedWireParts: Array<ReturnType<typeof inspectRawMultipartParts>> = [];
  const upstream = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(req) {
      const raw = new Uint8Array(await req.arrayBuffer());
      capturedWireParts.push(inspectRawMultipartParts(raw, req.headers.get("content-type") ?? ""));
      return Response.json({ text: "ok", usage: { type: "duration", seconds: 1 } });
    },
  });

  const acctId = "44444444-4444-4444-8444-444444444453";
  const chId = "33333333-3333-4333-8333-333333333353";
  const custPriceId = "66666666-6666-4666-8666-666666666653";
  const supPriceId = "77777777-7777-4777-8777-777777777753";
  snap.accounts.set(acctId, { id: acctId, name: "acct-mime", state: "ACTIVE", revision: "1" });
  snap.channels.set(chId, {
    id: chId,
    account_id: acctId,
    name: "ch-mime",
    kind: "openai",
    provider_plugin_id: pluginId,
    base_url: `http://127.0.0.1:${upstream.port}`,
    secret_ciphertext: "",
    adapter_config: {},
    rpm: 10_000,
    concurrency: 100,
    enabled: true,
    routing_policy: {},
    config_revision: 1,
  });
  for (const [id, side] of [
    [custPriceId, "CUSTOMER"],
    [supPriceId, "SUPPLIER"],
  ] as const) {
    snap.prices.set(id, {
      id,
      side,
      currency: "USD",
      basis: "PROVIDER_USAGE",
      rates: { audio_seconds: { numerator: "10", denominator: "1" } },
      ceilings: { audio_seconds: "60" },
      source_digest: "d-mime",
    });
  }
  snap.models.set("whisper-mime", {
    name: "whisper-mime",
    operations: ["transcription"],
    routing_strategy: "WEIGHTED",
    max_output_tokens: 100,
    enabled: true,
  });
  snap.routesByModel.set("whisper-mime", [
    {
      id: "r-mime",
      model_name: "whisper-mime",
      channel_id: chId,
      upstream_model: "whisper-mime-up",
      operations: ["transcription"],
      billing_price_id: custPriceId,
      cost_version_id: supPriceId,
      priority: 10,
      weight: 100,
      enabled: true,
      retry_policy: { max_attempts: 1 },
      request_policy: {},
      plugin_pipeline: [],
      adapter_overrides: {},
    },
  ]);

  const deps = makeFakeDeps(snap, makeConfig());
  deps.ledger = {
    async acquireHold() {
      return { ok: true, replayed: false, request_id: "req", held_atoms: "1000" };
    },
    async commitTerminal() {
      return {
        ok: true,
        replayed: false,
        charge_id: "chg",
        customer_atoms: "10",
        supplier_cost_atoms: "10",
      };
    },
    async releaseHold() {},
    async markUnknown() {},
  } as unknown as ProxyDependencies["ledger"];

  const binaryAudio = new Uint8Array([0x52, 0x49, 0x46, 0x46, 0x00, 0xff, 0x80, 0xfe]);
  const uploadCases = [
    { filename: "recording", mime: "audio/wav" },
    { filename: "clip.wav", mime: "audio/wav" },
    { filename: "stream", mime: "audio/wav; rate=16000" },
  ] as const;

  try {
    for (const item of uploadCases) {
      const form = new FormData();
      form.append("model", "whisper-mime");
      form.append("file", new File([binaryAudio], item.filename, { type: item.mime }));
      const resp = await handleInferenceRequest(
        {
          req: {
            raw: new Request("http://127.0.0.1/v1/audio/transcriptions", {
              method: "POST",
              headers: { authorization: `Bearer ${rawKey}` },
              body: form,
            }),
            query: () => undefined,
          },
          json(data, status, extra) {
            return new Response(JSON.stringify(data), {
              status: status ?? 200,
              headers: { "content-type": "application/json", ...extra },
            });
          },
        },
        deps,
        "transcription",
      );
      expect(resp.status).toBe(200);
    }

    expect(capturedWireParts).toHaveLength(uploadCases.length);
    for (let i = 0; i < uploadCases.length; i++) {
      const parts = capturedWireParts[i]!;
      const modelPart = parts.find((p) => p.name === "model");
      expect(new TextDecoder().decode(modelPart?.bytes)).toBe("whisper-mime-up");
      const filePart = parts.find((p) => p.name === "file");
      expect(filePart?.filename).toBe(uploadCases[i]!.filename);
      expect(filePart?.contentType).toBe(uploadCases[i]!.mime);
      expect(filePart?.bytes).toEqual(binaryAudio);
    }
  } finally {
    deps.state?.close();
    deps.transport.close();
    await upstream.stop(true);
  }
});
