import { z } from "zod";
import { stat } from "node:fs/promises";
import { parseArgs } from "node:util";
import {
  installBundledReleaseSchema,
  bindPluginSchema,
} from "../apps/core-api/src/plugins/application.ts";
const { values } = parseArgs({
  options: {
    "admin-url": { type: "string" },
    "credential-file": { type: "string" },
    "command-id": { type: "string" },
    "release-id": { type: "string" },
    reason: { type: "string" },
    "provider-id": { type: "string" },
    "binding-file": { type: "string" },
  },
  strict: true,
});
if (!values["admin-url"] || !values["credential-file"])
  throw new Error("Required: --admin-url --credential-file --command-id --release-id --reason");
const url = new URL(values["admin-url"]);
if (
  url.protocol !== "https:" &&
  !(url.protocol === "http:" && ["127.0.0.1", "[::1]", "localhost"].includes(url.hostname))
)
  throw new Error("Admin URL requires HTTPS or a loopback listener");
if (
  url.username ||
  url.password ||
  url.search ||
  url.hash ||
  !["/admin", "/admin/"].includes(url.pathname)
)
  throw new Error("Admin URL must end in /admin");
if (Boolean(values["provider-id"]) !== Boolean(values["binding-file"]))
  throw new Error("--provider-id and --binding-file must be supplied together");
const provider = values["provider-id"] ? z.uuid().parse(values["provider-id"]) : undefined;
const bindingInput = values["binding-file"]
  ? bindPluginSchema
      .omit({ version_id: true })
      .parse(JSON.parse(await Bun.file(values["binding-file"]).text()))
  : undefined;
const credentialPermissions = await stat(values["credential-file"]);
if (!credentialPermissions.isFile() || (credentialPermissions.mode & 0o077) !== 0)
  throw new Error("Credential must be a private regular file with mode 0600");
const credential = (await Bun.file(values["credential-file"]).text()).trim();
if (!credential || /\s/.test(credential)) throw new Error("Invalid credential file");
async function command(path: string, body: unknown, method = "POST") {
  const response = await fetch(new URL(url.toString().replace(/\/$/, "") + path), {
    method,
    headers: { authorization: `Bearer ${credential}`, "content-type": "application/json" },
    body: JSON.stringify(body),
    redirect: "error",
    signal: AbortSignal.timeout(90000),
  });
  const result = (await response.json()) as Record<string, unknown>;
  if (!response.ok)
    throw new Error(`Catalog command failed (${response.status}): ${JSON.stringify(result)}`);
  return result;
}
const input = installBundledReleaseSchema.parse({
  command_id: values["command-id"],
  release_id: values["release-id"],
  reason: values.reason,
});
const installed = await command("/plugins/install-bundled-release", input);
const binding = bindingInput
  ? await command(
      `/providers/${provider}/plugin-binding`,
      { ...bindingInput, version_id: installed.version_id },
      "PUT",
    )
  : undefined;
console.log(JSON.stringify({ publication: installed, ...(binding ? { binding } : {}) }));
