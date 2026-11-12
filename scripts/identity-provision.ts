import { SQL } from "bun";
import { X509Certificate } from "node:crypto";
import { parseArgs } from "node:util";
import { internalServiceSchema, internalInstanceSchema } from "@gateway/contracts";
import { sha256 } from "@gateway/runtime/crypto";
const { values } = parseArgs({
  options: {
    service: { type: "string" },
    instance: { type: "string" },
    certificate: { type: "string" },
    "uri-san": { type: "string" },
    hostname: { type: "string" },
  },
  strict: true,
});
const service = internalServiceSchema.parse(values.service),
  instance = internalInstanceSchema.parse(values.instance),
  uri = values["uri-san"];
if (
  !values.certificate ||
  !values.hostname ||
  !uri ||
  !/^spiffe:\/\/[A-Za-z0-9_./-]{1,200}$/.test(uri)
)
  throw new Error("Explicit certificate, hostname and URI SAN required");
const certificate = new X509Certificate(await Bun.file(values.certificate).text());
if (
  !certificate.checkHost(values.hostname) ||
  !certificate.subjectAltName?.split(", ").includes(`URI:${uri}`) ||
  Date.parse(certificate.validTo) <= Date.now() ||
  Date.parse(certificate.validFrom) > Date.now()
)
  throw new Error("Certificate identity mismatch");
const url = process.env.INTERNAL_IDENTITY_DEPLOY_DATABASE_URL;
if (!url) throw new Error("INTERNAL_IDENTITY_DEPLOY_DATABASE_URL required");
const db = new SQL(url, { max: 1, connection: { statement_timeout: 4000, lock_timeout: 1000 } });
try {
  await db`SELECT internal_identity_provision_v1(${service},${instance},${sha256(certificate.raw)},${uri},${certificate.subjectAltName},${new Date(certificate.validFrom).toISOString()}::timestamptz,${new Date(certificate.validTo).toISOString()}::timestamptz)`;
  console.log("Provisioned internal certificate");
} finally {
  await db.close();
}
