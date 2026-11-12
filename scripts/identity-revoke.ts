import { SQL } from "bun";
import { parseArgs } from "node:util";
import { z } from "zod";
import {
  internalServiceSchema,
  internalInstanceSchema,
  internalUuidSchema,
  digestSchema,
} from "@gateway/contracts";
const { values } = parseArgs({
  options: {
    service: { type: "string" },
    instance: { type: "string" },
    "certificate-sha256": { type: "string" },
    session: { type: "string" },
    action: { type: "string" },
  },
  strict: true,
});
const service = internalServiceSchema.parse(values.service),
  instance = internalInstanceSchema.parse(values.instance),
  hash =
    values["certificate-sha256"] === undefined
      ? null
      : digestSchema.parse(values["certificate-sha256"]),
  session = values.session === undefined ? null : internalUuidSchema.parse(values.session),
  action = z.enum(["DISABLE", "RETIRE", "SECURITY_REVOKE"]).parse(values.action);
const url = process.env.INTERNAL_IDENTITY_DEPLOY_DATABASE_URL;
if (!url) throw new Error("INTERNAL_IDENTITY_DEPLOY_DATABASE_URL required");
const db = new SQL(url, { max: 1, connection: { statement_timeout: 4000, lock_timeout: 1000 } });
try {
  await db`SELECT internal_identity_change_v1(${service},${instance},${hash},${session}::uuid,${action})`;
  console.log("Changed internal identity authorization");
} finally {
  await db.close();
}
