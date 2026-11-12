import assert from "node:assert/strict";
import {
  withIdentity,
  registeredIdentity,
  currentCommand,
  identityRequest,
  registerIdentity,
} from "../tests/support/internal-identity.ts";
let checks = 0;
await withIdentity(
  async (f, db) => {
    const client = await f.connection(),
      identity = await registeredIdentity(client, f.a.port),
      other = await f.connection("gateway", f.b.port),
      command = currentCommand(identity);
    const current = await identityRequest(
      other,
      f.b.port,
      "/_internal/core/v1/identity/current-session",
      command.body,
      command.header,
    );
    assert.equal(current.status, 200);
    checks++;
    assert.equal(
      (current.value.receipt as { session_id: string }).session_id,
      identity.receipt.session_id,
    );
    checks++;
    const recovered = await registerIdentity(other, f.b.port, identity, identity.challenge);
    assert.equal(recovered.status, 200);
    checks++;
    assert.deepEqual(recovered.value.receipt, identity.receipt);
    checks++;
    await db`SELECT internal_identity_change_v1('gateway','gatewayA',NULL,${identity.receipt.session_id}::uuid,'SECURITY_REVOKE')`;
    const next = currentCommand(identity),
      rejected = await identityRequest(
        other,
        f.b.port,
        "/_internal/core/v1/identity/current-session",
        next.body,
        next.header,
      );
    assert.equal(rejected.status, 403);
    checks++;
    assert.equal(rejected.value.code, "internal_identity_revoked");
    checks++;
    assert.equal(
      (
        await db`SELECT count(*)::int AS n FROM internal_startup_sessions WHERE service='gateway'`
      )[0].n,
      1,
    );
    checks++;
  },
  { entry: "dist/core-api/identity-main.js" },
);
console.log(`Compiled Core identity artifact passed ${checks} checks`);
