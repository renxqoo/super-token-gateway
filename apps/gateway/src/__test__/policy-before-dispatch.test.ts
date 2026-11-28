import { expect, test } from "bun:test";
import { GatewayError } from "@gateway/contracts";
import { invokeInference, jsonBody } from "./proxy-response-harness.ts";

test("a policy change after ingress prevents dispatch and releases the hold", async () => {
  let checks = 0;
  const result = await invokeInference(jsonBody, {
    checkConsoleAdmission() {
      if (++checks === 1) return Promise.resolve();
      return Promise.reject(new GatewayError(503, "config_unavailable", "API key policy changed"));
    },
  });
  expect(result.status).toBe(503);
  expect(JSON.parse(result.wire)).toMatchObject({ error: { code: "config_unavailable" } });
  expect(result.dispatches).toBe(0);
  expect(result.finalized).toEqual(["RELEASED"]);
});
