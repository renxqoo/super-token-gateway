import { expect, test } from "bun:test";
import { createCoreRpcClient } from "../index.ts";
test("unregistered or closed RPC clients cannot issue workload commands", async () => {
  const client = createCoreRpcClient({
    origin: "https://127.0.0.1:1",
    servername: "localhost",
    key: "",
    cert: "",
    ca: "",
    audience: "test/core-api",
    expectedPeerSha256: "a".repeat(64),
    expectedSubjectAltname: "URI:spiffe://test/core-api/A",
  });
  await expect(client.current()).rejects.toThrow("Startup required");
  await client.close();
  await expect(client.startup()).rejects.toThrow("Client closed");
});
test("Core RPC refuses to dial without an exact provisioned Core peer pin and SAN", () => {
  const configuration = {
    origin: "https://127.0.0.1:1",
    servername: "localhost",
    key: "",
    cert: "",
    ca: "",
    audience: "test/core-api",
  };
  expect(() => Reflect.apply(createCoreRpcClient, undefined, [configuration])).toThrow(
    "Core target pin required",
  );
});
