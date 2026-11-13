import { expect, test } from "bun:test";
import { openapi } from "../openapi.ts";

test("the published OpenAPI document matches the contract served by the gateway", async () => {
  const published = await Bun.file(
    new URL("../../../../docs/openapi.json", import.meta.url),
  ).json();
  expect(published).toEqual(JSON.parse(JSON.stringify(openapi())));
});
