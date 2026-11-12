import { expect, test } from "bun:test";
import { withFinancial, prepareFinancialAdmission } from "../support/financial-execution.ts";
test(
  "a valid signed plugin configuration with supplementary Unicode map keys keeps SDK prep binding digests compatible with Core",
  () =>
    withFinancial(async (f) => {
      const setup = await f.bootstrap();
      await f.catalog.application.bind(await f.catalog.actor(), setup.provider.id, {
        ...f.catalog.binding(setup.publication.version_id),
        expected_revision: "1",
        account_id: setup.provider.account_id,
        brand_id: setup.provider.brand_id,
        secret_reference: `channel:${setup.provider.id}`,
        config: {
          model_map: {
            [setup.model]: "synthetic",
            "\ue000": "unused-bmp",
            "😀": "unused-supplementary",
          },
          max_output_tokens: 100,
          max_input_tokens: 10000,
          auth: "bearer",
        },
      });
      const client = f.client();
      await client.startup();
      const prepared = await prepareFinancialAdmission(setup, client);
      expect((await prepared.admit()).receipt).toMatchObject({
        command_state: "COMMITTED",
        state: "RESERVED",
      });
    }),
  30000,
);
