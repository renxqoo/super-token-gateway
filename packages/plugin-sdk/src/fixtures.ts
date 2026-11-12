import { z } from "zod";
import { base64Schema, hooks } from "./abi.ts";
const expected = z
  .record(z.string(), z.unknown())
  .refine((value) => typeof value.kind === "string" && Object.keys(value).length > 1);
export const fixtureSuiteSchema = z
  .object({
    format: z.literal(1),
    config: z.record(z.string(), z.unknown()),
    cases: z
      .array(
        z
          .object({
            name: z.string().min(1).max(200),
            steps: z
              .array(
                z.discriminatedUnion("action", [
                  z
                    .object({
                      action: z.literal("invoke"),
                      hook: z.enum(hooks),
                      input: base64Schema,
                      expected,
                    })
                    .strict(),
                  z
                    .object({
                      action: z.literal("json"),
                      hook: z.enum(["prepare", "response", "error"]),
                      metadata: z.record(z.string(), z.unknown()),
                      body: base64Schema,
                      expected,
                      expected_body: base64Schema.optional(),
                    })
                    .strict(),
                  z
                    .object({
                      action: z.literal("cancel"),
                      hook: z.enum(hooks),
                      input: base64Schema,
                      expected_error: z.literal("CALL_CANCELLED"),
                    })
                    .strict(),
                ]),
              )
              .min(1)
              .max(1000),
          })
          .strict(),
      )
      .min(1)
      .max(1000),
  })
  .strict();
export type FixtureSuite = z.infer<typeof fixtureSuiteSchema>;
