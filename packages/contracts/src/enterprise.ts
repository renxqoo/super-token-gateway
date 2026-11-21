import { z } from "zod";

export const digestSchema = z.string().regex(/^[a-f0-9]{64}$/);
export const opaqueResourceIdSchema = z
  .string()
  .regex(/^[A-Za-z0-9_:/.-]{1,500}$/)
  .refine(
    (value) =>
      !value.includes("://") && !value.split("/").some((part) => part === "." || part === ".."),
    "Expected a stable opaque resource identity",
  );

export { upstreamCostConfigurationSchema, type UpstreamCostConfiguration } from "./public.ts";
