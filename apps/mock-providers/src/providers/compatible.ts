import type { Hono } from "hono";
import { obj } from "./http.ts";

export type CompatibleVariant = "deepseek" | "minimax";

type ChatFixture = {
  path: string;
  tokenField?: "max_tokens";
  variant?: CompatibleVariant;
  optionalBearer?: boolean;
};

const fixtures: readonly ChatFixture[] = [
  { path: "/openai-compatible/v1/chat/completions" },
  { path: "/deepseek/v1/chat/completions", tokenField: "max_tokens", variant: "deepseek" },
  { path: "/ollama/v1/chat/completions", tokenField: "max_tokens", optionalBearer: true },
  { path: "/qwen/compatible-mode/v1/chat/completions", tokenField: "max_tokens" },
  { path: "/volcengine/api/v3/chat/completions", tokenField: "max_tokens" },
  { path: "/zhipu/api/paas/v4/chat/completions", tokenField: "max_tokens" },
  { path: "/moonshot/v1/chat/completions", tokenField: "max_tokens" },
  { path: "/minimax/v1/text/chatcompletion_v2", tokenField: "max_tokens", variant: "minimax" },
  { path: "/openrouter/api/v1/chat/completions" },
];

export function registerCompatibleChat(
  app: Hono,
  generate: (request: Request, variant?: CompatibleVariant) => Promise<Response>,
): void {
  for (const fixture of fixtures) {
    app.post(fixture.path, async (c) => {
      if (!fixture.optionalBearer && !/^Bearer \S+$/.test(c.req.header("authorization") ?? "")) {
        return c.json({ error: { message: "Expected bearer authentication" } }, 401);
      }
      const body = obj(await c.req.raw.clone().json());
      if (
        typeof body.model !== "string" ||
        !Array.isArray(body.messages) ||
        !body.messages.length
      ) {
        return c.json({ error: { message: "Expected model and messages" } }, 400);
      }
      if (
        fixture.tokenField &&
        (typeof body[fixture.tokenField] !== "number" || body.max_completion_tokens !== undefined)
      ) {
        return c.json({ error: { message: "Expected max_tokens" } }, 400);
      }
      return generate(c.req.raw, fixture.variant);
    });
  }
}
