import { expect, test } from "bun:test";
import { CoreRpcError } from "@gateway/core-client";
import { executeChat, type ChatDependencies } from "../chat.ts";

for (const event of ["disconnect", "drain", "local rejection"] as const) {
  test(
    event === "local rejection"
      ? "a locally rejected retry preserves the earlier uncertain Core outcome"
      : `a ${event} during Core backoff stops the next context command`,
    async () => {
      const cancel = new AbortController();
      const drain = new AbortController();
      const busy = new CoreRpcError(
        {
          code: "identity_busy",
          retryability: "SAME_COMMAND",
          command_state: "UNKNOWN",
          category: "DEPENDENCY",
          message: "busy",
        },
        503,
      );
      const entered = Promise.withResolvers<void>();
      const commands: string[] = [];
      let released = false;
      const deps = {
        config: { total_timeout_ms: 5000 },
        host: {
          reservePreparation: () => () => {
            released = true;
          },
        },
        core: {
          async execution(kind: string) {
            commands.push(kind);
            entered.resolve();
            if (commands.length === 1) throw busy;
            if (event === "local rejection")
              throw new CoreRpcError({
                code: "internal_capacity",
                retryability: "SAME_COMMAND",
                command_state: "NOT_ATTEMPTED",
                category: "DEPENDENCY",
                message: "queue full",
              });
            throw new Error("Unexpected retry");
          },
        },
      } as unknown as ChatDependencies;
      const result = executeChat(deps, {
        requestId: crypto.randomUUID(),
        operation: "chat",
        token: "synthetic",
        body: new TextEncoder().encode(JSON.stringify({ model: "synthetic", stream: true })),
        ip: "127.0.0.1",
        signal: cancel.signal,
        drain: drain.signal,
        idempotencyKey: undefined,
        contentType: "application/json",
      });
      await entered.promise;
      if (event === "local rejection") {
        await expect(result).rejects.toMatchObject({ status: 503, code: "core_unavailable" });
        expect(commands).toEqual(["execution_context_v1", "execution_context_v1"]);
      } else {
        await Bun.sleep(1);
        (event === "drain" ? drain : cancel).abort();
        await expect(result).rejects.toMatchObject(
          event === "drain"
            ? { status: 503, code: "draining" }
            : { status: 499, code: "cancelled" },
        );
        expect(commands).toEqual(["execution_context_v1"]);
      }
      expect(released).toBe(true);
    },
  );
}
