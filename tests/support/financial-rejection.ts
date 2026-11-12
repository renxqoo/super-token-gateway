import { expect } from "bun:test";
import { CoreRpcError } from "@gateway/core-client";
import { executionReceiptSchema, type ExecutionReply } from "@gateway/contracts";
export async function financialRejection(
  pending: Promise<unknown>,
  status: number,
): Promise<ExecutionReply> {
  const error: unknown = await pending.catch((error: unknown) => error);
  expect(error).toBeInstanceOf(CoreRpcError);
  if (!(error instanceof CoreRpcError)) throw new Error("Expected typed financial rejection");
  expect(error.status).toBe(status);
  expect(error.detail.command_state).toBe("REJECTED");
  expect(error.detail.retryability).toBe("NONE");
  const { code, request_id, attempt_id } = error.detail;
  return {
    receipt: executionReceiptSchema.parse({
      command_state: "REJECTED",
      code,
      ...(request_id ? { request_id } : {}),
      ...(attempt_id ? { attempt_id } : {}),
    }),
  };
}
