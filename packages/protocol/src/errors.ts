import type { Operation } from "@gateway/kernel";
import { encodeSSE } from "./sse.ts";

function openaiErrorType(status: number, code: string): string {
  if (status === 401) return "authentication_error";
  if (status === 402 || status === 403) return "permission_error";
  if (status === 404) return "not_found_error";
  if (status === 429) return "rate_limit_error";
  if (code === "content_blocked") return "content_policy_violation";
  if (status >= 500) return "server_error";
  return "invalid_request_error";
}

function anthropicErrorType(status: number): string {
  if (status === 401) return "authentication_error";
  if (status === 402 || status === 403) return "permission_error";
  if (status === 404) return "not_found_error";
  if (status === 409) return "conflict_error";
  if (status === 413) return "request_too_large";
  if (status === 429) return "rate_limit_error";
  if (status === 503 || status === 529) return "overloaded_error";
  if (status >= 500) return "api_error";
  return "invalid_request_error";
}

function geminiGrpcStatus(status: number): string {
  if (status === 400 || status === 422) return "INVALID_ARGUMENT";
  if (status === 401) return "UNAUTHENTICATED";
  if (status === 402 || status === 403) return "PERMISSION_DENIED";
  if (status === 404) return "NOT_FOUND";
  if (status === 409) return "ABORTED";
  if (status === 413) return "FAILED_PRECONDITION";
  if (status === 429) return "RESOURCE_EXHAUSTED";
  if (status === 503 || status === 529) return "UNAVAILABLE";
  if (status === 504) return "DEADLINE_EXCEEDED";
  return "INTERNAL";
}

export function formatProtocolError(
  operation: Operation,
  status: number,
  code: string,
  message: string,
  requestId: string,
  extra?: Record<string, unknown>,
): Record<string, unknown> {
  if (operation === "messages") {
    return {
      type: "error",
      error: {
        type: anthropicErrorType(status),
        code,
        message,
        request_id: requestId,
        ...extra,
      },
    };
  }
  if (operation === "gemini") {
    return {
      error: {
        code: status,
        status: geminiGrpcStatus(status),
        gateway_code: code,
        message,
        request_id: requestId,
        ...extra,
      },
    };
  }
  return {
    error: {
      type: openaiErrorType(status, code),
      code,
      message,
      request_id: requestId,
      ...extra,
    },
  };
}

export function formatProtocolSseError(
  operation: Operation,
  status: number,
  code: string,
  message: string,
  requestId: string,
  extra?: Record<string, unknown>,
): Uint8Array {
  return encodeSSE(
    formatProtocolError(operation, status, code, message, requestId, extra),
    "error",
  );
}
