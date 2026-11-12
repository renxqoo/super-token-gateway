import { GatewayError } from "@gateway/contracts";
export function errorResponse(error: unknown, requestId: string): Response {
  const known = error instanceof GatewayError;
  return Response.json(
    {
      error: {
        type: known ? error.code : "internal_error",
        code: known ? error.code : "internal_error",
        message: known ? error.message : "The gateway could not complete this request",
      },
      request_id: requestId,
    },
    { status: known ? error.status : 500, headers: { "x-request-id": requestId } },
  );
}
