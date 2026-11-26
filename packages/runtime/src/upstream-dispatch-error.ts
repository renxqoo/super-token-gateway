import { GatewayError } from "@gateway/contracts";

export class UpstreamPreDispatchError extends GatewayError {
  constructor(
    code:
      | "unsafe_upstream"
      | "upstream_dns"
      | "upstream_not_dispatched"
      | "upstream_capacity"
      | "upstream_tls"
      | "upstream_connect",
    message: string,
    cause?: unknown,
  ) {
    super(code === "upstream_capacity" ? 503 : 502, code, message);
    this.cause = cause;
  }
}
