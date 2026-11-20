import { GatewayError } from "@gateway/contracts";

const signedGroupBytes = 16 << 10;

export function createTerminalBudget(maxGroups: number, maxBytes = 64 << 20) {
  let groups = 0;
  let bytes = 0;
  function tryReserve() {
    if (groups >= maxGroups || bytes + signedGroupBytes > maxBytes) return undefined;
    groups++;
    bytes += signedGroupBytes;
    let held = signedGroupBytes;
    let retained = false;
    return {
      retainOutput(size: number) {
        if (!held || retained || !Number.isSafeInteger(size) || size < 0 || bytes + size > maxBytes)
          return false;
        retained = true;
        bytes += size;
        held += size;
        return true;
      },
      release() {
        if (!held) return;
        groups--;
        bytes -= held;
        held = 0;
      },
    };
  }
  return {
    tryReserve,
    reserve() {
      const reservation = tryReserve();
      if (!reservation)
        throw new GatewayError(503, "overloaded", "Terminal evidence capacity exhausted");
      return reservation;
    },
  };
}
