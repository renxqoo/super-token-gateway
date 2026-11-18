import { exactParse, exactStringify } from "@gateway/plugin-sdk/guest";
import { invokeBuiltin } from "./builtin-host.ts";

export function startBuiltinWorker(modules: Readonly<Record<string, () => void>>) {
  if (Bun.version !== "1.4.4") throw new Error("BUILTIN_RUNTIME");
  const decoder = new TextDecoder("utf-8", { fatal: true });
  self.onmessage = ({ data }) => {
    const id = data?.id;
    try {
      if (
        data.version !== 1 ||
        typeof id !== "string" ||
        !/^[0-9]{1,32}$/.test(id) ||
        !Object.hasOwn(modules, data.module) ||
        !(data.input instanceof Uint8Array) ||
        data.input.length > 256 << 10 ||
        (data.state !== undefined &&
          (!(data.state instanceof Uint8Array) || data.state.length > 256 << 10))
      )
        throw new Error("BUILTIN_INPUT");
      const wrapper = `{"input":${JSON.stringify(Buffer.from(data.input).toString("base64"))},"state":${data.state?.length ? decoder.decode(data.state) : "null"}}`;
      const encoded = invokeBuiltin(wrapper, modules[data.module]!);
      const decoded = exactParse(encoded) as { output: unknown; state: unknown };
      if (typeof decoded?.output !== "string" || !Object.hasOwn(decoded, "state"))
        throw new Error("BUILTIN_OUTPUT");
      const output = new Uint8Array(Buffer.from(decoded.output, "base64"));
      if (Buffer.from(output).toString("base64") !== decoded.output)
        throw new Error("BUILTIN_OUTPUT");
      const state = new TextEncoder().encode(exactStringify(decoded.state));
      if (output.length > 256 << 10 || state.length > 256 << 10) {
        self.postMessage({ version: 1, id, error: "OUTPUT_BUDGET" });
        return;
      }
      self.postMessage({ version: 1, id, output, state }, [output.buffer, state.buffer]);
    } catch {
      self.postMessage({ version: 1, id, error: "GUEST_ERROR" });
    }
  };
  self.postMessage({ version: 1, event: "ready" });
}
