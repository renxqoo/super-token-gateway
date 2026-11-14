import manifest from "./package/manifest.json";
import { runMessages } from "./messages.ts";

export function invoke() {
  runMessages(manifest, "anthropic");
}
