import manifest from "./package/manifest.json";
import { runMessages } from "../anthropic-messages/messages.ts";

export function invoke() {
  runMessages(manifest, "bedrock");
}
