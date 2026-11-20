import { GatewayError } from "@gateway/contracts";
import { Batcher } from "./batcher.ts";
import { Limits } from "./limits.ts";

type Item = { keys: string[]; action: string; payload: string; deadline: number };

function unavailable(message: string, cause?: Error) {
  return new GatewayError(503, "rate_limit_unavailable", message, cause ? { cause } : undefined);
}

function timeout() {
  return unavailable("Distributed admission deadline reached");
}

function parseReply(reply: unknown): string | GatewayError {
  if (Array.isArray(reply) && reply.length === 2 && typeof reply[1] === "string") {
    if (reply[0] === 1) return reply[1];
    if (reply[0] === 0) {
      if (reply[1] === "QUOTA_LIMIT" || reply[1] === "ERR QUOTA_LIMIT")
        return new GatewayError(
          429,
          "rate_limited",
          "Request rate, token budget or concurrency limit reached",
        );
      const message = reply[1].startsWith("ERR ") ? reply[1] : `ERR ${reply[1]}`;
      return unavailable("Distributed admission is unavailable", new Error(message));
    }
  }
  return unavailable("Invalid distributed admission reply");
}

export function createQuotaBatch(
  url: string,
  namespace: string,
  timeoutMs: number,
  script: string,
) {
  const redis = new Limits(url, namespace, timeoutMs);
  const batchScript = `
local function operation(KEYS,ARGV)
${script}
end
local results={}
for i=1,#ARGV,2 do
 local k=(i-1)*2
 local ok,value=pcall(operation,{KEYS[k+1],KEYS[k+2],KEYS[k+3],KEYS[k+4]},{ARGV[i],ARGV[i+1]})
 if not ok then results[#results+1]={0,tostring(value)}
 elseif type(value)=='table' and value.err then results[#results+1]={0,value.err}
 else results[#results+1]={1,value} end
end
return results`;
  const queue = new Batcher<Item, string | GatewayError>(
    async (items) => {
      let sentAt = performance.now();
      if (items.every(({ deadline }) => deadline <= sentAt)) return items.map(timeout);
      let submitted = 0;
      const replies = await redis.command("EVAL", () => {
        sentAt = performance.now();
        const live = items.filter(({ deadline }) => deadline > sentAt);
        submitted = live.length;
        if (submitted === 0) return;
        const keys = live.flatMap((item) => item.keys);
        const args = live.flatMap((item) => [item.action, item.payload]);
        return [batchScript, String(keys.length), ...keys, ...args];
      });
      if (submitted === 0) return items.map(timeout);
      if (!Array.isArray(replies) || replies.length !== submitted)
        throw unavailable("Invalid distributed admission batch reply");
      let next = 0;
      return items.map(({ deadline }) =>
        deadline <= sentAt ? timeout() : parseReply(replies[next++]),
      );
    },
    512,
    4,
    1,
    2048,
    { collectWhenIdle: 2 },
  );
  return {
    async call(keys: string[], action: string, payload: string, started: number) {
      const deadline = started + timeoutMs;
      if (deadline <= performance.now()) throw timeout();
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        const reply = await Promise.race([
          queue.add({ keys, action, payload, deadline }),
          new Promise<never>((_, reject) => {
            timer = setTimeout(() => reject(timeout()), Math.ceil(deadline - performance.now()));
          }),
        ]);
        if (performance.now() >= deadline) throw timeout();
        if (reply instanceof GatewayError) throw reply;
        return reply;
      } catch (error) {
        if (error instanceof GatewayError && error.code === "overloaded")
          throw unavailable("Distributed admission batching capacity reached");
        throw error;
      } finally {
        clearTimeout(timer);
      }
    },
    close() {
      queue.close(unavailable("Admission is shutting down"));
      redis.close();
    },
  };
}
