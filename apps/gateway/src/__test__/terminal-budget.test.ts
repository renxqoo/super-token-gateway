import { expect, test } from "bun:test";
import { createTerminalBudget } from "../terminal-budget.ts";

test("stalled durable publication keeps its output reservation until release and cannot consume another request's space", () => {
  const budget = createTerminalBudget(2, (32 << 10) + 100);
  const waiting = budget.reserve();
  const other = budget.reserve();
  expect(waiting.retainOutput(70)).toBe(true);
  expect(other.retainOutput(40)).toBe(false);
  expect(() => budget.reserve()).toThrow();
  waiting.release();
  expect(other.retainOutput(40)).toBe(true);
  const next = budget.reserve();
  waiting.release();
  expect(waiting.retainOutput(0)).toBe(false);
  expect(next.retainOutput(60)).toBe(true);
  expect(() => budget.reserve()).toThrow();
  next.release();
  other.release();
});
