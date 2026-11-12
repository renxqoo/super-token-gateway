import { describe, expect, test } from "bun:test";
import { decrypt, encrypt, secureEqual } from "../crypto.ts";
import { canonical } from "@gateway/kernel";
describe("secret and outbound boundaries", () => {
  test("authenticated encryption binds a provider identity", () => {
    const key = "11".repeat(32);
    const value = encrypt("provider-secret", key, "provider-1");
    expect(value).not.toContain("provider-secret");
    expect(decrypt(value, key, "provider-1")).toBe("provider-secret");
    expect(() => decrypt(value, key, "provider-2")).toThrow();
    expect(secureEqual("abc", "abc")).toBe(true);
    expect(secureEqual("abc", "ab")).toBe(false);
  });
  test("canonical fingerprints preserve array order and explicit zero/false", () => {
    expect(canonical({ b: false, a: [0, 1] })).toBe(canonical({ a: [0, 1], b: false }));
    expect(canonical({ a: [1, 0], b: false })).not.toBe(canonical({ a: [0, 1], b: false }));
  });
});
