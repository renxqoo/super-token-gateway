import { expect, test } from "bun:test";
import { createJSONDocuments } from "../json-documents.ts";
const bytes = (value: string) => new TextEncoder().encode(value);
test("JSON inspection preserves exact literals and exposes only bounded immutable node facts", () => {
  const registry = createJSONDocuments(),
    original = bytes(
      '{"n":9007199254740993,"d":0.10000000000000001,"a":["北京",true,null],"long":"' +
        "x".repeat(5000) +
        '"}',
    );
  const doc = registry.register("owner", original);
  original.fill(0);
  const facts = registry.inspect("owner", doc.handle, [
    "/n",
    "/d",
    "/a",
    "/a/0",
    "/a/1",
    "/a/2",
    "/absent",
    "/long",
  ]);
  expect(facts["/n"]?.literal).toBe("9007199254740993");
  expect(facts["/d"]?.literal).toBe("0.10000000000000001");
  expect(facts["/a"]?.count).toBe(3);
  expect(facts["/a/0"]?.literal).toBe('"北京"');
  expect(facts["/a/1"]?.kind).toBe("boolean");
  expect(facts["/a/2"]?.kind).toBe("null");
  expect(facts["/absent"]?.kind).toBe("missing");
  expect(facts["/long"]?.literal).toBeUndefined();
  expect(facts["/long"]?.empty).toBe(false);
  registry.dispose("owner");
  expect(registry.snapshot().bytes).toBe(0);
});
test("JSON plans change selected original slices and retain all other numeric and UTF-8 bytes", () => {
  const registry = createJSONDocuments(),
    doc = registry.register(
      "owner",
      bytes(
        ' { "model" : "logical", "n":9007199254740993, "d":0.10000000000000001, "nested":{"a/b":"🌏"} } ',
      ),
    );
  const result = registry.materialize("owner", {
    document: doc.handle,
    edits: [
      { op: "set", pointer: "/model", value: '"upstream"' },
      { op: "set", pointer: "/new", value: '{"safe":true}' },
    ],
  });
  expect(new TextDecoder().decode(result.bytes)).toBe(
    ' { "model" : "upstream", "n":9007199254740993, "d":0.10000000000000001, "nested":{"a/b":"🌏"} ,"new":{"safe":true}} ',
  );
  expect(result.digest).toBe(new Bun.CryptoHasher("sha256").update(result.bytes).digest("hex"));
  expect(registry.snapshot().documents).toBe(0);
  expect(registry.snapshot().outputs).toBe(1);
  expect(() => registry.materialize("owner", { document: doc.handle, edits: [] })).toThrow(
    "JSON_HANDLE",
  );
  registry.dispose("owner");
  expect(registry.snapshot().bytes).toBe(0);
});
test("JSON pointer escaping and single removals use the immutable document rather than shifting arrays", () => {
  for (const [pointer, expected] of [
    ["/a", '{"b":2,"c":3}'],
    ["/b", '{"a":1,"c":3}'],
    ["/c", '{"a":1,"b":2}'],
  ] as const) {
    const r = createJSONDocuments(),
      d = r.register("o", bytes('{"a":1,"b":2,"c":3}'));
    expect(
      JSON.parse(
        new TextDecoder().decode(
          r.materialize("o", { document: d.handle, edits: [{ op: "remove", pointer }] }).bytes,
        ),
      ),
    ).toEqual(JSON.parse(expected));
    r.dispose("o");
  }
  const r = createJSONDocuments(),
    d = r.register("o", bytes('{"a/b":{"~x":[1,2,3]}}'));
  expect(r.inspect("o", d.handle, ["/a~1b/~0x/1"])["/a~1b/~0x/1"]?.literal).toBe("2");
  expect(
    new TextDecoder().decode(
      r.materialize("o", {
        document: d.handle,
        edits: [
          { op: "set", pointer: "/a~1b/~0x/0", value: "10" },
          { op: "remove", pointer: "/a~1b/~0x/2" },
        ],
      }).bytes,
    ),
  ).toBe('{"a/b":{"~x":[10,2]}}');
  r.dispose("o");
});
test("cross owner, document, disposal and consumed handles cannot inspect or modify another snapshot", () => {
  const r = createJSONDocuments(),
    a = r.register("A", bytes('{"a":1}')),
    b = r.register("B", bytes('{"b":2}'));
  expect(() => r.inspect("B", a.handle, ["/a"])).toThrow("JSON_HANDLE");
  expect(() => r.materialize("A", { document: b.handle, edits: [] })).toThrow("JSON_HANDLE");
  r.dispose("A");
  expect(() => r.inspect("A", a.handle, [""])).toThrow("JSON_HANDLE");
  expect(r.inspect("B", b.handle, ["/b"])["/b"]?.literal).toBe("2");
  r.dispose("B");
  expect(r.snapshot().bytes).toBe(0);
});
test("duplicate keys, invalid UTF-8, depth, fields and oversized input fail and return every reservation", () => {
  const r = createJSONDocuments();
  for (const source of [
    '{"a":1,"a":2}',
    '{"a":1,"\\u0061":2}',
    '{"x":"\\q"}',
    '{"x":"unterminated}',
    "[01]",
    "[1,]",
    '{"a":NaN}',
    "[true false]",
    "[".repeat(66) + "0" + "]".repeat(66),
    "[" + Array.from({ length: 100001 }, () => "0").join(",") + "]",
  ]) {
    expect(() => r.register("o", bytes(source))).toThrow();
    expect(r.snapshot().bytes).toBe(0);
  }
  expect(() => r.register("o", new Uint8Array([123, 34, 120, 34, 58, 34, 255, 34, 125]))).toThrow();
  expect(() => r.register("o", new Uint8Array((16 << 20) + 1))).toThrow("JSON_BODY_BUDGET");
  expect(r.snapshot().bytes).toBe(0);
});
test("duplicate, ancestor overlapping, invalid and oversized plans fail without touching the original", () => {
  const r = createJSONDocuments(),
    d = r.register("o", bytes('{"a":{"b":1},"list":[1,2,3]}'));
  for (const edits of [
    [
      { op: "set" as const, pointer: "/a", value: "2" },
      { op: "set" as const, pointer: "/a/b", value: "3" },
    ],
    [
      { op: "remove" as const, pointer: "/list/0" },
      { op: "remove" as const, pointer: "/list/0" },
    ],
    [{ op: "remove" as const, pointer: "/missing" }],
    [{ op: "set" as const, pointer: "/list/01", value: "4" }],
    [{ op: "set" as const, pointer: "/a~2", value: "1" }],
    [{ op: "set" as const, pointer: "/new", value: '"' + "x".repeat(4096) + '"' }],
  ])
    expect(() => r.materialize("o", { document: d.handle, edits })).toThrow();
  expect(r.inspect("o", d.handle, ["/a/b"])["/a/b"]?.literal).toBe("1");
  r.dispose("o");
  expect(r.snapshot().bytes).toBe(0);
});
test("the aggregate JSON budget is reserved before copies and released after validation failure", () => {
  const r = createJSONDocuments(140000),
    d = r.register("o", bytes('{"a":1}'));
  expect(() => r.register("other", bytes('{"a":1}'))).toThrow("JSON_MEMORY_BUDGET");
  r.dispose("o");
  expect(r.snapshot().bytes).toBe(0);
  expect(() => r.inspect("o", d.handle, [""])).toThrow("JSON_HANDLE");
});
test("a non-ASCII token cannot be masked into a valid numeric exponent", () => {
  const r = createJSONDocuments();
  expect(() => r.register("o", bytes('{"n":1尰}'))).toThrow("JSON_INVALID");
  expect(r.snapshot().bytes).toBe(0);
});
test("flat arrays count every element toward the node ceiling and share the aggregate ownership ledger", () => {
  const r = createJSONDocuments(110 << 20),
    raw = bytes("[" + "0,".repeat(99998) + "0]"),
    doc = r.register("A", raw);
  expect(r.inspect("A", doc.handle, [""])[""]?.count).toBe(99999);
  expect(r.snapshot().bytes).toBeGreaterThanOrEqual(100000 * 1024);
  const retained = r.snapshot().bytes;
  expect(() => r.register("B", raw)).toThrow("JSON_MEMORY_BUDGET");
  expect(r.snapshot().bytes).toBe(retained);
  expect(() => r.register("B", bytes("[" + "0,".repeat(99999) + "0]"))).toThrow();
  expect(r.snapshot().bytes).toBe(retained);
  expect(r.inspect("A", doc.handle, ["/99998"])["/99998"]?.literal).toBe("0");
  r.dispose("A");
  expect(r.snapshot().bytes).toBe(0);
});
test("opaque string identity uses Unicode code units including escaped and split-surrogate representations", () => {
  const r = createJSONDocuments(),
    doc = r.register(
      "A",
      bytes('{"literal":"🙂","escaped":"\\ud83d\\ude42","high":"\\ud83d","low":"\\ude42"}'),
    ),
    facts = r.inspect("A", doc.handle, ["/literal", "/escaped", "/high", "/low"]);
  expect(facts["/literal"]?.string_info).toEqual(facts["/escaped"]?.string_info);
  expect(facts["/high"]?.string_info?.digest).not.toBe(facts["/low"]?.string_info?.digest);
  expect(facts["/literal"]?.string_info?.utf16_units).toBe(2);
  r.dispose("A");
  expect(r.snapshot().bytes).toBe(0);
});
test("a compose plan builds a new document from source slices, parsed argument strings and stringified values without changing numbers or UTF-8", () => {
  const registry = createJSONDocuments(),
    doc = registry.register(
      "owner",
      bytes(
        '{"messages":[{"role":"system","content":"规则 🌏"},{"role":"assistant","tool_calls":[{"id":"call_1","function":{"name":"lookup","arguments":"{\\"n\\":9007199254740993,\\"d\\":0.10000000000000001}"}}]}],"keep":{"x":1e400}}',
      ),
    );
  const result = registry.materialize("owner", {
    document: doc.handle,
    compose: {
      template:
        '{"system":null,"messages":[{"role":"assistant","content":[{"type":"tool_use","id":null,"name":null,"input":null}]}],"raw":null,"text":null,"joined":null}',
      slots: [
        { at: "/system", from: "/messages/0/content", mode: "value" },
        { at: "/messages/0/content/0/id", from: "/messages/1/tool_calls/0/id", mode: "value" },
        {
          at: "/messages/0/content/0/name",
          from: "/messages/1/tool_calls/0/function/name",
          mode: "value",
        },
        {
          at: "/messages/0/content/0/input",
          from: "/messages/1/tool_calls/0/function/arguments",
          mode: "parse",
        },
        { at: "/raw", from: "/keep", mode: "value" },
        { at: "/text", from: "/keep", mode: "stringify" },
        {
          at: "/joined",
          sources: ["/messages/0/content", "/messages/1/tool_calls/0/id"],
          mode: "concat",
        },
      ],
    },
  });
  expect(new TextDecoder().decode(result.bytes)).toBe(
    '{"system":"规则 🌏","messages":[{"role":"assistant","content":[{"type":"tool_use","id":"call_1","name":"lookup","input":{"n":9007199254740993,"d":0.10000000000000001}}]}],"raw":{"x":1e400},"text":"{\\"x\\":1e400}","joined":"规则 🌏call_1"}',
  );
  expect(registry.snapshot().documents).toBe(0);
  registry.dispose("owner");
  expect(registry.snapshot().bytes).toBe(0);
});

test("compose rejects non-null or missing targets, missing sources, duplicate targets, unparsable argument strings and oversized templates", () => {
  const registry = createJSONDocuments();
  const source = '{"s":"text","bad":"{not json","n":1}';
  for (const [compose, code] of [
    [{ template: '{"a":1}', slots: [{ at: "/a", from: "/n", mode: "value" }] }, "JSON_PLAN"],
    [{ template: '{"a":null}', slots: [{ at: "/b", from: "/n", mode: "value" }] }, "JSON_PLAN"],
    [
      { template: '{"a":null}', slots: [{ at: "/a", from: "/absent", mode: "value" }] },
      "JSON_PLAN",
    ],
    [{ template: '{"a":null}', slots: [{ at: "/a", from: "/n", mode: "parse" }] }, "JSON_PLAN"],
    [
      {
        template: '{"a":null}',
        slots: [
          { at: "/a", from: "/n", mode: "value" },
          { at: "/a", from: "/s", mode: "value" },
        ],
      },
      "JSON_PLAN_OVERLAP",
    ],
    [
      { template: '{"a":null}', slots: [{ at: "/a", from: "/bad", mode: "parse" }] },
      "JSON_COMPOSE_SOURCE",
    ],
    [{ template: '{"a":"' + "x".repeat(256 << 10) + '"}', slots: [] }, "JSON_PLAN_BUDGET"],
    [{ template: "{broken", slots: [] }, "JSON_INVALID"],
  ] as const) {
    const doc = registry.register("owner", bytes(source));
    expect(() =>
      registry.materialize("owner", {
        document: doc.handle,
        compose: compose as unknown as {
          template: string;
          slots: { at: string; from: string; mode: "value" }[];
        },
      }),
    ).toThrow(code);
    registry.discard("owner", doc.handle);
  }
  registry.dispose("owner");
  expect(registry.snapshot().bytes).toBe(0);
});

test("a wildcard segment expands over array elements into concrete pointer facts within the per-round budget", () => {
  const registry = createJSONDocuments(),
    doc = registry.register(
      "owner",
      bytes(
        '{"messages":[{"role":"user","content":[{"type":"text"},{"type":"image_url"}]},{"role":"assistant","content":"hi"}],"obj":{"a":{"role":"x"}}}',
      ),
    );
  const facts = registry.inspect("owner", doc.handle, [
    "/messages/*/role",
    "/messages/*/content/*/type",
    "/obj/*/role",
  ]);
  expect(Object.keys(facts).sort()).toEqual([
    "/messages/0/content/0/type",
    "/messages/0/content/1/type",
    "/messages/0/role",
    "/messages/1/role",
    "/obj/a/role",
  ]);
  expect(Object.keys(registry.inspect("owner", doc.handle, ["/*"])).sort()).toEqual([
    "/messages",
    "/obj",
  ]);
  expect(facts["/messages/1/role"]?.literal).toBe('"assistant"');
  const many = registry.register(
    "owner",
    bytes(JSON.stringify({ items: Array.from({ length: 4097 }, () => 0) })),
  );
  expect(() => registry.inspect("owner", many.handle, ["/items/*"])).toThrow(
    "JSON_INSPECTION_BUDGET",
  );
  registry.dispose("owner");
  expect(registry.snapshot().bytes).toBe(0);
});
