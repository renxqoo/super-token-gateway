# Native plugin runner

This module is the execution boundary used by the Bun plugin host. It does not implement plugin publication, an adapter SDK, routing, credentials, accounting, or upstream transport. The public Go API is `DefaultConfig`, `ParseConfig`, `Config.Validate`, `New`, and a single `Server.Serve` invocation. `OpenStdio` and `RunWorker` support the executable. Build the executable, rather than importing an Extism instance into Bun.

```sh
go -C packages/plugin-host/native build -o ../../../.local/plugin-host ./cmd/plugin-host
go -C packages/plugin-host/native vet ./... ./__test__
go -C packages/plugin-host/native test ./__test__ -count=1
go -C packages/plugin-host/native test -race ./__test__ -count=1
bun packages/plugin-host/native/acceptance.mts .local/plugin-host
```

Tests build and launch the actual executable in a temporary directory; race tests also build the executable with `-race`. Go ignores `__test__` in `./...`, so the explicit test argument is required. Go 1.27.4, Extism Go SDK 1.7.1 and wazero 1.12.0 are pinned. No Docker, database, Redis, supplier, environment file or runtime test download is required.

## Supervision and framing

Start the executable with stdin/stdout connected to pipes or Unix stream sockets. Terminals and regular files are refused with exit 2. Bun 1.4.4 uses sockets for spawned stdio; both socket and pipe behavior have regression coverage. stderr stays empty; errors, traps, URLs, input, output and guest logs are never logged. The supervisor must continuously consume stdout. Normal exit is 0; protocol transport/resource failure is 1; startup/configuration failure is 2.

`--config BASE64_JSON` accepts a partial configuration over defaults; unknown keys and invalid combinations fail startup. `--worker` is a private child process entry point for the same executable. Configuration arguments contain only numerical budgets. Workers receive a fresh environment containing only `GOMEMLIMIT` and `GOMAXPROCS=2`; no parent environment, credentials or WASI filesystem mappings are inherited by guests.

Each UTF-8 JSON line has `version:2`, an `id` of 1–96 ASCII letters, digits, `_ . : -`, and `op`. Use a new command ID for every command in a connection. Replies can arrive out of order. Every reply has `version` and `id`. Before any command, the parent emits exactly one `{"version":2,"id":"","event":"ready"}` after installing its command reader and cancellation facilities. The worker does not emit READY. Host dispatch waits for this event under `startupMs` (default 5000, integer 1–30000) and the original operation absolute deadline; no frame write timer starts before READY. Missing READY fails startup, and heartbeat or ordinary replies cannot substitute for it. Invalid, duplicate or late READY retires the connection. Startup reservations remain bounded by pending count/bytes; startup does not consume an execution slot or the ordinary busy-slot `queueMs` budget. Once ready, queue admission and frame writes still consume the original operation deadline. Abort of the final startup waiter and host close kill and reap the unready process. A safe `error` code replaces all guest or engine error text. Once per second an unsolicited `{"version":2,"id":"","event":"heartbeat"}` detects closed/stalled stdout even when idle. Supervisors ignore this event for request matching. Malformed/oversized frames can have an empty ID because their claimed fields are untrusted.

The maximum line is `maxFrameBytes` including scanner overhead; keep serialized messages below this value. Unknown JSON fields, malformed base64, unsupported protocol versions and trailing JSON are refused. An oversized line ends the connection after a safe error when stdout is usable. Commands and error responses never echo input fields.

| op       | Fields                                                                                                      | Success                                                                                                     |
| -------- | ----------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `load`   | `hash`: lowercase SHA-256 of exact WASM bytes; `module`: base64 raw WASM                                    | Empty acknowledgement after validation and compilation                                                      |
| `call`   | `hash`, `function`, `input`: base64 bytes; optional `state`: base64 opaque JSON bytes; optional `timeoutMs` | `output`: base64 bytes; `state`: base64 opaque JSON bytes                                                   |
| `cancel` | `target`: accepted call/load ID                                                                             | Empty acknowledgement after cancellation is requested; the target also settles                              |
| `stats`  | none                                                                                                        | `stats` numeric resource snapshot and effective config                                                      |
| `close`  | none                                                                                                        | Stop intake, cancel queued/running work, wait, close all workers, final zero pending/modules snapshot, exit |
| `drain`  | none                                                                                                        | Stop intake, finish accepted work under its existing deadlines, close workers, final snapshot, exit         |

A load also consumes an execution slot. A call timeout starts after queue admission and cache acquisition; queue length/bytes are independently bounded. Call timeout cannot exceed configured `callMs`. Zero/absent timeout selects that configured value. Compilation has its separate `compileMs` deadline. Canceling a queued job does not execute it. Canceling an active call cancels exactly its context. Worker write-lock acquisition and frame transmission share a transport budget capped by `writeMs` and the current call/compile context. Cancellation interrupts a blocked write. A partially transmitted frame retires the worker by killing and waiting it, because that stream cannot safely carry another request. A call canceled before transmission does not cancel another call sharing the worker. Wazero closes the actual execution on cancellation; the 150 ms fallback starts before attempting the cancel frame, and includes any cancel write-lock wait or transmission. If the call has not settled within that entire grace period, the module process is killed and waited. Cancel delivery never receives a fresh `writeMs` allowance. These deadlines bound execution and IPC waits; finite JSON serialization, OS process reaping and scheduler latency add cleanup overhead. Regression tests allow at most 500 ms end to end for a 20 ms call deadline, a 100 ms compile deadline or explicit cancellation with `writeMs:1500`, and assert the owned worker is gone. This fallback can fail other calls sharing a broken module process; Bun must treat their transport failure conservatively. It never fabricates a successful call.

EOF and SIGTERM cancel all accepted work and wait for worker exit. Worker stdin EOF calls `os.Exit(0)` from an independent reader even if compilation/execution is stuck, so SIGKILL of the parent cannot orphan a module process. Normal close explicitly cancels tasks and closes guest instances, compiled plugin/runtime and pipes; parent `Wait` reaps children. The acceptance script tests Bun EOF, parent SIGKILL and closed stdout, then confirms worker PIDs no longer exist.

## Guest call envelope

The selected exported function receives UTF-8 JSON with exactly `input` (base64 bytes) and `state` (the original JSON value, or null). It must return UTF-8 JSON with exactly `output` (base64 bytes, including an empty base64 string when appropriate) and `state` (a JSON value). Missing fields, unknown fields or malformed data fail as `OUTPUT_INVALID`.

The runner copies state as JSON source bytes and never decodes numeric values into floats. Public IPC encodes those bytes as base64, so Bun's outer `JSON.parse` cannot round `9007199254740993` in opaque state. Consumers must retain the state string as opaque data and return it unchanged on the next call; ownership/tenant binding belongs to the Bun attempt context. Input/output are bytes, allowing streaming chunks, binary envelopes and exact lexical usage without an IEEE-754 conversion. Business protocol hooks and terminal semantics are owned by the SDK/adapters; the included synthetic fixture tests these capabilities, not a complete production adapter.

Every call instantiates a fresh guest and closes it after the response is copied. Only a compiled module is cached. Mutable guest globals, guest memory and Extism variables are never reused across calls or tenants. All needed attempt state is explicit. Batch ready bytes only; the transport host must not wait indefinitely to fill a batch.

## Cache and budgets

Each cache entry owns one module child process and one `CompiledPlugin`. Hashes are verified in both parent and worker, including cache hits. No user-supplied manifest, path, URL, secondary module, custom host function or compiler plugin is accepted. Entries are immutable. LRU eviction only selects fully compiled entries with zero in-use references; the old worker is killed and waited before replacing its reservation. A full in-use cache returns `CACHE_BUSY`. A crashed worker is removed and a subsequent load can compile it again.

| Configuration                                  |            Default |                                                      Allowed |
| ---------------------------------------------- | -----------------: | -----------------------------------------------------------: |
| slots / queue                                  |             8 / 24 |                                                1–64 / 0–1024 |
| maxPendingBytes / maxBufferedOutputBytes       |     64 MiB / 8 MiB |                                           4 KiB–256 MiB each |
| maxModuleBytes / maxCacheBytes / maxModules    | 8 MiB / 32 MiB / 4 |                            8 B–32 MiB / up to 256 MiB / 1–32 |
| maxInputBytes / maxOutputBytes / maxStateBytes |       256 KiB each |                          1 B–16 MiB / 1 B–16 MiB / 1 B–1 MiB |
| maxFrameBytes                                  |             12 MiB | up to 48 MiB, sufficient for all configured base64 envelopes |
| memoryPages / tableElements                    |       1024 / 16384 |                             64–1024 pages / 1–65536 elements |
| compileMs / callMs / writeMs                   | 5000 / 1000 / 2000 |                                                 1–30000 each |
| startupMs (host READY deadline)                |               5000 |                                                      1–30000 |
| maxWorkerRssBytes / maxTotalRssBytes           |    512 MiB / 1 GiB |           32 MiB–4 GiB / 32 MiB–8 GiB; total at least worker |
| goMemoryBytes                                  |            256 MiB |                   16 MiB–2 GiB per Go process soft GC target |

`memoryPages` is a hard engine limit for each guest and Extism-kernel linear memory, not an RSS limit. The default is 64 MiB per memory; the fixture separately passes at 8 MiB. The maximum input/output setting does not promise every language runtime can transform that many bytes within its guest memory budget. Bun must negotiate these limits and reject oversized work before sending/charging; large media should use bounded chunks or a separately specified controlled byte-reference ABI. Native input/output/state failures are typed and finite.

Pending byte accounting covers decoded module+input+state+512 bytes per accepted command until its response finishes writing. Response buffers have an independent global byte budget, retained through output backpressure. Admission capacity is held through response writing, so stalled stdout cannot create unlimited completed goroutines. At most slots+queue admitted commands and one current input line are retained; encoding/decoding copies and active guest allocations add overhead. Output pressure returns `OUTPUT_PRESSURE`. Cache bytes measure original module byte lengths, not machine-code allocation; actual compiled memory is protected separately by process budgets.

Compilation/validation run in the module process. A parent deadline kills and waits that process even if the compiler does not honor context. The parent samples worker RSS and its own RSS every 50 ms using Linux `/proc` or bounded `/bin/ps` calls on macOS; per-worker high water kills that worker, total high water shuts down the runner and all workers. Failed RSS measurement stops the affected worker or runner rather than silently disabling the watchdog. Sampling is a watchdog, not a hard OS allocation cap. With many workers the interval includes the bounded sampling calls. `GOMEMLIMIT` is a soft Go heap target. Deploy the entire runner process tree under service-level memory/CPU limits such as Linux systemd `MemoryMax`/cgroups, and have Bun kill/restart the runner on deadline, loss of heartbeat or observed high water. This task validates macOS process isolation, not Linux service enforcement.

Stats report pending/active/cache counts and bytes, buffered response bytes, current sampled parent/worker RSS, peak sampled total RSS, parent heap allocation/system bytes, worker PIDs and parent goroutine count. RSS samples can lag; peak remains after close, and worker RSS is reset to zero after all workers are reaped. Bun can use these measurements to recycle a high-water runner. Idle explicit attempt state belongs to Bun and needs its own aggregate budget there.

## Capability validation

Wabin parses the complete binary, checks duplicate sections, all imports, defined memory/table limits, package and function/type/global budgets; wazero performs executable validation. This is not a byte-rewriting validator. Imported memory/table/global, multiple memory, shared/memory64 flags, unsupported features, start sections, unknown imports and unbounded tables are rejected. Defined memory without a maximum is allowed only under `WithMemoryLimitPages`; a real synthetic growth attempt proves `memory.grow` returns -1 at the configured limit. Declared maxima above the configured cap are refused. Tables require an encoded finite maximum within budget.

The allowlist is encoded in `validate.go`: Extism allocation/input/output/error/config/variable/log/http runtime functions and the minimal WASI imports emitted by the pinned real JS-PDK. HTTP imports remain for PDK compatibility, but the manifest has no allowed hosts and no request can reach a network endpoint. Filesystem path/socket/process imports are denied; WASI has no env, arguments, preopen or usable external stdio. Guest stdout/stderr are discarded and Extism logging is disabled. There are no signing/digest/custom credential functions. Later Bun authentication must consume an authorization descriptor bound to permit, target, method, body digest and expiry, rather than exposing a generic signing oracle.

Errors include `VERSION_UNSUPPORTED`, `COMMAND_ID`, `COMMAND_UNKNOWN`, `COMMAND_DUPLICATE`, `COMMAND_MISSING`, `FRAME_INVALID`, `FRAME_TOO_LARGE`, `MODULE_HASH`, `MODULE_INVALID`, `MODULE_BUDGET`, `MODULE_MISSING`, `MEMORY_BUDGET`, `TABLE_BUDGET`, `CAPABILITY_DENIED`, `CACHE_BUSY`, `OVERLOADED`, `PENDING_BUDGET`, `INPUT_BUDGET`, `STATE_INVALID`, `STATE_BUDGET`, `OUTPUT_INVALID`, `OUTPUT_BUDGET`, `OUTPUT_PRESSURE`, `FUNCTION_INVALID`, `TIMEOUT_INVALID`, `COMPILE_TIMEOUT`, `CALL_TIMEOUT`, `CALL_CANCELLED`, `GUEST_TRAP`, `WORKER_FAILED`, `WORKER_BUDGET` and `WORKER_MONITOR_FAILED`. Startup/transport/total-resource failures exit with the status described above. Guest diagnostics are intentionally not returned.

## Fixture source

`testdata/plugin.wasm` is a checked-in, synthetic TypeScript JS-PDK module. Its source, declarations, independent expected vectors, tool URLs and hashes are in `testdata/provenance.json`. Tests never read the probe worktree and never skip missing fixtures. To rebuild on the recorded macOS arm64 platform, download and verify the two official archives in that file, unpack Extism JS-PDK and Binaryen to a task-local tool directory, and install exact esbuild/lossless-json versions into a separate task-local npm project. Run:

```sh
.local/fixture-tools/node_modules/.bin/esbuild packages/plugin-host/native/testdata/plugin.cts --bundle --format=cjs --target=es2020 --minify --alias:lossless-json="$PWD/.local/fixture-tools/node_modules/lossless-json/lib/esm/index.js" --outfile=.local/plugin-fixture.js
PATH="$PWD/.local/fixture-tools/binaryen-version_133/bin:$PATH" .local/fixture-tools/extism-js .local/plugin-fixture.js -i packages/plugin-host/native/testdata/plugin.d.ts -o packages/plugin-host/native/testdata/plugin.wasm
```

The official v1.7.0 PDK archive self-reports 1.6.1; both values are preserved in provenance. The fixture's memory is not rewritten. Lossless JSON's MIT notice is included beside the fixture. Extism JS-PDK source/license is [BSD-3-Clause](https://github.com/extism/js-pdk/blob/v1.7.0/LICENSE); QuickJS is [MIT](https://bellard.org/quickjs/); Binaryen's build tools are [Apache-2.0](https://github.com/WebAssembly/binaryen/blob/version_133/LICENSE). This fixture is used for isolated testing only.
