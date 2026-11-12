package native_test

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	native "gateway.local/plugin-host"
	"github.com/tetratelabs/wabin/binary"
	"github.com/tetratelabs/wabin/leb128"
	"github.com/tetratelabs/wabin/wasm"
)

var executable string
var fixture []byte
var fixtureHash string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "gateway-plugin-tests-")
	if err != nil {
		panic(err)
	}
	executable = filepath.Join(dir, "plugin-host")
	args := []string{"build", "-o", executable}
	info, _ := debug.ReadBuildInfo()
	for _, setting := range info.Settings {
		if setting.Key == "-race" && setting.Value == "true" {
			args = append(args, "-race")
		}
	}
	args = append(args, "../cmd/plugin-host")
	cmd := exec.Command("go", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if cmd.Run() != nil {
		os.RemoveAll(dir)
		os.Exit(1)
	}
	fixture, err = os.ReadFile("../testdata/plugin.wasm")
	if err != nil {
		panic(err)
	}
	fixtureHash = digest(fixture)
	exit := m.Run()
	os.RemoveAll(dir)
	os.Exit(exit)
}
func digest(data []byte) string { s := sha256.Sum256(data); return hex.EncodeToString(s[:]) }
func jsonBytes(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

type client struct {
	t       *testing.T
	cmd     *exec.Cmd
	in      io.WriteCloser
	out     io.ReadCloser
	err     bytes.Buffer
	stdout  bytes.Buffer
	mu      sync.Mutex
	writes  sync.Mutex
	pending map[string]chan native.Response
	seq     atomic.Int64
	done    chan struct{}
	ready   atomic.Int32
	exit    error
}

func launch(t *testing.T, c native.Config) *client {
	t.Helper()
	data := base64.StdEncoding.EncodeToString(jsonBytes(c))
	cmd := exec.Command(executable, "--config", data)
	cmd.Env = append(os.Environ(), "CANARY_T02A=private-env-canary")
	p := &client{t: t, cmd: cmd, pending: map[string]chan native.Response{}, done: make(chan struct{})}
	var err error
	p.in, err = cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	p.out, err = cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = &p.err
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(p.out)
		scanner.Buffer(make([]byte, 4096), 48<<20)
		for scanner.Scan() {
			p.stdout.Write(scanner.Bytes())
			var r native.Response
			if json.Unmarshal(scanner.Bytes(), &r) != nil {
				continue
			}
			if r.Event == "ready" {
				if r.Version != 2 || r.ID != "" || p.ready.Add(1) != 1 {
					t.Error("invalid or duplicate READY")
				} else {
					close(ready)
				}
				continue
			}
			if p.ready.Load() != 1 {
				t.Error("response before READY")
			}
			p.mu.Lock()
			ch := p.pending[r.ID]
			p.mu.Unlock()
			if ch != nil {
				ch <- r
			}
		}
		p.exit = cmd.Wait()
		close(p.done)
	}()
	t.Cleanup(func() {
		select {
		case <-p.done:
		default:
			p.in.Close()
			select {
			case <-p.done:
			case <-time.After(4 * time.Second):
				p.cmd.Process.Kill()
				<-p.done
				t.Error("host did not close after EOF")
			}
		}
		if p.err.Len() != 0 {
			t.Errorf("stderr must be empty, bytes=%d", p.err.Len())
		}
	})
	select {
	case <-ready:
	case <-p.done:
		t.Fatal("host exited before READY")
	case <-time.After(3 * time.Second):
		t.Fatal("host did not become READY")
	}
	return p
}
func (p *client) send(req native.Request) (string, <-chan native.Response) {
	if req.ID == "" {
		req.ID = fmt.Sprintf("c%d", p.seq.Add(1))
	}
	req.Version = 2
	ch := make(chan native.Response, 1)
	p.mu.Lock()
	p.pending[req.ID] = ch
	p.mu.Unlock()
	p.writes.Lock()
	_, err := p.in.Write(append(jsonBytes(req), '\n'))
	p.writes.Unlock()
	if err != nil {
		p.t.Fatal(err)
	}
	return req.ID, ch
}
func (p *client) wait(ch <-chan native.Response) native.Response {
	p.t.Helper()
	select {
	case r := <-ch:
		return r
	case <-p.done:
		p.t.Fatalf("host exited: %v", p.exit)
	case <-time.After(12 * time.Second):
		p.t.Fatal("response deadline")
	}
	return native.Response{}
}
func (p *client) request(req native.Request) native.Response { _, ch := p.send(req); return p.wait(ch) }
func (p *client) load(module []byte) string {
	p.t.Helper()
	h := digest(module)
	r := p.request(native.Request{Op: "load", Hash: h, Module: module})
	if r.Error != "" {
		p.t.Fatalf("load: %s", r.Error)
	}
	return h
}
func (p *client) call(hash string, input any, state json.RawMessage) native.Response {
	return p.request(native.Request{Op: "call", Hash: hash, Function: "invoke", Input: jsonBytes(input), State: state})
}
func (p *client) close() {
	p.t.Helper()
	r := p.request(native.Request{Op: "close"})
	if r.Error != "" || r.Stats == nil || r.Stats.Pending != 0 || r.Stats.Modules != 0 || len(r.Stats.Workers) != 0 {
		p.t.Fatalf("close: %+v", r)
	}
	select {
	case <-p.done:
		if p.exit != nil {
			p.t.Fatal(p.exit)
		}
	case <-time.After(3 * time.Second):
		p.t.Fatal("close exit deadline")
	}
}
func expectError(t *testing.T, r native.Response, want string) {
	t.Helper()
	if r.Error != want {
		t.Fatalf("want %s, got %s", want, r.Error)
	}
}
func output(t *testing.T, r native.Response) map[string]any {
	t.Helper()
	if r.Error != "" {
		t.Fatal(r.Error)
	}
	var v map[string]any
	if json.Unmarshal(r.Output, &v) != nil {
		t.Fatalf("bad output %q", r.Output)
	}
	return v
}

func TestRealPDKIncrementalUTF8ToolAndExactUsage(t *testing.T) {
	p := launch(t, native.DefaultConfig())
	p.load(fixture)
	var v struct {
		Frames []string `json:"frames"`
	}
	data, err := os.ReadFile("../testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	if json.Unmarshal(data, &v) != nil {
		t.Fatal("vectors")
	}
	r := p.call(fixtureHash, map[string]any{"op": "begin", "id": "tenant-a"}, nil)
	state := r.State
	stream := []byte(strings.Join(v.Frames, ""))
	var text, tool, exact string
	first := -1
	for i, b := range stream {
		r = p.call(fixtureHash, map[string]any{"op": "push", "bytes": []int{int(b)}}, state)
		value := output(t, r)
		state = r.State
		events := value["events"].([]any)
		if len(events) > 0 && first < 0 {
			first = i
		}
		for _, event := range events {
			e := event.(map[string]any)
			switch e["type"] {
			case "text":
				text += e["text"].(string)
			case "tool":
				tool += e["args"].(string)
			case "exact":
				exact = e["decimal"].(string)
			}
		}
	}
	end := output(t, p.call(fixtureHash, map[string]any{"op": "end"}, state))
	usage := end["usage"].(map[string]any)
	if text != "你🙂" || tool != "{\"city\":\"北京\",\"n\":9007199254740993}" || exact != "0.10000000000000001" || usage["input_tokens"] != "9007199254740993" || usage["output_tokens"] != "17" || end["frames"] != float64(6) || first >= len(stream)-1 || first < 0 {
		t.Fatalf("stream mismatch: %s %s %s %+v first=%d", text, tool, exact, usage, first)
	}
	p.close()
}

func TestMissingTerminalAndMalformedPayloadAreRejected(t *testing.T) {
	p := launch(t, native.DefaultConfig())
	p.load(fixture)
	r := p.call(fixtureHash, map[string]any{"op": "begin", "id": "a"}, nil)
	expectError(t, p.call(fixtureHash, map[string]any{"op": "end"}, r.State), "GUEST_TRAP")
	expectError(t, p.call(fixtureHash, map[string]any{"op": "push", "bytes": []int{255, 0, 0, 0}}, r.State), "GUEST_TRAP")
	expectError(t, p.call(fixtureHash, map[string]any{"op": "invalid"}, nil), "OUTPUT_INVALID")
	p.close()
}

func TestFreshGuestDoesNotLeakHiddenGlobalAcrossTenants(t *testing.T) {
	p := launch(t, native.DefaultConfig())
	p.load(fixture)
	for _, secret := range []string{"tenant-a-private", "tenant-b-private", "tenant-a-private"} {
		value := output(t, p.call(fixtureHash, map[string]any{"op": "sticky", "value": secret}, nil))
		if value["previous"] != nil || value["current"] != secret {
			t.Fatal("guest leaked prior tenant")
		}
	}
	p.close()
}

func TestConcurrentCallsKeepExplicitStateIsolated(t *testing.T) {
	p := launch(t, native.DefaultConfig())
	p.load(fixture)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("tenant-%d", i)
			r := p.call(fixtureHash, map[string]any{"op": "begin", "id": id}, nil)
			var state map[string]any
			if r.Error != "" || json.Unmarshal(r.State, &state) != nil || state["id"] != id {
				t.Errorf("cross tenant state %d: %s", i, r.Error)
			}
		}(i)
	}
	wg.Wait()
	p.close()
}

func TestInfiniteLoopDeadlineReallyStopsAndNextCallWorks(t *testing.T) {
	p := launch(t, native.DefaultConfig())
	p.load(fixture)
	start := time.Now()
	r := p.request(native.Request{Op: "call", Hash: fixtureHash, Function: "loop", TimeoutMS: 60})
	expectError(t, r, "CALL_TIMEOUT")
	if time.Since(start) > time.Second {
		t.Fatal("loop was not stopped promptly")
	}
	value := output(t, p.call(fixtureHash, map[string]any{"op": "echo", "value": map[string]any{"alive": true}}, nil))
	if value["alive"] != true {
		t.Fatal("host unusable")
	}
	p.close()
}

func TestGuestOOMIsBoundedAndNextCallWorks(t *testing.T) {
	c := native.DefaultConfig()
	c.MemoryPages = 128
	p := launch(t, c)
	p.load(fixture)
	r := p.request(native.Request{Op: "call", Hash: fixtureHash, Function: "exhaustMemory"})
	expectError(t, r, "GUEST_TRAP")
	if output(t, p.call(fixtureHash, map[string]any{"op": "echo", "value": map[string]any{"alive": true}}, nil))["alive"] != true {
		t.Fatal("host unusable")
	}
	p.close()
}

func TestCancelTargetsOnlyOneCallAndCloseCancelsRemaining(t *testing.T) {
	c := native.DefaultConfig()
	c.CallMS = 3000
	p := launch(t, c)
	p.load(fixture)
	id, a := p.send(native.Request{Op: "call", Hash: fixtureHash, Function: "loop"})
	_, b := p.send(native.Request{Op: "call", Hash: fixtureHash, Function: "loop"})
	time.Sleep(50 * time.Millisecond)
	expectError(t, p.request(native.Request{Op: "cancel", Target: id}), "")
	expectError(t, p.wait(a), "CALL_CANCELLED")
	select {
	case r := <-b:
		t.Fatalf("cancel affected unrelated call %s", r.Error)
	case <-time.After(50 * time.Millisecond):
	}
	_, closeResponse := p.send(native.Request{Op: "close"})
	expectError(t, p.wait(b), "CALL_CANCELLED")
	r := p.wait(closeResponse)
	if r.Stats.Pending != 0 || r.Stats.Modules != 0 {
		t.Fatal("close leaked resources")
	}
}

func TestAdmissionQueueRejectsOverload(t *testing.T) {
	c := native.DefaultConfig()
	c.Slots = 1
	c.Queue = 1
	c.CallMS = 3000
	p := launch(t, c)
	p.load(fixture)
	id, a := p.send(native.Request{Op: "call", Hash: fixtureHash, Function: "loop"})
	_, b := p.send(native.Request{Op: "call", Hash: fixtureHash, Function: "loop"})
	expectError(t, p.request(native.Request{Op: "call", Hash: fixtureHash, Function: "invoke", Input: jsonBytes(map[string]any{"op": "echo"})}), "OVERLOADED")
	p.request(native.Request{Op: "cancel", Target: id})
	expectError(t, p.wait(a), "CALL_CANCELLED")
	_, cl := p.send(native.Request{Op: "close"})
	expectError(t, p.wait(b), "CALL_CANCELLED")
	p.wait(cl)
}

func TestSafeProtocolErrorsAndInputStateOutputBudgets(t *testing.T) {
	c := native.DefaultConfig()
	c.MaxInputBytes = 256
	c.MaxStateBytes = 128
	c.MaxOutputBytes = 128
	p := launch(t, c)
	p.load(fixture)
	expectError(t, p.request(native.Request{Op: "unknown"}), "COMMAND_UNKNOWN")
	expectError(t, p.request(native.Request{Op: "load", Hash: strings.Repeat("0", 64), Module: fixture}), "MODULE_HASH")
	expectError(t, p.request(native.Request{Op: "call", Hash: fixtureHash, Function: "invoke", Input: make([]byte, 257)}), "INPUT_BUDGET")
	expectError(t, p.request(native.Request{Op: "call", Hash: fixtureHash, Function: "invoke", State: jsonBytes(strings.Repeat("s", 129))}), "STATE_BUDGET")
	expectError(t, p.call(fixtureHash, map[string]any{"op": "large", "bytes": 300}, nil), "OUTPUT_BUDGET")
	expectError(t, p.call(fixtureHash, map[string]any{"op": "state", "state": strings.Repeat("s", 160)}, nil), "STATE_BUDGET")
	p.close()
}

func TestNetworkAndLogsCannotExposeBody(t *testing.T) {
	var hits atomic.Int64
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); w.WriteHeader(204) })}
	go server.Serve(listener)
	defer server.Close()
	p := launch(t, native.DefaultConfig())
	p.load(fixture)
	expectError(t, p.call(fixtureHash, map[string]any{"op": "network", "url": "http://" + listener.Addr().String() + "/private-body-canary"}, nil), "GUEST_TRAP")
	if output(t, p.call(fixtureHash, map[string]any{"op": "log", "value": "private-body-canary"}, nil))["logged"] != true {
		t.Fatal("log fixture")
	}
	p.close()
	if hits.Load() != 0 {
		t.Fatal("sandbox reached network")
	}
	if p.err.Len() != 0 || bytes.Contains(p.stdout.Bytes(), []byte("private-body-canary")) {
		t.Fatal("body reached logs or protocol errors")
	}
}

func tinyModule(memory *wasm.Memory, table *wasm.Table, imports []*wasm.Import) []byte {
	m := &wasm.Module{TypeSection: []*wasm.FunctionType{{Results: []wasm.ValueType{wasm.ValueTypeI32}}}, FunctionSection: []wasm.Index{0}, CodeSection: []*wasm.Code{{Body: []byte{0x41, 0x00, 0x0b}}}, MemorySection: memory, ImportSection: imports}
	if table != nil {
		m.TableSection = []*wasm.Table{table}
	}
	return binary.EncodeModule(m)
}
func TestValidatorRejectsUnknownImportsImportedMemoryAndUnboundedTables(t *testing.T) {
	p := launch(t, native.DefaultConfig())
	vectors := []struct {
		name   string
		module []byte
		want   string
	}{
		{"custom-host", tinyModule(nil, nil, []*wasm.Import{{Type: wasm.ExternTypeFunc, Module: "gateway", Name: "sign", DescFunc: 0}}), "CAPABILITY_DENIED"},
		{"filesystem", tinyModule(nil, nil, []*wasm.Import{{Type: wasm.ExternTypeFunc, Module: "wasi_snapshot_preview1", Name: "path_open", DescFunc: 0}}), "CAPABILITY_DENIED"},
		{"imported-memory", tinyModule(nil, nil, []*wasm.Import{{Type: wasm.ExternTypeMemory, Module: "env", Name: "memory", DescMem: &wasm.Memory{Min: 1}}}), "CAPABILITY_DENIED"},
		{"unbounded-table", tinyModule(nil, &wasm.Table{Min: 1, Type: wasm.RefTypeFuncref}, nil), "TABLE_BUDGET"},
		{"memory", tinyModule(&wasm.Memory{Min: 1025}, nil, nil), "MEMORY_BUDGET"},
	}
	for _, v := range vectors {
		t.Run(v.name, func(t *testing.T) {
			expectError(t, p.request(native.Request{Op: "load", Hash: digest(v.module), Module: v.module}), v.want)
		})
	}
	p.close()
}

func TestCacheEvictionHonorsInUseReferences(t *testing.T) {
	c := native.DefaultConfig()
	c.MaxModules = 1
	c.CallMS = 3000
	p := launch(t, c)
	p.load(fixture)
	id, ch := p.send(native.Request{Op: "call", Hash: fixtureHash, Function: "loop"})
	time.Sleep(30 * time.Millisecond)
	module := tinyModule(nil, nil, nil)
	expectError(t, p.request(native.Request{Op: "load", Hash: digest(module), Module: module}), "CACHE_BUSY")
	p.request(native.Request{Op: "cancel", Target: id})
	expectError(t, p.wait(ch), "CALL_CANCELLED")
	p.load(module)
	expectError(t, p.call(fixtureHash, map[string]any{"op": "echo"}, nil), "MODULE_MISSING")
	p.close()
}

func waitGone(t *testing.T, pids []int) {
	t.Helper()
	for _, pid := range pids {
		deadline := time.Now().Add(3 * time.Second)
		for syscall.Kill(pid, 0) == nil {
			if time.Now().After(deadline) {
				t.Fatalf("worker %d survived", pid)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}
func TestEOFAndSIGTERMCloseWorkers(t *testing.T) {
	for _, mode := range []string{"eof", "sigterm"} {
		t.Run(mode, func(t *testing.T) {
			p := launch(t, native.DefaultConfig())
			p.load(fixture)
			stats := p.request(native.Request{Op: "stats"}).Stats
			p.send(native.Request{Op: "call", Hash: fixtureHash, Function: "loop"})
			if mode == "eof" {
				p.in.Close()
			} else {
				p.cmd.Process.Signal(syscall.SIGTERM)
			}
			select {
			case <-p.done:
			case <-time.After(3 * time.Second):
				t.Fatal("shutdown deadline")
			}
			waitGone(t, stats.Workers)
		})
	}
}
func TestKilledParentLeavesNoModuleWorker(t *testing.T) {
	p := launch(t, native.DefaultConfig())
	p.load(fixture)
	stats := p.request(native.Request{Op: "stats"}).Stats
	p.send(native.Request{Op: "call", Hash: fixtureHash, Function: "loop"})
	p.cmd.Process.Kill()
	<-p.done
	waitGone(t, stats.Workers)
}
func TestClosedStdoutStopsParentAndWorkers(t *testing.T) {
	p := launch(t, native.DefaultConfig())
	p.load(fixture)
	stats := p.request(native.Request{Op: "stats"}).Stats
	p.out.Close()
	select {
	case <-p.done:
	case <-time.After(4 * time.Second):
		t.Fatal("closed stdout was not detected")
	}
	waitGone(t, stats.Workers)
}

func TestOversizedStdinTerminatesSafely(t *testing.T) {
	c := native.DefaultConfig()
	c.MaxModuleBytes = 4096
	c.MaxCacheBytes = 4096
	c.MaxInputBytes = 128
	c.MaxOutputBytes = 128
	c.MaxStateBytes = 128
	c.MaxFrameBytes = 10000
	p := launch(t, c)
	p.in.Write(bytes.Repeat([]byte{'x'}, 11000))
	p.in.Write([]byte{'\n'})
	select {
	case <-p.done:
		if p.exit == nil {
			t.Fatal("expected rejected input exit")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("oversized frame hung")
	}
}

func TestCompileDeadlineKillsWorkerAndDoesNotPoisonCache(t *testing.T) {
	c := native.DefaultConfig()
	c.CompileMS = 1
	p := launch(t, c)
	expectError(t, p.request(native.Request{Op: "load", Hash: fixtureHash, Module: fixture}), "COMPILE_TIMEOUT")
	stats := p.request(native.Request{Op: "stats"}).Stats
	if stats.Modules != 0 || len(stats.Workers) != 0 {
		t.Fatal("compile worker survived")
	}
	p.close()
}

func TestPendingByteBudgetRejectsBeforeQueueing(t *testing.T) {
	c := native.DefaultConfig()
	c.MaxPendingBytes = 4096
	p := launch(t, c)
	expectError(t, p.request(native.Request{Op: "load", Hash: fixtureHash, Module: fixture}), "PENDING_BUDGET")
	if p.request(native.Request{Op: "stats"}).Stats.Pending != 0 {
		t.Fatal("rejected request queued")
	}
	p.close()
}

func guardedModule(guard []byte, extraTypes []*wasm.FunctionType, extraImports []*wasm.Import, states ...string) []byte {
	types := []*wasm.FunctionType{{Params: []wasm.ValueType{wasm.ValueTypeI64}, Results: []wasm.ValueType{wasm.ValueTypeI64}}, {Params: []wasm.ValueType{wasm.ValueTypeI64, wasm.ValueTypeI32}}, {Params: []wasm.ValueType{wasm.ValueTypeI64, wasm.ValueTypeI64}}, {Results: []wasm.ValueType{wasm.ValueTypeI32}}}
	imports := []*wasm.Import{{Type: wasm.ExternTypeFunc, Module: "extism:host/env", Name: "alloc", DescFunc: 0}, {Type: wasm.ExternTypeFunc, Module: "extism:host/env", Name: "store_u8", DescFunc: 1}, {Type: wasm.ExternTypeFunc, Module: "extism:host/env", Name: "output_set", DescFunc: 2}}
	imports = append(imports, extraImports...)
	types = append(types, extraTypes...)
	state := "null"
	if len(states) > 0 {
		state = states[0]
	}
	envelope := []byte(`{"output":"eyJndWFyZCI6dHJ1ZX0=","state":` + state + `}`)
	body := append([]byte(nil), guard...)
	body = append(body, 0x42, byte(len(envelope)), 0x10, 0, 0x21, 0)
	for i, b := range envelope {
		body = append(body, 0x20, 0, 0x42, byte(i), 0x7c, 0x41)
		body = append(body, leb128.EncodeInt32(int32(b))...)
		body = append(body, 0x10, 1)
	}
	body = append(body, 0x20, 0, 0x42, byte(len(envelope)), 0x10, 2, 0x41, 0, 0x0b)
	m := &wasm.Module{TypeSection: types, ImportSection: imports, FunctionSection: []wasm.Index{3}, MemorySection: &wasm.Memory{Min: 1}, ExportSection: []*wasm.Export{{Type: wasm.ExternTypeFunc, Name: "invoke", Index: uint32(len(imports))}}, CodeSection: []*wasm.Code{{LocalTypes: []wasm.ValueType{wasm.ValueTypeI64}, Body: body}}}
	return binary.EncodeModule(m)
}

func TestEngineCapsUnboundedMemoryDeclaration(t *testing.T) {
	c := native.DefaultConfig()
	c.MemoryPages = 64
	p := launch(t, c)
	guard := []byte{0x41, 0xc0, 0, 0x40, 0, 0x41, 0x7f, 0x47, 0x04, 0x40, 0x00, 0x0b}
	h := p.load(guardedModule(guard, nil, nil))
	if output(t, p.call(h, map[string]any{}, nil))["guard"] != true {
		t.Fatal("memory grew past engine maximum")
	}
	p.close()
}

func TestWASIHasNoEnvironmentOrPreopenedFilesystem(t *testing.T) {
	p := launch(t, native.DefaultConfig())
	types := []*wasm.FunctionType{{Params: []wasm.ValueType{wasm.ValueTypeI32, wasm.ValueTypeI32}, Results: []wasm.ValueType{wasm.ValueTypeI32}}}
	imports := []*wasm.Import{{Type: wasm.ExternTypeFunc, Module: "wasi_snapshot_preview1", Name: "environ_sizes_get", DescFunc: 4}, {Type: wasm.ExternTypeFunc, Module: "wasi_snapshot_preview1", Name: "fd_prestat_get", DescFunc: 4}}
	guard := []byte{0x41, 0, 0x41, 4, 0x10, 3, 0x1a, 0x41, 0, 0x28, 2, 0, 0x04, 0x40, 0x00, 0x0b, 0x41, 3, 0x41, 8, 0x10, 4, 0x41, 8, 0x47, 0x04, 0x40, 0x00, 0x0b}
	h := p.load(guardedModule(guard, types, imports))
	if output(t, p.call(h, map[string]any{}, nil))["guard"] != true {
		t.Fatal("environment or directory exposed")
	}
	p.close()
}

func TestBlockedStdoutHasDeadlineAndReapsWorkers(t *testing.T) {
	c := native.DefaultConfig()
	c.WriteMS = 100
	cmd := exec.Command(executable, "--config", base64.StdEncoding.EncodeToString(jsonBytes(c)))
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if cmd.Start() != nil {
		t.Fatal("start")
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() { cmd.Process.Kill(); in.Close(); out.Close() })
	reader := bufio.NewReader(out)
	request := func(req native.Request) native.Response {
		req.Version = 2
		in.Write(append(jsonBytes(req), '\n'))
		for {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				t.Fatal(err)
			}
			var r native.Response
			if json.Unmarshal(line, &r) != nil {
				t.Fatal("invalid frame")
			}
			if r.ID == req.ID {
				return r
			}
		}
	}
	expectError(t, request(native.Request{ID: "load", Op: "load", Hash: fixtureHash, Module: fixture}), "")
	stats := request(native.Request{ID: "stats", Op: "stats"}).Stats
	req := native.Request{Version: 2, ID: "large", Op: "call", Hash: fixtureHash, Function: "invoke", Input: jsonBytes(map[string]any{"op": "large", "bytes": 200000})}
	in.Write(append(jsonBytes(req), '\n'))
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("backpressure was ignored")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stdout write deadline did not stop service")
	}
	waitGone(t, stats.Workers)
	if stderr.Len() != 0 {
		t.Fatal("stderr exposed content")
	}
}

func TestWorkerRSSGuardKillsAndReapsCompilation(t *testing.T) {
	c := native.DefaultConfig()
	c.MaxWorkerRSSBytes = 32 << 20
	p := launch(t, c)
	r := p.request(native.Request{Op: "load", Hash: fixtureHash, Module: fixture})
	expectError(t, r, "WORKER_BUDGET")
	if p.request(native.Request{Op: "stats"}).Stats.Modules != 0 {
		t.Fatal("failed worker retained in cache")
	}
	p.close()
}

func TestOpaqueStatePreservesIntegersBeyondJavaScriptRange(t *testing.T) {
	p := launch(t, native.DefaultConfig())
	h := p.load(guardedModule(nil, nil, nil, "9007199254740993"))
	r := p.call(h, map[string]any{}, nil)
	if r.Error != "" || string(r.State) != "9007199254740993" {
		t.Fatal("opaque state lost numeric precision")
	}
	r = p.call(h, map[string]any{}, r.State)
	if r.Error != "" || string(r.State) != "9007199254740993" {
		t.Fatal("state changed during IPC round trip")
	}
	p.close()
}

func TestModuleBudgetAndMalformedFramesReturnSafeCodes(t *testing.T) {
	c := native.DefaultConfig()
	c.MaxModuleBytes = 1 << 20
	p := launch(t, c)
	expectError(t, p.request(native.Request{Op: "load", Hash: fixtureHash, Module: fixture}), "MODULE_BUDGET")
	p.in.Write([]byte("{\"version\":2,\"id\":\"bad\",\"op\":\"load\",\"manifest\":{\"url\":\"file:///private-canary\"}}\n"))
	p.in.Write([]byte("{\"version\":2,\"id\":\"bad2\",\"op\":\"call\",\"input\":\"!!!\"}\n"))
	p.in.Write([]byte("{\"version\":3,\"id\":\"bad3\",\"op\":\"stats\"}\n"))
	p.request(native.Request{Op: "stats"})
	p.close()
	if !bytes.Contains(p.stdout.Bytes(), []byte(`"error":"FRAME_INVALID"`)) || !bytes.Contains(p.stdout.Bytes(), []byte(`"error":"VERSION_UNSUPPORTED"`)) || bytes.Contains(p.stdout.Bytes(), []byte("private-canary")) {
		t.Fatal("unsafe or missing protocol rejection")
	}
}

func TestDrainWaitsForAcceptedCallsThenClosesWorkers(t *testing.T) {
	p := launch(t, native.DefaultConfig())
	p.load(fixture)
	_, call := p.send(native.Request{Op: "call", Hash: fixtureHash, Function: "invoke", Input: jsonBytes(map[string]any{"op": "echo", "value": map[string]any{"done": true}})})
	_, drain := p.send(native.Request{Op: "drain"})
	if output(t, p.wait(call))["done"] != true {
		t.Fatal("drain discarded accepted work")
	}
	r := p.wait(drain)
	if r.Stats.Pending != 0 || r.Stats.PendingBytes != 0 || r.Stats.Modules != 0 {
		t.Fatal("drain retained resources")
	}
}

func TestUnixSocketStdioReturnsCommandResponses(t *testing.T) {
	inPair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	outPair, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	inParent := os.NewFile(uintptr(inPair[0]), "in-parent")
	inChild := os.NewFile(uintptr(inPair[1]), "in-child")
	outParent := os.NewFile(uintptr(outPair[0]), "out-parent")
	outChild := os.NewFile(uintptr(outPair[1]), "out-child")
	defer inParent.Close()
	defer outParent.Close()
	cmd := exec.Command(executable)
	cmd.Stdin = inChild
	cmd.Stdout = outChild
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if cmd.Start() != nil {
		t.Fatal("start")
	}
	inChild.Close()
	outChild.Close()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() { cmd.Process.Kill() })
	inParent.Write([]byte("{\"version\":2,\"id\":\"close\",\"op\":\"close\"}\n"))
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("socket stdio hung")
	}
	data, err := io.ReadAll(outParent)
	if err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	var ready, reply native.Response
	if !scanner.Scan() || json.Unmarshal(scanner.Bytes(), &ready) != nil || ready.Version != 2 || ready.Event != "ready" || ready.ID != "" {
		t.Fatalf("missing socket READY: %q", data)
	}
	if !scanner.Scan() || json.Unmarshal(scanner.Bytes(), &reply) != nil || reply.ID != "close" || reply.Stats == nil || scanner.Scan() {
		t.Fatalf("missing socket reply: %q", data)
	}
	if stderr.Len() != 0 {
		t.Fatal("unexpected stderr")
	}
}

func TestGlobalRSSBudgetStopsRunnerAndAllModuleWorkers(t *testing.T) {
	c := native.DefaultConfig()
	c.MaxWorkerRSSBytes = 32 << 20
	c.MaxTotalRSSBytes = 32 << 20
	p := launch(t, c)
	req := native.Request{Version: 2, ID: "load", Op: "load", Hash: fixtureHash, Module: fixture}
	p.in.Write(append(jsonBytes(req), '\n'))
	select {
	case <-p.done:
		if p.cmd.ProcessState.ExitCode() != 1 {
			t.Fatal("total RSS budget did not stop runner")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("global RSS budget was not enforced")
	}
	data, err := exec.Command("/bin/ps", "-axo", "pid,command").Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.Contains(line, executable+" --worker ") {
			t.Fatal("global budget shutdown left module process")
		}
	}
}
