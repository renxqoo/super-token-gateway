package native

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"runtime/debug"
	"sync"

	extism "github.com/extism/go-sdk"
	"github.com/tetratelabs/wazero"
)

type guestInput struct {
	Input []byte          `json:"input"`
	State json.RawMessage `json:"state"`
}
type guestOutput struct {
	Output []byte          `json:"output"`
	State  json.RawMessage `json:"state"`
}

func RunWorker(c Config, input *os.File, output *os.File) int {
	if c.Validate() != nil {
		return 2
	}
	debug.SetMemoryLimit(c.GoMemoryBytes)
	extism.SetLogLevel(extism.LogLevelOff)
	os.Unsetenv("EXTISM_ENABLE_WASI_OUTPUT")
	frames := make(chan []byte, 1)
	go func() {
		scanner := bufio.NewScanner(input)
		scanner.Buffer(make([]byte, 4096), c.MaxFrameBytes)
		for scanner.Scan() {
			frame := append([]byte(nil), scanner.Bytes()...)
			frames <- frame
		}
		os.Exit(0)
	}()
	var load Request
	if decode(<-frames, &load) != nil || load.Op != "load" {
		return 2
	}
	writeMu := sync.Mutex{}
	write := func(r Response) {
		writeMu.Lock()
		defer writeMu.Unlock()
		if json.NewEncoder(output).Encode(r) != nil {
			os.Exit(0)
		}
	}
	fail := func(err error) int { write(Response{Version: 2, ID: load.ID, Error: err.Error()}); return 1 }
	if err := validateModule(load.Module, load.Hash, c); err != nil {
		return fail(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manifest := extism.Manifest{Wasm: []extism.Wasm{extism.WasmData{Data: load.Module}}, Memory: &extism.ManifestMemory{MaxPages: c.MemoryPages, MaxVarBytes: int64(c.MaxStateBytes), MaxHttpResponseBytes: 1}, AllowedHosts: []string{}, AllowedPaths: map[string]string{}}
	compiled, err := extism.NewCompiledPlugin(ctx, manifest, extism.PluginConfig{EnableWasi: true, RuntimeConfig: wazero.NewRuntimeConfig().WithMemoryLimitPages(c.MemoryPages).WithCloseOnContextDone(true)}, nil)
	if err != nil {
		return fail(code("MODULE_INVALID"))
	}
	defer compiled.Close(context.Background())
	load.Module = nil
	write(Response{Version: 2, ID: load.ID})
	var mu sync.Mutex
	calls := map[string]context.CancelFunc{}
	var jobs sync.WaitGroup
	for data := range frames {
		var req Request
		if decode(data, &req) != nil {
			return 2
		}
		switch req.Op {
		case "cancel":
			mu.Lock()
			fn := calls[req.Target]
			if fn != nil {
				fn()
			}
			mu.Unlock()
		case "close":
			cancel()
			mu.Lock()
			for _, fn := range calls {
				fn()
			}
			mu.Unlock()
			jobs.Wait()
			write(Response{Version: 2, ID: req.ID})
			return 0
		case "call":
			callCtx, stop := context.WithTimeout(ctx, duration(req.TimeoutMS))
			mu.Lock()
			calls[req.ID] = stop
			mu.Unlock()
			jobs.Add(1)
			go func(req Request) {
				defer jobs.Done()
				defer stop()
				defer func() { mu.Lock(); delete(calls, req.ID); mu.Unlock() }()
				result := execute(callCtx, compiled, req, c)
				write(result)
			}(req)
		default:
			return 2
		}
	}
	return 0
}

func execute(ctx context.Context, compiled *extism.CompiledPlugin, req Request, c Config) (result Response) {
	result = Response{Version: 2, ID: req.ID}
	defer func() {
		if recover() != nil {
			result = Response{Version: 2, ID: req.ID, Error: "GUEST_TRAP"}
		}
	}()
	plugin, err := compiled.Instance(ctx, extism.PluginInstanceConfig{ModuleConfig: wazero.NewModuleConfig().WithStdout(io.Discard).WithStderr(io.Discard)})
	if err != nil {
		result.Error = "GUEST_TRAP"
		return
	}
	defer plugin.Close(context.Background())
	state := json.RawMessage(req.State)
	if len(state) == 0 {
		state = json.RawMessage("null")
	}
	input, _ := json.Marshal(guestInput{Input: req.Input, State: state})
	exit, out, err := plugin.CallWithContext(ctx, req.Function, input)
	if ctx.Err() != nil {
		result.Error = "CALL_CANCELLED"
		if ctx.Err() == context.DeadlineExceeded {
			result.Error = "CALL_TIMEOUT"
		}
		return
	}
	if err != nil || exit != 0 {
		result.Error = "GUEST_TRAP"
		return
	}
	if len(out) > c.MaxOutputBytes*2+c.MaxStateBytes+1024 {
		result.Error = "OUTPUT_BUDGET"
		return
	}
	var value guestOutput
	if decode(out, &value) != nil || value.Output == nil || len(value.State) == 0 {
		result.Error = "OUTPUT_INVALID"
		return
	}
	if len(value.Output) > c.MaxOutputBytes {
		result.Error = "OUTPUT_BUDGET"
		return
	}
	if len(value.State) > c.MaxStateBytes {
		result.Error = "STATE_BUDGET"
		return
	}
	result.Output = value.Output
	result.State = value.State
	return
}
