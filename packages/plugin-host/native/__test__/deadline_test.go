package native_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	native "gateway.local/plugin-host"
)

func stoppedWorker(t *testing.T, p *client) []int {
	t.Helper()
	workers := p.request(native.Request{Op: "stats"}).Stats.Workers
	if len(workers) != 1 {
		t.Fatalf("want one owned worker, got %v", workers)
	}
	if err := syscall.Kill(workers[0], syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { syscall.Kill(workers[0], syscall.SIGCONT) })
	return workers
}

func boundedResponse(t *testing.T, p *client, ch <-chan native.Response, started time.Time, want string) {
	t.Helper()
	r := p.wait(ch)
	elapsed := time.Since(started)
	t.Logf("response=%s elapsed=%s", r.Error, elapsed)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("request exceeded its deadline and bounded cleanup allowance: %s", elapsed)
	}
	expectError(t, r, want)
}

func TestStoppedWorkerInputBackpressureDoesNotExtendCallDeadline(t *testing.T) {
	c := native.DefaultConfig()
	c.WriteMS = 1500
	c.MaxInputBytes = 1 << 20
	p := launch(t, c)
	p.load(fixture)
	workers := stoppedWorker(t, p)
	started := time.Now()
	_, ch := p.send(native.Request{Op: "call", Hash: fixtureHash, Function: "invoke", Input: bytes.Repeat([]byte{'x'}, 1<<20), TimeoutMS: 20})
	boundedResponse(t, p, ch, started, "CALL_TIMEOUT")
	waitGone(t, workers)
	p.load(fixture)
	if output(t, p.call(fixtureHash, map[string]any{"op": "echo", "value": map[string]any{"alive": true}}, nil))["alive"] != true {
		t.Fatal("reload did not recover from failed worker")
	}
	workers = p.request(native.Request{Op: "stats"}).Stats.Workers
	p.close()
	waitGone(t, workers)
}

func TestWorkerWriteLockContentionDoesNotExtendDeadlineOrCancel(t *testing.T) {
	for _, mode := range []string{"deadline", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			c := native.DefaultConfig()
			c.WriteMS = 1500
			c.CallMS = 3000
			c.MaxInputBytes = 1 << 20
			p := launch(t, c)
			p.load(fixture)
			workers := stoppedWorker(t, p)
			firstID, first := p.send(native.Request{Op: "call", Hash: fixtureHash, Function: "invoke", Input: bytes.Repeat([]byte{'x'}, 1<<20)})
			time.Sleep(30 * time.Millisecond)
			timeout := 20
			want := "CALL_TIMEOUT"
			if mode == "cancel" {
				timeout = 3000
				want = "CALL_CANCELLED"
			}
			started := time.Now()
			id, ch := p.send(native.Request{Op: "call", Hash: fixtureHash, Function: "loop", TimeoutMS: timeout})
			if mode == "cancel" {
				time.Sleep(20 * time.Millisecond)
				expectError(t, p.request(native.Request{Op: "cancel", Target: id}), "")
			}
			boundedResponse(t, p, ch, started, want)
			canceled := p.request(native.Request{Op: "cancel", Target: firstID})
			firstResponse := p.wait(first)
			if canceled.Error == "COMMAND_MISSING" {
				expectError(t, firstResponse, "WORKER_FAILED")
			} else {
				expectError(t, canceled, "")
				if firstResponse.Error != "CALL_CANCELLED" && firstResponse.Error != "WORKER_FAILED" {
					t.Fatalf("unexpected contending call result: %s", firstResponse.Error)
				}
			}
			waitGone(t, workers)
			p.close()
		})
	}
}

func TestBlockedCancelWriteDoesNotExtendLoopCancellationOrClose(t *testing.T) {
	for _, mode := range []string{"cancel", "close", "eof"} {
		t.Run(mode, func(t *testing.T) {
			c := native.DefaultConfig()
			c.WriteMS = 1500
			c.CallMS = 3000
			c.MaxInputBytes = 1 << 20
			p := launch(t, c)
			p.load(fixture)
			id, loop := p.send(native.Request{Op: "call", Hash: fixtureHash, Function: "loop"})
			time.Sleep(50 * time.Millisecond)
			workers := stoppedWorker(t, p)
			_, blocked := p.send(native.Request{Op: "call", Hash: fixtureHash, Function: "invoke", Input: bytes.Repeat([]byte{'x'}, 1<<20)})
			time.Sleep(30 * time.Millisecond)
			started := time.Now()
			switch mode {
			case "cancel":
				expectError(t, p.request(native.Request{Op: "cancel", Target: id}), "")
			case "close":
				p.send(native.Request{Op: "close"})
			case "eof":
				p.in.Close()
			}
			boundedResponse(t, p, loop, started, "CALL_CANCELLED")
			waitGone(t, workers)
			r := p.wait(blocked)
			if mode == "cancel" {
				expectError(t, r, "WORKER_FAILED")
				p.close()
			} else {
				expectError(t, r, "CALL_CANCELLED")
				select {
				case <-p.done:
				case <-time.After(3 * time.Second):
					t.Fatal("shutdown did not finish after cancellation")
				}
			}
		})
	}
}

func TestCompileInputBackpressureDoesNotExtendCompileDeadline(t *testing.T) {
	c := native.DefaultConfig()
	c.CompileMS = 100
	c.WriteMS = 1500
	gate := filepath.Join(t.TempDir(), "worker-gate")
	if err := os.WriteFile(gate, []byte("#!/bin/sh\nif [ \"$1\" = warm ]; then exit 0; fi\necho $$ > '"+gate+".pid'\nkill -STOP $$\nexec '"+executable+"' \"$@\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := exec.Command(gate, "warm").Run(); err != nil {
		t.Fatal(err)
	}
	s, err := native.New(c, gate)
	if err != nil {
		t.Fatal(err)
	}
	input, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	reader, output, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	defer reader.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, input, output); output.Close() }()
	t.Cleanup(func() {
		cancel()
		writer.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("compile server did not shut down")
		}
	})
	started := time.Now()
	if _, err := writer.Write(append(jsonBytes(native.Request{Version: 2, ID: "load", Op: "load", Hash: fixtureHash, Module: fixture}), '\n')); err != nil {
		t.Fatal(err)
	}
	var r native.Response
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		if json.Unmarshal(scanner.Bytes(), &r) != nil {
			t.Fatal("invalid compile response")
		}
		if r.ID == "load" {
			break
		}
	}
	if r.ID != "load" {
		t.Fatal("missing compile response")
	}
	elapsed := time.Since(started)
	t.Logf("response=%s elapsed=%s", r.Error, elapsed)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("compile deadline was extended by transport: %s", elapsed)
	}
	expectError(t, r, "COMPILE_TIMEOUT")
	data, err := os.ReadFile(gate + ".pid")
	var pid int
	if err != nil || json.Unmarshal(data, &pid) != nil || pid <= 0 {
		t.Fatalf("missing owned compile worker PID: read=%v data=%q", err, data)
	}
	waitGone(t, []int{pid})
	if _, err := writer.Write(append(jsonBytes(native.Request{Version: 2, ID: "stats", Op: "stats"}), '\n')); err != nil {
		t.Fatal(err)
	}
	if !scanner.Scan() || json.Unmarshal(scanner.Bytes(), &r) != nil || r.Stats == nil || r.Stats.Modules != 0 || len(r.Stats.Workers) != 0 {
		t.Fatal("failed compile retained its reservation or worker")
	}
}
