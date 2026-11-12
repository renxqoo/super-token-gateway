package native

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

type worker struct {
	reason  string
	cmd     *exec.Cmd
	in      io.WriteCloser
	mu      sync.Mutex
	writeMu chan struct{}
	pending map[string]chan Response
	done    chan struct{}
	stopped sync.Once
	config  Config
}

func startWorker(executable string, c Config) (*worker, error) {
	data, _ := json.Marshal(c)
	cmd := exec.Command(executable, "--worker", base64.StdEncoding.EncodeToString(data))
	cmd.Env = []string{"GOMEMLIMIT=" + strconv.FormatInt(c.GoMemoryBytes, 10), "GOMAXPROCS=2"}
	cmd.Stderr = io.Discard
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, code("WORKER_FAILED")
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		in.Close()
		return nil, code("WORKER_FAILED")
	}
	if cmd.Start() != nil {
		in.Close()
		out.Close()
		return nil, code("WORKER_FAILED")
	}
	w := &worker{cmd: cmd, in: in, pending: map[string]chan Response{}, done: make(chan struct{}), writeMu: make(chan struct{}, 1), config: c}
	go func() {
		scanner := bufio.NewScanner(out)
		scanner.Buffer(make([]byte, 4096), c.MaxFrameBytes)
		for scanner.Scan() {
			var r Response
			if decode(scanner.Bytes(), &r) != nil {
				break
			}
			w.mu.Lock()
			ch := w.pending[r.ID]
			w.mu.Unlock()
			if ch != nil {
				select {
				case ch <- r:
				default:
				}
			}
		}
		cmd.Process.Kill()
		cmd.Wait()
		out.Close()
		in.Close()
		close(w.done)
	}()
	return w, nil
}

func (w *worker) send(ctx context.Context, req Request) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, duration(w.config.WriteMS))
	defer cancel()
	data, _ := json.Marshal(req)
	data = append(data, '\n')
	select {
	case w.writeMu <- struct{}{}:
		defer func() { <-w.writeMu }()
	case <-ctx.Done():
		return false, ctx.Err()
	case <-w.done:
		return false, code("WORKER_FAILED")
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	f, ok := w.in.(*os.File)
	if !ok {
		return false, code("WORKER_FAILED")
	}
	deadline, _ := ctx.Deadline()
	if f.SetWriteDeadline(deadline) != nil {
		return false, code("WORKER_FAILED")
	}
	interrupted := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		f.SetWriteDeadline(time.Now())
		close(interrupted)
	})
	defer func() {
		if !stop() {
			<-interrupted
		}
	}()
	n, err := f.Write(data)
	if err != nil {
		if !time.Now().Before(deadline) {
			<-ctx.Done()
		}
		return n > 0, code("WORKER_FAILED")
	}
	return true, nil
}

func (w *worker) cancelCall(id string, result <-chan Response) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	type delivery struct {
		started bool
		err     error
	}
	sent := make(chan delivery, 1)
	go func() {
		started, err := w.send(ctx, Request{Version: 2, ID: "_cancel", Op: "cancel", Target: id})
		sent <- delivery{started: started, err: err}
	}()
	select {
	case <-result:
		cancel()
		d := <-sent
		if d.started && d.err != nil {
			w.stop()
		}
	case <-w.done:
		cancel()
		<-sent
	case d := <-sent:
		if d.err != nil {
			w.stop()
			return
		}
		select {
		case <-result:
		case <-w.done:
		case <-ctx.Done():
			w.stop()
		}
	case <-ctx.Done():
		<-sent
		w.stop()
	}
}

func contextError(ctx context.Context) string {
	if ctx.Err() == context.DeadlineExceeded {
		return "CALL_TIMEOUT"
	}
	return "CALL_CANCELLED"
}

func (w *worker) request(ctx context.Context, req Request) Response {
	ch := make(chan Response, 1)
	w.mu.Lock()
	w.pending[req.ID] = ch
	w.mu.Unlock()
	defer func() { w.mu.Lock(); delete(w.pending, req.ID); w.mu.Unlock() }()
	started, err := w.send(ctx, req)
	if err != nil {
		if started || ctx.Err() == nil {
			w.stop()
		}
		if ctx.Err() != nil {
			return Response{Error: contextError(ctx)}
		}
		return Response{Error: w.failure()}
	}
	select {
	case result := <-ch:
		return result
	case <-w.done:
		if ctx.Err() != nil {
			return Response{Error: contextError(ctx)}
		}
		return Response{Error: w.failure()}
	case <-ctx.Done():
		if req.Op == "call" {
			w.cancelCall(req.ID, ch)
		} else {
			w.stop()
		}
		return Response{Error: contextError(ctx)}
	}
}

func (w *worker) failure() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.reason != "" {
		return w.reason
	}
	return "WORKER_FAILED"
}
func (w *worker) stopFor(reason string) { w.mu.Lock(); w.reason = reason; w.mu.Unlock(); w.stop() }

func (w *worker) stop() { w.stopped.Do(func() { w.cmd.Process.Kill(); w.in.Close() }); <-w.done }
func (w *worker) close() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	w.request(ctx, Request{Version: 2, ID: "_close", Op: "close"})
	w.stop()
}

func processRSS(ctx context.Context, pid int) (uint64, error) {
	if data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/statm"); err == nil {
		f := strings.Fields(string(data))
		if len(f) > 1 {
			n, err := strconv.ParseUint(f[1], 10, 64)
			return n * uint64(os.Getpagesize()), err
		}
	}
	callCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	data, err := exec.CommandContext(callCtx, "/bin/ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	return n * 1024, err
}
