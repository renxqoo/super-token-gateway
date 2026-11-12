package native

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

type entry struct {
	hash   string
	size   int
	refs   int
	last   time.Time
	ready  chan struct{}
	worker *worker
	err    string
}
type task struct{ cancel context.CancelFunc }
type Server struct {
	config              Config
	executable          string
	mu                  sync.Mutex
	entries             map[string]*entry
	tasks               map[string]task
	cacheBytes          int
	pendingBytes        int
	active              int
	bufferedOutputBytes int
	rssBytes            uint64
	workerRSSBytes      uint64
	peakTotalRSSBytes   uint64
	slots               chan struct{}
	capacity            chan struct{}
	jobs                sync.WaitGroup
}

func New(c Config, executable string) (*Server, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if executable == "" {
		return nil, code("CONFIG_INVALID")
	}
	return &Server{config: c, executable: executable, entries: map[string]*entry{}, tasks: map[string]task{}, slots: make(chan struct{}, c.Slots), capacity: make(chan struct{}, c.Slots+c.Queue)}, nil
}

func (s *Server) snapshot() *Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	pids := []int{}
	for _, e := range s.entries {
		if e.worker != nil {
			pids = append(pids, e.worker.cmd.Process.Pid)
		}
	}
	return &Stats{Pending: len(s.tasks), PendingBytes: s.pendingBytes, BufferedOutputBytes: s.bufferedOutputBytes, RSSBytes: s.rssBytes, WorkerRSSBytes: s.workerRSSBytes, PeakTotalRSSBytes: s.peakTotalRSSBytes, Active: s.active, Modules: len(s.entries), CacheBytes: s.cacheBytes, Workers: pids, HeapBytes: m.HeapAlloc, HeapSysBytes: m.HeapSys, Goroutines: runtime.NumGoroutine(), Config: s.config}
}

func (s *Server) acquire(ctx context.Context, req Request) (*entry, error) {
retry:
	s.mu.Lock()
	if e := s.entries[req.Hash]; e != nil {
		e.refs++
		e.last = time.Now()
		s.mu.Unlock()
		select {
		case <-e.ready:
			if e.worker != nil {
				select {
				case <-e.worker.done:
					s.mu.Lock()
					if s.entries[e.hash] == e {
						delete(s.entries, e.hash)
						s.cacheBytes -= e.size
					}
					s.mu.Unlock()
					s.release(e)
					if req.Op == "load" {
						goto retry
					}
					return nil, code("MODULE_MISSING")
				default:
				}
			}
			if e.err != "" {
				s.release(e)
				return nil, code(e.err)
			}
			return e, nil
		case <-ctx.Done():
			s.release(e)
			return nil, ctx.Err()
		}
	}
	if req.Op != "load" {
		s.mu.Unlock()
		return nil, code("MODULE_MISSING")
	}
	for len(s.entries) >= s.config.MaxModules || s.cacheBytes+len(req.Module) > s.config.MaxCacheBytes {
		var oldest *entry
		for _, e := range s.entries {
			select {
			case <-e.ready:
				if e.refs == 0 && (oldest == nil || e.last.Before(oldest.last)) {
					oldest = e
				}
			default:
			}
		}
		if oldest == nil {
			s.mu.Unlock()
			return nil, code("CACHE_BUSY")
		}
		delete(s.entries, oldest.hash)
		s.cacheBytes -= oldest.size
		if oldest.worker != nil {
			oldest.worker.stop()
		}
	}
	e := &entry{hash: req.Hash, size: len(req.Module), refs: 1, last: time.Now(), ready: make(chan struct{})}
	s.entries[req.Hash] = e
	s.cacheBytes += e.size
	s.mu.Unlock()
	w, err := startWorker(s.executable, s.config)
	if err == nil {
		s.mu.Lock()
		e.worker = w
		s.mu.Unlock()
		compileCtx, cancel := context.WithTimeout(ctx, duration(s.config.CompileMS))
		response := w.request(compileCtx, req)
		if response.Error != "" {
			err = code(response.Error)
		}
		if compileCtx.Err() == context.DeadlineExceeded {
			err = code("COMPILE_TIMEOUT")
		}
		cancel()
	}
	s.mu.Lock()
	if err != nil {
		e.err = err.Error()
		delete(s.entries, e.hash)
		s.cacheBytes -= e.size
	}
	close(e.ready)
	s.mu.Unlock()
	if err != nil {
		if w != nil {
			w.stop()
		}
		return nil, err
	}
	return e, nil
}

func (s *Server) release(e *entry) { s.mu.Lock(); e.refs--; e.last = time.Now(); s.mu.Unlock() }

func (s *Server) handle(ctx context.Context, req Request) Response {
	result := Response{Version: 2, ID: req.ID}
	e, err := s.acquire(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			result.Error = "CALL_CANCELLED"
		} else {
			result.Error = err.Error()
		}
		return result
	}
	defer s.release(e)
	if req.Op == "load" {
		return result
	}
	callCtx, cancel := context.WithTimeout(ctx, duration(req.TimeoutMS))
	defer cancel()
	result = e.worker.request(callCtx, req)
	result.Version = 2
	result.ID = req.ID
	select {
	case <-e.worker.done:
		s.mu.Lock()
		if s.entries[e.hash] == e {
			delete(s.entries, e.hash)
			s.cacheBytes -= e.size
		}
		s.mu.Unlock()
	default:
	}
	return result
}

func (s *Server) validate(req Request) string {
	if req.Version != 2 {
		return "VERSION_UNSUPPORTED"
	}
	if !validID.MatchString(req.ID) {
		return "COMMAND_ID"
	}
	switch req.Op {
	case "load":
		sum := sha256.Sum256(req.Module)
		if hex.EncodeToString(sum[:]) != req.Hash {
			return "MODULE_HASH"
		}
		if len(req.Module) > s.config.MaxModuleBytes {
			return "MODULE_BUDGET"
		}
		if !validHash.MatchString(req.Hash) {
			return "MODULE_HASH"
		}
		if len(req.Module) < 8 {
			return "MODULE_INVALID"
		}
	case "call":
		if !validHash.MatchString(req.Hash) {
			return "MODULE_HASH"
		}
		if !validFunction.MatchString(req.Function) || req.Function == "_start" {
			return "FUNCTION_INVALID"
		}
		if len(req.Input) > s.config.MaxInputBytes {
			return "INPUT_BUDGET"
		}
		if len(req.State) > 0 && !json.Valid(req.State) {
			return "STATE_INVALID"
		}
		if len(req.State) > s.config.MaxStateBytes {
			return "STATE_BUDGET"
		}
		if req.TimeoutMS < 0 || req.TimeoutMS > s.config.CallMS {
			return "TIMEOUT_INVALID"
		}
	case "cancel":
		if !validID.MatchString(req.Target) {
			return "COMMAND_ID"
		}
	case "stats", "close", "drain":
	default:
		return "COMMAND_UNKNOWN"
	}
	return ""
}

func (s *Server) Serve(ctx context.Context, input io.ReadCloser, output *os.File) error {
	debug.SetMemoryLimit(s.config.GoMemoryBytes)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var writeMu sync.Mutex
	var failureMu sync.Mutex
	var failure error
	fail := func(err error) {
		failureMu.Lock()
		if failure == nil {
			failure = err
		}
		failureMu.Unlock()
		cancel()
		input.Close()
	}
	write := func(r Response) {
		writeMu.Lock()
		defer writeMu.Unlock()
		if output.SetWriteDeadline(time.Now().Add(duration(s.config.WriteMS))) != nil {
			fail(code("OUTPUT_FAILED"))
			return
		}
		if json.NewEncoder(output).Encode(r) != nil {
			fail(code("OUTPUT_FAILED"))
		}
	}
	monitorDone := make(chan struct{})
	go func() { defer close(monitorDone); s.monitor(ctx, fail) }()
	readDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			input.Close()
		case <-readDone:
		}
	}()
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), s.config.MaxFrameBytes)
	write(Response{Version: 2, Event: "ready"})
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				write(Response{Version: 2, Event: "heartbeat"})
			}
		}
	}()
	var closing *Request
	for scanner.Scan() {
		var req Request
		if decode(scanner.Bytes(), &req) != nil {
			write(Response{Version: 2, Error: "FRAME_INVALID"})
			continue
		}
		result := Response{Version: 2, ID: req.ID}
		if err := s.validate(req); err != "" {
			result.Error = err
			write(result)
			continue
		}
		s.mu.Lock()
		_, duplicate := s.tasks[req.ID]
		s.mu.Unlock()
		if duplicate {
			result.Error = "COMMAND_DUPLICATE"
			write(result)
			continue
		}
		switch req.Op {
		case "stats":
			result.Stats = s.snapshot()
			write(result)
		case "cancel":
			s.mu.Lock()
			t, ok := s.tasks[req.Target]
			if ok {
				t.cancel()
			}
			s.mu.Unlock()
			if !ok {
				result.Error = "COMMAND_MISSING"
			}
			write(result)
		case "close", "drain":
			closing = &req
		default:
			taskBytes := len(req.Module) + len(req.Input) + len(req.State) + 512
			s.mu.Lock()
			budgetOK := s.pendingBytes+taskBytes <= s.config.MaxPendingBytes
			if budgetOK {
				s.pendingBytes += taskBytes
			}
			s.mu.Unlock()
			if !budgetOK {
				result.Error = "PENDING_BUDGET"
				write(result)
				continue
			}
			select {
			case s.capacity <- struct{}{}:
				taskCtx, stop := context.WithCancel(ctx)
				if req.Op == "call" && req.TimeoutMS == 0 {
					req.TimeoutMS = s.config.CallMS
				}
				s.mu.Lock()
				s.tasks[req.ID] = task{cancel: stop}
				s.mu.Unlock()
				s.jobs.Add(1)
				go func(req Request) {
					defer s.jobs.Done()
					defer stop()
					var response Response
					select {
					case s.slots <- struct{}{}:
						s.mu.Lock()
						s.active++
						s.mu.Unlock()
						if taskCtx.Err() != nil {
							response = Response{Version: 2, ID: req.ID, Error: "CALL_CANCELLED"}
						} else {
							response = s.handle(taskCtx, req)
						}
						s.mu.Lock()
						s.active--
						s.mu.Unlock()
						<-s.slots
					case <-taskCtx.Done():
						response = Response{Version: 2, ID: req.ID, Error: "CALL_CANCELLED"}
					}
					responseBytes := len(response.Output) + len(response.State)
					s.mu.Lock()
					if s.bufferedOutputBytes+responseBytes > s.config.MaxBufferedOutputBytes {
						response.Output = nil
						response.State = nil
						response.Error = "OUTPUT_PRESSURE"
						responseBytes = 0
					}
					s.bufferedOutputBytes += responseBytes
					s.mu.Unlock()
					write(response)
					s.mu.Lock()
					s.bufferedOutputBytes -= responseBytes
					delete(s.tasks, req.ID)
					s.pendingBytes -= taskBytes
					s.mu.Unlock()
					<-s.capacity
				}(req)
			default:
				s.mu.Lock()
				s.pendingBytes -= taskBytes
				s.mu.Unlock()
				result.Error = "OVERLOADED"
				write(result)
			}
		}
		if closing != nil {
			break
		}
	}
	close(readDone)
	if scanner.Err() != nil && ctx.Err() == nil {
		write(Response{Version: 2, Error: "FRAME_TOO_LARGE"})
		fail(code("INPUT_FAILED"))
	}
	if closing == nil || closing.Op == "close" {
		cancel()
	}
	s.jobs.Wait()
	cancel()
	<-monitorDone
	<-heartbeatDone
	s.mu.Lock()
	entries := s.entries
	s.entries = map[string]*entry{}
	s.cacheBytes = 0
	s.mu.Unlock()
	for _, e := range entries {
		if e.worker != nil {
			e.worker.close()
		}
	}
	s.mu.Lock()
	s.workerRSSBytes = 0
	s.mu.Unlock()
	if closing != nil {
		write(Response{Version: 2, ID: closing.ID, Stats: s.snapshot()})
	}
	failureMu.Lock()
	defer failureMu.Unlock()
	return failure
}

func (s *Server) monitor(ctx context.Context, fail func(error)) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			workers := []*worker{}
			for _, e := range s.entries {
				if e.worker != nil {
					workers = append(workers, e.worker)
				}
			}
			s.mu.Unlock()
			parentRSS, rssErr := processRSS(ctx, os.Getpid())
			if rssErr != nil {
				if ctx.Err() != nil {
					return
				}
				for _, w := range workers {
					w.stopFor("PROCESS_MONITOR_FAILED")
				}
				fail(code("PROCESS_MONITOR_FAILED"))
				return
			}
			var total uint64
			for _, w := range workers {
				rss, err := processRSS(ctx, w.cmd.Process.Pid)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					select {
					case <-w.done:
					default:
						w.stopFor("WORKER_MONITOR_FAILED")
					}
					continue
				}
				if err == nil {
					total += rss
					if rss > s.config.MaxWorkerRSSBytes {
						w.stopFor("WORKER_BUDGET")
					}
				}
			}
			s.mu.Lock()
			s.rssBytes = parentRSS
			s.workerRSSBytes = total
			if total+parentRSS > s.peakTotalRSSBytes {
				s.peakTotalRSSBytes = total + parentRSS
			}
			s.mu.Unlock()
			if total+parentRSS > s.config.MaxTotalRSSBytes {
				for _, w := range workers {
					w.stopFor("PROCESS_BUDGET")
				}
				fail(code("PROCESS_BUDGET"))
				return
			}
		}
	}
}
