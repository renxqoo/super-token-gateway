package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"super-token-gateway/v3-engine/internal/gateway"
	"super-token-gateway/v3-engine/internal/ledger"
	"super-token-gateway/v3-engine/internal/ratelimit"
)

func startMockUpstream(h2c bool, streamDelay time.Duration) (string, func(), *atomic.Uint64) {
	var calls atomic.Uint64
	jsonResp := []byte(`{"id":"chatcmpl-v3","object":"chat.completion","created":1732100000,"model":"gpt-4o-mini","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15,"prompt_tokens_details":{"cached_tokens":3}}}`)
	sseChunk1 := []byte("data: {\"id\":\"chatcmpl-v3\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":null}]}\n\n")
	sseChunk2 := []byte("data: {\"id\":\"chatcmpl-v3\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"total_tokens\":15,\"prompt_tokens_details\":{\"cached_tokens\":3}}}\n\n")
	sseDone := []byte("data: [DONE]\n\n")

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		if bytes.Contains(body, []byte(`"stream":true`)) {
			w.Header().Set("content-type", "text/event-stream; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			fl, _ := w.(http.Flusher)
			_, _ = w.Write(sseChunk1)
			if fl != nil {
				fl.Flush()
			}
			if streamDelay > 0 {
				select {
				case <-time.After(streamDelay):
				case <-r.Context().Done():
					return
				}
			}
			_, _ = w.Write(sseChunk2)
			_, _ = w.Write(sseDone)
			if fl != nil {
				fl.Flush()
			}
			return
		}
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(jsonResp)
	})

	var protos http.Protocols
	protos.SetHTTP1(true)
	if h2c {
		protos.SetUnencryptedHTTP2(true)
	}
	srv := &http.Server{
		Handler:   mux,
		Protocols: &protos,
		HTTP2: &http.HTTP2Config{
			MaxConcurrentStreams:          65536,
			MaxReadFrameSize:              1 << 20,
			MaxReceiveBufferPerConnection: 4 << 20,
			MaxReceiveBufferPerStream:     256 << 10,
		},
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	go func() { _ = srv.Serve(ln) }()
	return "http://" + ln.Addr().String(), func() { _ = srv.Close() }, &calls
}

func cpuUsageSeconds() float64 {
	var r syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &r); err != nil {
		return 0
	}
	return float64(r.Utime.Sec) + float64(r.Utime.Usec)/1e6 + float64(r.Stime.Sec) + float64(r.Stime.Usec)/1e6
}

func main() {
	mode := flag.String("mode", "json", "json or sse")
	duration := flag.Duration("duration", 10*time.Second, "benchmark duration")
	concurrency := flag.Int("concurrency", 256, "concurrent workers")
	wallets := flag.Int("wallets", 1000, "wallet count")
	walShards := flag.Int("wal-shards", 1, "parallel wal shards")
	batchWait := flag.Duration("batch-wait", 0, "wal group commit wait")
	syncDisk := flag.Bool("sync-disk", true, "fsync wal on disk")
	quorum := flag.Bool("quorum", true, "enable 3-node 2-of-3 quorum replication")
	killReplicaAt := flag.Duration("kill-replica-at", 0, "kill replica 1 at elapsed offset")
	streamDelay := flag.Duration("stream-delay", 0, "sse stream duration")
	h2c := flag.Bool("h2c", true, "use HTTP/2 h2c")
	clients := flag.Int("clients", 8, "number of distinct HTTP/2 client transports")
	flag.Parse()

	tmpDir, err := os.MkdirTemp("", "v3-bench-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	upURL, stopUp, upCalls := startMockUpstream(*h2c, *streamDelay)
	defer stopUp()

	var rep *ledger.QuorumReplicator
	var r1, r2 *ledger.ReplicaNode
	if *quorum {
		r1, err = ledger.StartReplicaNode("127.0.0.1:0", filepath.Join(tmpDir, "rep1.wal"), 1, *syncDisk)
		if err != nil {
			panic(err)
		}
		defer r1.Close()
		r2, err = ledger.StartReplicaNode("127.0.0.1:0", filepath.Join(tmpDir, "rep2.wal"), 1, *syncDisk)
		if err != nil {
			panic(err)
		}
		defer r2.Close()
		rep = ledger.NewQuorumReplicator([]string{r1.Addr(), r2.Addr()}, 1, 2*time.Second)
		defer rep.Close()
	}

	walDir := filepath.Join(tmpDir, "primary")
	l, err := ledger.OpenLedger(ledger.LedgerOptions{
		WALDir:     walDir,
		NumWALs:    *walShards,
		Epoch:      1,
		SyncDisk:   *syncDisk,
		MaxBatch:   4096,
		BatchWait:  *batchWait,
		Replicator: rep,
		VerifySigs: true,
	})
	if err != nil {
		panic(err)
	}
	l.RegisterTariff(ledger.Tariff{
		ID:                1,
		CustomerReqNum:    100,
		CustomerReqDen:    1,
		SupplierInNum:     3,
		SupplierInDen:     1,
		SupplierCachedNum: 1,
		SupplierCachedDen: 1,
		SupplierOutNum:    7,
		SupplierOutDen:    1,
	})
	if err := l.SeedAllSupplierCells(1_000_000_000_000); err != nil {
		panic(err)
	}

	lim := ratelimit.New()
	srv, err := gateway.NewServer(gateway.ServerConfig{
		Pepper:               []byte("bench-pepper-32bytes-secret-key!"),
		MaxBodyBytes:         1 << 20,
		MaxResponseBytes:     1 << 20,
		MaxInFlight:          int64(*concurrency) * 4,
		DefaultOutputBound:   64,
		CustomerHoldAtoms:    100,
		SupplierHoldAtoms:    100,
		AllowPrivateUpstream: true,
		UpstreamH2C:          *h2c,
		UpstreamTimeout:      *streamDelay + 30*time.Second,
		Channels:             []gateway.ChannelConfig{{ID: "mock", BaseURL: upURL, Weight: 1}},
	}, l, lim)
	if err != nil {
		panic(err)
	}

	keys := make([]string, *wallets)
	for i := 0; i < *wallets; i++ {
		if err := l.CreditWallet(uint32(i), 1_000_000_000, 0, false); err != nil {
			panic(err)
		}
		k := fmt.Sprintf("gw_bench_%06d_0000000000000000000000000000", i)
		keys[i] = k
		srv.RegisterKey(k, gateway.APIKeyRecord{
			KeyID:    fmt.Sprintf("k-%d", i),
			WalletID: uint32(i),
			TariffID: 1,
		})
	}

	httpSrv := srv.HTTPServer("127.0.0.1:0")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	go func() { _ = httpSrv.Serve(ln) }()
	defer httpSrv.Close()
	gwURL := "http://" + ln.Addr().String()

	httpClients := make([]*http.Client, *clients)
	for i := range httpClients {
		tr := &http.Transport{
			MaxIdleConns:        4096,
			MaxIdleConnsPerHost: 2048,
			DisableCompression:  true,
			HTTP2: &http.HTTP2Config{
				MaxConcurrentStreams:          16384,
				MaxReadFrameSize:              1 << 20,
				MaxReceiveBufferPerConnection: 4 << 20,
				MaxReceiveBufferPerStream:     256 << 10,
			},
		}
		var protos http.Protocols
		if *h2c {
			protos.SetUnencryptedHTTP2(true)
		} else {
			protos.SetHTTP1(true)
		}
		tr.Protocols = &protos
		httpClients[i] = &http.Client{Transport: tr}
	}

	payload := []byte(`{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"max_completion_tokens":32}`)
	if *mode == "sse" {
		payload = []byte(`{"model":"gpt-4o-mini","stream":true,"messages":[{"role":"user","content":"hi"}],"max_completion_tokens":32}`)
	}

	stopAck := make(chan struct{})
	var ackWG sync.WaitGroup
	var ackedTotal atomic.Uint64
	for c := 0; c < 4; c++ {
		ackWG.Add(1)
		go func(idx int) {
			defer ackWG.Done()
			cl := httpClients[idx%len(httpClients)]
			claimBody := []byte(`{"limit":1024,"lease_ms":30000}`)
			for {
				resp, err := cl.Post(gwURL+"/v1/events/claim", "application/json", bytes.NewReader(claimBody))
				if err == nil {
					var res struct {
						Events []struct {
							Seq uint64 `json:"seq"`
						} `json:"events"`
					}
					_ = json.NewDecoder(resp.Body).Decode(&res)
					_ = resp.Body.Close()
					if len(res.Events) > 0 {
						seqs := make([]uint64, len(res.Events))
						for i, e := range res.Events {
							seqs[i] = e.Seq
						}
						ab, _ := json.Marshal(map[string]any{"seqs": seqs})
						ar, aerr := cl.Post(gwURL+"/v1/events/ack", "application/json", bytes.NewReader(ab))
						if aerr == nil {
							var ares struct {
								Acked int `json:"acked"`
							}
							_ = json.NewDecoder(ar.Body).Decode(&ares)
							_ = ar.Body.Close()
							ackedTotal.Add(uint64(ares.Acked))
						}
						continue
					}
				}
				select {
				case <-stopAck:
					return
				case <-time.After(2 * time.Millisecond):
				}
			}
		}(c)
	}

	replicaKilled := false
	if *quorum && *killReplicaAt > 0 {
		go func() {
			time.Sleep(*killReplicaAt)
			_ = r1.Close()
			replicaKilled = true
		}()
	}

	for _, cl := range httpClients {
		if r, err := cl.Get(gwURL + "/livez"); err == nil {
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
		}
	}
	warmReq, _ := http.NewRequest(http.MethodPost, gwURL+"/v1/chat/completions", bytes.NewReader([]byte(`{"model":"gpt-4o-mini","messages":[{"role":"user","content":"warm"}],"max_completion_tokens":16}`)))
	warmReq.Header.Set("Authorization", "Bearer "+keys[0])
	warmReq.Header.Set("Content-Type", "application/json")
	if wr, err := httpClients[0].Do(warmReq); err == nil {
		_, _ = io.Copy(io.Discard, wr.Body)
		_ = wr.Body.Close()
	}

	stopAt := time.Now().Add(*duration)
	reqCtx := context.Background()

	var okCount, errCount atomic.Uint64
	var errMu sync.Mutex
	errSamples := make(map[string]int)
	recordErr := func(key string) {
		errCount.Add(1)
		errMu.Lock()
		errSamples[key]++
		errMu.Unlock()
	}
	latCh := make(chan []int64, *concurrency)
	cpuBefore := cpuUsageSeconds()
	start := time.Now()

	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			cl := httpClients[workerID%len(httpClients)]
			localLat := make([]int64, 0, 2048)
			i := workerID
			for time.Now().Before(stopAt) {
				key := keys[i%*wallets]
				i++
				t0 := time.Now()
				req, _ := http.NewRequestWithContext(reqCtx, http.MethodPost, gwURL+"/v1/chat/completions", bytes.NewReader(payload))
				req.Header.Set("Authorization", "Bearer "+key)
				req.Header.Set("Content-Type", "application/json")
				resp, err := cl.Do(req)
				if err != nil {
					recordErr("transport:" + err.Error())
					continue
				}
				body, rerr := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if rerr != nil {
					recordErr("read:" + rerr.Error())
					continue
				}
				if resp.StatusCode != 200 || (*mode == "sse" && !bytes.Contains(body, []byte("[DONE]"))) || (*mode == "json" && !bytes.Contains(body, []byte(`"chatcmpl-v3"`))) {
					recordErr(fmt.Sprintf("status_%d", resp.StatusCode))
					continue
				}
				okCount.Add(1)
				localLat = append(localLat, time.Since(t0).Microseconds())
			}
			latCh <- localLat
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	cpuUsed := cpuUsageSeconds() - cpuBefore

	drainStart := time.Now()
	for time.Since(drainStart) < 5*time.Second {
		if srv.InFlight() == 0 && ackedTotal.Load() >= okCount.Load()*2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stopAck)
	ackWG.Wait()
	close(latCh)
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = httpSrv.Shutdown(shutCtx)
	shutCancel()

	allLat := make([]int64, 0, okCount.Load())
	for slice := range latCh {
		allLat = append(allLat, slice...)
	}
	sort.Slice(allLat, func(i, j int) bool { return allLat[i] < allLat[j] })
	pct := func(p float64) float64 {
		if len(allLat) == 0 {
			return 0
		}
		idx := int(float64(len(allLat)-1) * p)
		return float64(allLat[idx]) / 1000.0
	}

	auditBefore := l.AuditConservation()
	walStats := l.AggregateWALStats()
	_ = l.Close()

	recovered, err := ledger.OpenLedger(ledger.LedgerOptions{
		WALDir:   walDir,
		NumWALs:  *walShards,
		Epoch:    2,
		SyncDisk: false,
	})
	if err != nil {
		panic(err)
	}
	auditRecovered := recovered.AuditConservation()
	_ = recovered.Close()

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	okN := okCount.Load()
	rps := float64(okN) / elapsed.Seconds()
	walPerReq := 0.0
	cpuMsPerReq := 0.0
	if okN > 0 {
		walPerReq = float64(walStats.WrittenBytes) / float64(okN)
		cpuMsPerReq = (cpuUsed * 1000.0) / float64(okN)
	}
	report := map[string]any{
		"mode":                  *mode,
		"h2c":                   *h2c,
		"sync_disk":             *syncDisk,
		"quorum_2of3":           *quorum,
		"replica1_killed":       replicaKilled,
		"concurrency":           *concurrency,
		"stream_delay_ms":       streamDelay.Milliseconds(),
		"elapsed_sec":           elapsed.Seconds(),
		"ok_requests":           okN,
		"errors":                errCount.Load(),
		"error_breakdown":       errSamples,
		"upstream_calls":        upCalls.Load(),
		"rps":                   rps,
		"p50_ms":                pct(0.50),
		"p95_ms":                pct(0.95),
		"p99_ms":                pct(0.99),
		"p999_ms":               pct(0.999),
		"cpu_ms_per_req":        cpuMsPerReq,
		"cpu_cores_used":        cpuUsed / elapsed.Seconds(),
		"wal_bytes_per_req":     walPerReq,
		"wal_sync_batches":      walStats.SyncBatches,
		"wal_max_batch_recs":    walStats.MaxBatchRecs,
		"heap_alloc_mb":         float64(mem.HeapAlloc) / (1024 * 1024),
		"external_acked_events": ackedTotal.Load(),
		"audit_balanced":        auditBefore.Balanced,
		"audit_terminals":       auditBefore.Terminals,
		"audit_active_holds":    auditBefore.ActiveHolds,
		"audit_cust_spent":      auditBefore.TotalCustSpent,
		"audit_supp_spent":      auditBefore.TotalSuppSpent,
		"crash_recovery_exact":  auditRecovered.Balanced && auditRecovered.Terminals == auditBefore.Terminals && auditRecovered.TotalCustSpent == auditBefore.TotalCustSpent && auditRecovered.TotalSuppSpent == auditBefore.TotalSuppSpent && auditRecovered.ActiveHolds == auditBefore.ActiveHolds,
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(report)
}
