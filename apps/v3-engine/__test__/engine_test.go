package engine_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"super-token-gateway/v3-engine/internal/gateway"
	"super-token-gateway/v3-engine/internal/ledger"
	"super-token-gateway/v3-engine/internal/protocol"
	"super-token-gateway/v3-engine/internal/ratelimit"
)

func randUUID() ledger.UUID {
	var id ledger.UUID
	_, _ = rand.Read(id[:])
	return id
}

func testTariff() ledger.Tariff {
	return ledger.Tariff{
		ID:                1,
		CustomerReqNum:    100,
		CustomerReqDen:    1,
		SupplierInNum:     3,
		SupplierInDen:     1,
		SupplierCachedNum: 1,
		SupplierCachedDen: 1,
		SupplierOutNum:    7,
		SupplierOutDen:    1,
	}
}

func TestLedgerInvariantsIdempotencyAndCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	l, err := ledger.OpenLedger(ledger.LedgerOptions{
		WALDir:     dir,
		NumWALs:    4,
		Epoch:      1,
		SyncDisk:   true,
		VerifySigs: true,
	})
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	l.RegisterTariff(testTariff())
	if err := l.SeedAllSupplierCells(10000); err != nil {
		t.Fatalf("seed cells: %v", err)
	}
	if err := l.CreditWallet(7, 250, 50, false); err != nil {
		t.Fatalf("credit wallet: %v", err)
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	sess := randUUID()
	l.RegisterSession(sess, pub)

	reqID, attID, cmdID := randUUID(), randUUID(), randUUID()
	g1, err := l.AcquireGrant(ledger.GrantRequest{
		RequestID:    reqID,
		AttemptID:    attID,
		CommandID:    cmdID,
		WalletID:     7,
		TariffID:     1,
		CustomerHold: 100,
		SupplierHold: 100,
	})
	if err != nil || !g1.Fresh {
		t.Fatalf("grant 1 failed: %+v %v", g1, err)
	}
	gDup, err := l.AcquireGrant(ledger.GrantRequest{
		RequestID:    reqID,
		AttemptID:    attID,
		CommandID:    cmdID,
		WalletID:     7,
		TariffID:     1,
		CustomerHold: 100,
		SupplierHold: 100,
	})
	if err != nil || !gDup.Duplicate || gDup.Fresh {
		t.Fatalf("idempotent grant failed: %+v %v", gDup, err)
	}

	ev := ledger.SignedEvidence{
		SessionID: sess,
		RequestID: reqID,
		AttemptID: attID,
		EventID1:  randUUID(),
		EventID2:  randUUID(),
		WalletID:  7,
		Usage: protocol.Usage{
			InputTokens:       7,
			CachedInputTokens: 3,
			OutputTokens:      5,
			HasUsage:          true,
		},
	}
	ledger.SignEvidence(priv, &ev)
	s1, err := l.CommitTerminal(&ev)
	if err != nil || !s1.Fresh || s1.CustomerAtoms != 100 || s1.SupplierAtoms != 59 {
		t.Fatalf("settle 1 failed: %+v %v", s1, err)
	}
	sDup, err := l.CommitTerminal(&ev)
	if err != nil || !sDup.Duplicate || sDup.Fresh {
		t.Fatalf("idempotent settle failed: %+v %v", sDup, err)
	}

	badEv := ev
	badEv.RequestID = randUUID()
	badEv.Signature[0] ^= 0xff
	if _, err := l.CommitTerminal(&badEv); err != ledger.ErrEvidenceSigInvalid {
		t.Fatalf("expected invalid signature error, got %v", err)
	}

	abortReq, abortAtt := randUUID(), randUUID()
	if _, err := l.AcquireGrant(ledger.GrantRequest{
		RequestID:    abortReq,
		AttemptID:    abortAtt,
		CommandID:    randUUID(),
		WalletID:     7,
		TariffID:     1,
		CustomerHold: 150,
		SupplierHold: 100,
	}); err != nil {
		t.Fatalf("grant 2 failed: %v", err)
	}
	if _, err := l.AcquireGrant(ledger.GrantRequest{
		RequestID:    randUUID(),
		AttemptID:    randUUID(),
		CommandID:    randUUID(),
		WalletID:     7,
		TariffID:     1,
		CustomerHold: 100,
		SupplierHold: 100,
	}); err != ledger.ErrInsufficientCustomer {
		t.Fatalf("expected insufficient customer funds, got %v", err)
	}
	if err := l.AbortGrant(ledger.AbortRequest{RequestID: abortReq, AttemptID: abortAtt, WalletID: 7}); err != nil {
		t.Fatalf("abort failed: %v", err)
	}

	audit1 := l.AuditConservation()
	if !audit1.Balanced || audit1.ActiveHolds != 0 || audit1.TotalCustSpent != 100 || audit1.TotalSuppSpent != 59 {
		t.Fatalf("unexpected audit before crash: %+v", audit1)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	walFile := filepath.Join(dir, "shard-03.wal")
	f, err := os.OpenFile(walFile, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open wal for torn tail: %v", err)
	}
	_, _ = f.Write([]byte("STGW\x01\x03\x00\x00\x99\x00\x00\x00partial"))
	_ = f.Close()

	recovered, err := ledger.OpenLedger(ledger.LedgerOptions{
		WALDir:     dir,
		NumWALs:    4,
		Epoch:      2,
		SyncDisk:   true,
		VerifySigs: true,
	})
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer recovered.Close()
	audit2 := recovered.AuditConservation()
	if !audit2.Balanced || audit2.Terminals != 1 || audit2.ActiveHolds != 0 || audit2.TotalCustSpent != 100 || audit2.TotalSuppSpent != 59 {
		t.Fatalf("crash recovery mismatch: %+v", audit2)
	}
}

func TestQuorumFailoverAndEpochFencing(t *testing.T) {
	dir := t.TempDir()
	r1, err := ledger.StartReplicaNode("127.0.0.1:0", filepath.Join(dir, "r1.wal"), 1, true)
	if err != nil {
		t.Fatalf("r1: %v", err)
	}
	defer r1.Close()
	r2, err := ledger.StartReplicaNode("127.0.0.1:0", filepath.Join(dir, "r2.wal"), 1, true)
	if err != nil {
		t.Fatalf("r2: %v", err)
	}
	defer r2.Close()

	rep := ledger.NewQuorumReplicator([]string{r1.Addr(), r2.Addr()}, 1, 500*time.Millisecond)
	defer rep.Close()
	l, err := ledger.OpenLedger(ledger.LedgerOptions{
		WALDir:     filepath.Join(dir, "primary"),
		NumWALs:    2,
		Epoch:      1,
		SyncDisk:   true,
		Replicator: rep,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer l.Close()
	l.RegisterTariff(testTariff())
	if err := l.SeedAllSupplierCells(10000); err != nil {
		t.Fatalf("seed cells: %v", err)
	}
	if err := l.CreditWallet(1, 1000, 0, false); err != nil {
		t.Fatalf("credit: %v", err)
	}

	_ = r1.Close()
	if _, err := l.AcquireGrant(ledger.GrantRequest{
		RequestID:    randUUID(),
		AttemptID:    randUUID(),
		CommandID:    randUUID(),
		WalletID:     1,
		TariffID:     1,
		CustomerHold: 100,
		SupplierHold: 100,
	}); err != nil {
		t.Fatalf("expected 2-of-3 quorum to survive 1 replica loss, got %v", err)
	}

	r2.PromoteEpoch(2)
	if _, err := l.AcquireGrant(ledger.GrantRequest{
		RequestID:    randUUID(),
		AttemptID:    randUUID(),
		CommandID:    randUUID(),
		WalletID:     1,
		TariffID:     1,
		CustomerHold: 100,
		SupplierHold: 100,
	}); err != ledger.ErrFenced {
		t.Fatalf("expected stale primary to be fenced, got %v", err)
	}
}

func TestGatewayZeroOutputPersistenceAndSSEHold(t *testing.T) {
	secretPrompt := "TOP_SECRET_PROMPT_SENTINEL_987654321"
	secretReply := "TOP_SECRET_COMPLETION_SENTINEL_123456789"

	upMux := http.NewServeMux()
	upMux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if bytes.Contains(body, []byte(`"stream":true`)) {
			w.Header().Set("content-type", "text/event-stream")
			_, _ = w.Write([]byte(`data: {"id":"1","choices":[{"delta":{"content":"` + secretReply + `"}}]}` + "\n\n"))
			_, _ = w.Write([]byte(`data: {"id":"1","choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":3}}}` + "\n\n"))
			_, _ = w.Write([]byte("data: [DONE]\n\n"))
			return
		}
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","choices":[{"message":{"content":"` + secretReply + `"}}],"usage":{"prompt_tokens":10,"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":3}}}`))
	})
	upLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("up listen: %v", err)
	}
	upSrv := &http.Server{Handler: upMux}
	go func() { _ = upSrv.Serve(upLn) }()
	defer upSrv.Close()

	dir := t.TempDir()
	l, err := ledger.OpenLedger(ledger.LedgerOptions{
		WALDir:     dir,
		NumWALs:    2,
		Epoch:      1,
		SyncDisk:   true,
		VerifySigs: true,
	})
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	defer l.Close()
	l.RegisterTariff(testTariff())
	_ = l.SeedAllSupplierCells(100000)
	_ = l.CreditWallet(0, 100000, 0, false)

	srv, err := gateway.NewServer(gateway.ServerConfig{
		Pepper:               []byte("test-pepper"),
		CustomerHoldAtoms:    100,
		SupplierHoldAtoms:    100,
		AllowPrivateUpstream: true,
		Channels:             []gateway.ChannelConfig{{ID: "c1", BaseURL: "http://" + upLn.Addr().String(), Weight: 1}},
	}, l, ratelimit.New())
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	token := "gw_test_key_000000000000000000000000000000000"
	srv.RegisterKey(token, gateway.APIKeyRecord{KeyID: "k0", WalletID: 0, TariffID: 1})

	gwLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("gw listen: %v", err)
	}
	gwSrv := srv.HTTPServer("")
	go func() { _ = gwSrv.Serve(gwLn) }()
	defer gwSrv.Close()
	gwURL := "http://" + gwLn.Addr().String()

	var wg sync.WaitGroup
	for _, stream := range []bool{false, true} {
		wg.Add(1)
		go func(isStream bool) {
			defer wg.Done()
			payload, _ := json.Marshal(map[string]any{
				"model":                 "gpt-4o-mini",
				"stream":                isStream,
				"max_completion_tokens": 32,
				"messages":              []map[string]string{{"role": "user", "content": secretPrompt}},
			})
			req, _ := http.NewRequest(http.MethodPost, gwURL+"/v1/chat/completions", bytes.NewReader(payload))
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Errorf("request failed: %v", err)
				return
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != 200 || !bytes.Contains(body, []byte(secretReply)) {
				t.Errorf("unexpected response status=%d body=%s", resp.StatusCode, string(body))
			}
			if isStream && !bytes.Contains(body, []byte("[DONE]")) {
				t.Errorf("missing [DONE] in SSE")
			}
		}(stream)
	}
	wg.Wait()

	unmetered, _ := http.NewRequest(http.MethodPost, gwURL+"/v1/chat/completions", bytes.NewReader([]byte(`{"model":"gpt-4o-mini","web_search_options":{}}`)))
	unmetered.Header.Set("Authorization", "Bearer "+token)
	uResp, err := http.DefaultClient.Do(unmetered)
	if err != nil || uResp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for unmetered tool, got err=%v status=%v", err, uResp)
	}
	_ = uResp.Body.Close()

	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		raw, _ := os.ReadFile(filepath.Join(dir, e.Name()))
		if bytes.Contains(raw, []byte(secretPrompt)) || bytes.Contains(raw, []byte(secretReply)) {
			t.Fatalf("model content leaked into WAL file %s", e.Name())
		}
	}

	audit := l.AuditConservation()
	if !audit.Balanced || audit.Terminals != 2 || audit.ActiveHolds != 0 || audit.TotalCustSpent != 200 || audit.TotalSuppSpent != 118 {
		t.Fatalf("unexpected audit: %+v", audit)
	}
	claimed := l.ClaimEvents(10, time.Minute)
	if len(claimed) != 4 {
		t.Fatalf("expected 4 outbox events, got %d", len(claimed))
	}
	seqs := make([]uint64, len(claimed))
	for i, c := range claimed {
		seqs[i] = c.Seq
	}
	if acked := l.AckEvents(seqs); acked != 4 {
		t.Fatalf("expected 4 acked, got %d", acked)
	}
}

func TestProtocolSSEFramerAndLimiter(t *testing.T) {
	framer := protocol.NewSSEFramer(64 << 10)
	stream := "data: {\"choices\":[{\"delta\":{\"content\":\"hel\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"lo\"}}],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"prompt_tokens_details\":{\"cached_tokens\":3}}}\n\ndata: [DONE]\n\n"
	var emitted []byte
	for i := 0; i < len(stream); i += 7 {
		end := min(i+7, len(stream))
		out, _, err := framer.Feed([]byte(stream[i:end]))
		if err != nil {
			t.Fatalf("feed failed: %v", err)
		}
		emitted = append(emitted, out...)
	}
	if !framer.Done() || string(framer.HeldDone()) != "data: [DONE]\n\n" {
		t.Fatalf("expected held [DONE], got done=%v held=%q", framer.Done(), string(framer.HeldDone()))
	}
	if bytes.Contains(emitted, []byte("[DONE]")) {
		t.Fatalf("emitted premature [DONE]")
	}
	u := framer.Usage()
	if !u.HasUsage || u.InputTokens != 7 || u.CachedInputTokens != 3 || u.OutputTokens != 5 {
		t.Fatalf("unexpected usage: %+v", u)
	}
	if _, _, err := framer.Feed([]byte("data: extra\n\n")); err != protocol.ErrTrailingAfterDone {
		t.Fatalf("expected trailing error, got %v", err)
	}

	lim := ratelimit.New()
	rel1, ok := lim.Enter("k1", 2, 1, 10, 100)
	if !ok {
		t.Fatalf("expected first enter ok")
	}
	if _, ok := lim.Enter("k1", 2, 1, 10, 100); ok {
		t.Fatalf("expected concurrency rejection")
	}
	rel1()
	rel2, ok := lim.Enter("k1", 2, 1, 10, 100)
	if !ok {
		t.Fatalf("expected second enter ok after release")
	}
	rel2()
	if _, ok := lim.Enter("k1", 2, 1, 10, 100); ok {
		t.Fatalf("expected rpm rejection")
	}

	for i := 0; i < 3; i++ {
		lim.ObserveChannel("ch1", true, 100, 30*time.Millisecond)
	}
	if _, ok := lim.AllowChannel("ch1"); ok {
		t.Fatalf("expected breaker open")
	}
	time.Sleep(40 * time.Millisecond)
	half, ok := lim.AllowChannel("ch1")
	if !ok || !half {
		t.Fatalf("expected single half-open probe")
	}
	if _, ok := lim.AllowChannel("ch1"); ok {
		t.Fatalf("expected second concurrent probe rejected")
	}
	lim.ObserveChannel("ch1", false, 10, 30*time.Millisecond)
	if half, ok := lim.AllowChannel("ch1"); !ok || half {
		t.Fatalf("expected breaker closed after successful probe")
	}
}
