package gateway

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"super-token-gateway/v3-engine/internal/ledger"
	"super-token-gateway/v3-engine/internal/protocol"
	"super-token-gateway/v3-engine/internal/ratelimit"
)

type APIKeyRecord struct {
	KeyID          string
	WalletID       uint32
	TariffID       uint32
	RPM            int64
	TPM            int64
	MaxConcurrency int64
	AllowedCIDRs   []netip.Prefix
}

type ChannelConfig struct {
	ID       string
	BaseURL  string
	APIKey   string
	Priority int
	Weight   int
}

type ServerConfig struct {
	Pepper               []byte
	MaxBodyBytes         int
	MaxResponseBytes     int64
	MaxInFlight          int64
	DefaultOutputBound   int64
	CustomerHoldAtoms    int64
	SupplierHoldAtoms    int64
	AllowPrivateUpstream bool
	UpstreamH2C          bool
	UpstreamTimeout      time.Duration
	Channels             []ChannelConfig
}

type Server struct {
	cfg        ServerConfig
	ledger     *ledger.Ledger
	limiter    *ratelimit.Limiter
	sessionID  ledger.UUID
	privKey    ed25519.PrivateKey
	pubKey     ed25519.PublicKey
	client     *http.Client
	keysMu     sync.RWMutex
	keys       map[[32]byte]APIKeyRecord
	hmacPool   sync.Pool
	inFlight   atomic.Int64
	accepting  atomic.Bool
	httpOK     atomic.Uint64
	httpErr    atomic.Uint64
	seqCounter atomic.Uint64
	idPrefix   [8]byte
}

func NewServer(cfg ServerConfig, l *ledger.Ledger, lim *ratelimit.Limiter) (*Server, error) {
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = 16 << 20
	}
	if cfg.MaxResponseBytes <= 0 {
		cfg.MaxResponseBytes = 32 << 20
	}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = 200000
	}
	if cfg.DefaultOutputBound <= 0 {
		cfg.DefaultOutputBound = 4096
	}
	if cfg.UpstreamTimeout <= 0 {
		cfg.UpstreamTimeout = 180 * time.Second
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	var sessID ledger.UUID
	_, _ = rand.Read(sessID[:])
	l.RegisterSession(sessID, pub)

	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	tr := &http.Transport{
		Proxy:                 nil,
		MaxIdleConns:          4096,
		MaxIdleConnsPerHost:   2048,
		MaxConnsPerHost:       0,
		IdleConnTimeout:       60 * time.Second,
		DisableCompression:    true,
		ForceAttemptHTTP2:     true,
		ResponseHeaderTimeout: 30 * time.Second,
		HTTP2: &http.HTTP2Config{
			MaxConcurrentStreams:          65536,
			MaxReadFrameSize:              1 << 20,
			MaxReceiveBufferPerConnection: 4 << 20,
			MaxReceiveBufferPerStream:     256 << 10,
		},
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return safeDial(ctx, dialer, network, addr, cfg.AllowPrivateUpstream)
		},
	}
	if cfg.UpstreamH2C {
		var protos http.Protocols
		protos.SetUnencryptedHTTP2(true)
		tr.Protocols = &protos
	} else {
		var protos http.Protocols
		protos.SetHTTP1(true)
		protos.SetHTTP2(true)
		tr.Protocols = &protos
	}

	var prefix [8]byte
	_, _ = rand.Read(prefix[:])
	s := &Server{
		cfg:       cfg,
		ledger:    l,
		limiter:   lim,
		sessionID: sessID,
		privKey:   priv,
		pubKey:    pub,
		client:    &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		keys:      make(map[[32]byte]APIKeyRecord, 1024),
		idPrefix:  prefix,
	}
	pepperCopy := append([]byte(nil), cfg.Pepper...)
	s.hmacPool.New = func() any {
		return hmac.New(sha256.New, pepperCopy)
	}
	s.accepting.Store(true)
	return s, nil
}

func (s *Server) nextUUID() ledger.UUID {
	var id ledger.UUID
	copy(id[0:8], s.idPrefix[:])
	n := s.seqCounter.Add(1)
	id[8] = byte(n >> 56)
	id[9] = byte(n >> 48)
	id[10] = byte(n >> 40)
	id[11] = byte(n >> 32)
	id[12] = byte(n >> 24)
	id[13] = byte(n >> 16)
	id[14] = byte(n >> 8)
	id[15] = byte(n)
	return id
}

func (s *Server) RegisterKey(rawToken string, rec APIKeyRecord) {
	mac := hmac.New(sha256.New, s.cfg.Pepper)
	_, _ = mac.Write([]byte(rawToken))
	var digest [32]byte
	copy(digest[:], mac.Sum(nil))
	s.keysMu.Lock()
	s.keys[digest] = rec
	s.keysMu.Unlock()
}

func (s *Server) SetAccepting(ok bool) {
	s.accepting.Store(ok)
}

func (s *Server) InFlight() int64 {
	return s.inFlight.Load()
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", s.handleLivez)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.HandleFunc("GET /admin/stats", s.handleStats)
	mux.HandleFunc("POST /v1/chat/completions", s.handleChat)
	mux.HandleFunc("GET /v1/wallets", s.handleWallet)
	mux.HandleFunc("POST /v1/events/claim", s.handleClaim)
	mux.HandleFunc("POST /v1/events/ack", s.handleAck)
	return mux
}

func (s *Server) HTTPServer(addr string) *http.Server {
	var protos http.Protocols
	protos.SetHTTP1(true)
	protos.SetHTTP2(true)
	protos.SetUnencryptedHTTP2(true)
	return &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		Protocols:         &protos,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
		HTTP2: &http.HTTP2Config{
			MaxConcurrentStreams:          65536,
			MaxReadFrameSize:              1 << 20,
			MaxReceiveBufferPerConnection: 4 << 20,
			MaxReceiveBufferPerStream:     256 << 10,
		},
	}
}

func (s *Server) Close() {
	s.accepting.Store(false)
	s.client.CloseIdleConnections()
}

func (s *Server) handleLivez(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("content-type", "application/json")
	_, _ = w.Write([]byte(`{"status":"alive"}`))
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("content-type", "application/json")
	if !s.accepting.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"draining"}`))
		return
	}
	_, _ = w.Write([]byte(`{"status":"ready"}`))
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("content-type", "application/json")
	audit := s.ledger.AuditConservation()
	wal := s.ledger.AggregateWALStats()
	_ = json.NewEncoder(w).Encode(map[string]any{
		"in_flight": s.inFlight.Load(),
		"http_ok":   s.httpOK.Load(),
		"http_err":  s.httpErr.Load(),
		"audit":     audit,
		"wal":       wal,
	})
}

func (s *Server) authenticate(r *http.Request) (APIKeyRecord, int, string) {
	auth := r.Header.Get("Authorization")
	var token string
	if strings.HasPrefix(auth, "Bearer ") {
		token = auth[7:]
	} else if k := r.Header.Get("x-api-key"); k != "" {
		token = k
	}
	if len(token) < 8 {
		return APIKeyRecord{}, http.StatusUnauthorized, "unauthenticated"
	}
	mac := s.hmacPool.Get().(hash.Hash)
	mac.Reset()
	_, _ = mac.Write([]byte(token))
	var digest [32]byte
	mac.Sum(digest[:0])
	s.hmacPool.Put(mac)
	s.keysMu.RLock()
	rec, ok := s.keys[digest]
	s.keysMu.RUnlock()
	if !ok {
		return APIKeyRecord{}, http.StatusUnauthorized, "unauthenticated"
	}
	if len(rec.AllowedCIDRs) > 0 {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		addr, err := netip.ParseAddr(host)
		if err != nil {
			return APIKeyRecord{}, http.StatusForbidden, "ip_denied"
		}
		addr = addr.Unmap()
		allowed := false
		for _, pfx := range rec.AllowedCIDRs {
			if pfx.Contains(addr) {
				allowed = true
				break
			}
		}
		if !allowed {
			return APIKeyRecord{}, http.StatusForbidden, "ip_denied"
		}
	}
	return rec, 0, ""
}

func (s *Server) handleWallet(w http.ResponseWriter, r *http.Request) {
	rec, status, code := s.authenticate(r)
	if status != 0 {
		writeErr(w, status, code, "")
		return
	}
	wal, ok := s.ledger.WalletSnapshot(rec.WalletID)
	if !ok {
		writeErr(w, http.StatusNotFound, "wallet_not_found", "")
		return
	}
	w.Header().Set("content-type", "application/json")
	_ = json.NewEncoder(w).Encode(wal)
}

type claimReq struct {
	Limit   int `json:"limit"`
	LeaseMs int `json:"lease_ms"`
}

func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request) {
	var req claimReq
	_ = json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req)
	if req.Limit <= 0 || req.Limit > 2048 {
		req.Limit = 256
	}
	lease := 30 * time.Second
	if req.LeaseMs > 0 {
		lease = time.Duration(req.LeaseMs) * time.Millisecond
	}
	events := s.ledger.ClaimEvents(req.Limit, lease)
	type dto struct {
		Seq           uint64 `json:"seq"`
		RequestID     string `json:"request_id"`
		Revision      uint8  `json:"revision"`
		CustomerAtoms int64  `json:"customer_atoms"`
		SupplierAtoms int64  `json:"supplier_atoms"`
	}
	out := make([]dto, len(events))
	for i, ev := range events {
		out[i] = dto{
			Seq:           ev.Seq,
			RequestID:     hex.EncodeToString(ev.RequestID[:]),
			Revision:      ev.Revision,
			CustomerAtoms: ev.CustomerAtoms,
			SupplierAtoms: ev.SupplierAtoms,
		}
	}
	w.Header().Set("content-type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"events": out})
}

type ackReq struct {
	Seqs []uint64 `json:"seqs"`
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	var req ackReq
	if err := json.NewDecoder(io.LimitReader(r.Body, 256<<10)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_json", "")
		return
	}
	acked := s.ledger.AckEvents(req.Seqs)
	w.Header().Set("content-type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"acked": acked})
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	reqID := s.nextUUID()
	reqHex := hex.EncodeToString(reqID[:])
	if !s.accepting.Load() {
		s.httpErr.Add(1)
		writeErr(w, http.StatusServiceUnavailable, "draining", reqHex)
		return
	}
	cur := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)
	if cur > s.cfg.MaxInFlight {
		s.httpErr.Add(1)
		writeErr(w, http.StatusServiceUnavailable, "overloaded", reqHex)
		return
	}

	keyRec, status, code := s.authenticate(r)
	if status != 0 {
		s.httpErr.Add(1)
		writeErr(w, status, code, reqHex)
		return
	}
	if r.ContentLength > int64(s.cfg.MaxBodyBytes) {
		s.httpErr.Add(1)
		writeErr(w, http.StatusRequestEntityTooLarge, "body_too_large", reqHex)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, int64(s.cfg.MaxBodyBytes)+1))
	if err != nil {
		s.httpErr.Add(1)
		writeErr(w, http.StatusBadRequest, "body_read_failed", reqHex)
		return
	}
	if len(body) > s.cfg.MaxBodyBytes {
		s.httpErr.Add(1)
		writeErr(w, http.StatusRequestEntityTooLarge, "body_too_large", reqHex)
		return
	}
	facts, err := protocol.ParseChatRequest(body, s.cfg.MaxBodyBytes, s.cfg.DefaultOutputBound)
	if err != nil {
		s.httpErr.Add(1)
		if errors.Is(err, protocol.ErrUnmeteredTool) {
			writeErr(w, http.StatusUnprocessableEntity, "unmetered_tool", reqHex)
			return
		}
		writeErr(w, http.StatusBadRequest, err.Error(), reqHex)
		return
	}

	releaseLimit, ok := s.limiter.Enter(keyRec.KeyID, keyRec.RPM, keyRec.MaxConcurrency, facts.OutputBound, keyRec.TPM)
	if !ok {
		s.httpErr.Add(1)
		writeErr(w, http.StatusTooManyRequests, "rate_limited", reqHex)
		return
	}
	defer releaseLimit()

	channel, ok := s.pickChannel()
	if !ok {
		s.httpErr.Add(1)
		writeErr(w, http.StatusServiceUnavailable, "no_route", reqHex)
		return
	}

	attemptID := s.nextUUID()
	cmdID := s.nextUUID()
	_, err = s.ledger.AcquireGrant(ledger.GrantRequest{
		RequestID:    reqID,
		AttemptID:    attemptID,
		CommandID:    cmdID,
		WalletID:     keyRec.WalletID,
		TariffID:     keyRec.TariffID,
		CustomerHold: s.cfg.CustomerHoldAtoms,
		SupplierHold: s.cfg.SupplierHoldAtoms,
	})
	if err != nil {
		s.httpErr.Add(1)
		writeLedgerErr(w, err, reqHex)
		return
	}

	upCtx, cancel := context.WithTimeout(r.Context(), s.cfg.UpstreamTimeout)
	defer cancel()
	upReq, err := http.NewRequestWithContext(upCtx, http.MethodPost, strings.TrimRight(channel.BaseURL, "/")+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		_ = s.ledger.AbortGrant(ledger.AbortRequest{RequestID: reqID, AttemptID: attemptID, WalletID: keyRec.WalletID})
		s.httpErr.Add(1)
		writeErr(w, http.StatusBadGateway, "upstream_request_invalid", reqHex)
		return
	}
	upReq.Header.Set("Content-Type", "application/json")
	if channel.APIKey != "" {
		upReq.Header.Set("Authorization", "Bearer "+channel.APIKey)
	}

	started := time.Now()
	resp, err := s.client.Do(upReq)
	if err != nil {
		s.limiter.ObserveChannel(channel.ID, true, float64(time.Since(started).Milliseconds()), 10*time.Second)
		_ = s.ledger.AbortGrant(ledger.AbortRequest{RequestID: reqID, AttemptID: attemptID, WalletID: keyRec.WalletID})
		s.httpErr.Add(1)
		writeErr(w, http.StatusBadGateway, "upstream_unavailable", reqHex)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		s.limiter.ObserveChannel(channel.ID, resp.StatusCode >= 500 || resp.StatusCode == 429, float64(time.Since(started).Milliseconds()), 10*time.Second)
		_ = s.ledger.AbortGrant(ledger.AbortRequest{RequestID: reqID, AttemptID: attemptID, WalletID: keyRec.WalletID})
		s.httpErr.Add(1)
		writeErr(w, http.StatusBadGateway, "upstream_http_"+strconv.Itoa(resp.StatusCode), reqHex)
		return
	}

	if facts.Stream {
		s.relaySSE(w, resp, reqID, reqHex, attemptID, keyRec.WalletID, channel.ID, started)
		return
	}
	s.relayJSON(w, resp, reqID, reqHex, attemptID, keyRec.WalletID, channel.ID, started)
}

func (s *Server) relayJSON(
	w http.ResponseWriter,
	resp *http.Response,
	reqID ledger.UUID,
	reqHex string,
	attemptID ledger.UUID,
	walletID uint32,
	channelID string,
	started time.Time,
) {
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, s.cfg.MaxResponseBytes+1))
	if err != nil || int64(len(respBody)) > s.cfg.MaxResponseBytes {
		s.limiter.ObserveChannel(channelID, true, float64(time.Since(started).Milliseconds()), 10*time.Second)
		_ = s.ledger.AbortGrant(ledger.AbortRequest{RequestID: reqID, AttemptID: attemptID, WalletID: walletID})
		s.httpErr.Add(1)
		writeErr(w, http.StatusBadGateway, "upstream_body_invalid", reqHex)
		return
	}
	usage, err := protocol.ParseUsageFromJSON(respBody)
	if err != nil || !usage.HasUsage {
		s.limiter.ObserveChannel(channelID, true, float64(time.Since(started).Milliseconds()), 10*time.Second)
		_ = s.ledger.AbortGrant(ledger.AbortRequest{RequestID: reqID, AttemptID: attemptID, WalletID: walletID})
		s.httpErr.Add(1)
		writeErr(w, http.StatusBadGateway, "invalid_upstream_usage", reqHex)
		return
	}
	ev := ledger.SignedEvidence{
		SessionID: s.sessionID,
		RequestID: reqID,
		AttemptID: attemptID,
		EventID1:  s.nextUUID(),
		EventID2:  s.nextUUID(),
		WalletID:  walletID,
		Usage:     usage,
	}
	ledger.SignEvidence(s.privKey, &ev)
	if _, err := s.ledger.CommitTerminal(&ev); err != nil {
		s.httpErr.Add(1)
		writeErr(w, http.StatusServiceUnavailable, "evidence_unavailable", reqHex)
		return
	}
	s.limiter.ObserveChannel(channelID, false, float64(time.Since(started).Milliseconds()), 10*time.Second)
	s.httpOK.Add(1)
	w.Header().Set("content-type", "application/json")
	w.Header().Set("x-request-id", reqHex)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(respBody)
}

func (s *Server) relaySSE(
	w http.ResponseWriter,
	resp *http.Response,
	reqID ledger.UUID,
	reqHex string,
	attemptID ledger.UUID,
	walletID uint32,
	channelID string,
	started time.Time,
) {
	framer := protocol.NewSSEFramer(1 << 20)
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 8192)
	headersSent := false
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			emit, done, ferr := framer.Feed(buf[:n])
			if ferr != nil {
				s.limiter.ObserveChannel(channelID, true, float64(time.Since(started).Milliseconds()), 10*time.Second)
				s.httpErr.Add(1)
				if !headersSent {
					_ = s.ledger.AbortGrant(ledger.AbortRequest{RequestID: reqID, AttemptID: attemptID, WalletID: walletID})
					writeErr(w, http.StatusBadGateway, ferr.Error(), reqHex)
				} else {
					_, _ = w.Write([]byte("event: gateway_stream_error\ndata: {\"error\":\"stream_protocol_error\"}\n\n"))
					if flusher != nil {
						flusher.Flush()
					}
				}
				return
			}
			if len(emit) > 0 {
				if !headersSent {
					w.Header().Set("content-type", "text/event-stream; charset=utf-8")
					w.Header().Set("cache-control", "no-store")
					w.Header().Set("x-request-id", reqHex)
					w.WriteHeader(http.StatusOK)
					headersSent = true
				}
				if _, werr := w.Write(emit); werr != nil {
					s.httpErr.Add(1)
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
			}
			if done {
				break
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				break
			}
			s.limiter.ObserveChannel(channelID, true, float64(time.Since(started).Milliseconds()), 10*time.Second)
			s.httpErr.Add(1)
			if !headersSent {
				_ = s.ledger.AbortGrant(ledger.AbortRequest{RequestID: reqID, AttemptID: attemptID, WalletID: walletID})
				writeErr(w, http.StatusBadGateway, "upstream_stream_error", reqHex)
			} else {
				_, _ = w.Write([]byte("event: gateway_stream_error\ndata: {\"error\":\"upstream_stream_error\"}\n\n"))
				if flusher != nil {
					flusher.Flush()
				}
			}
			return
		}
	}

	usage := framer.Usage()
	if !framer.Done() || !usage.HasUsage {
		s.limiter.ObserveChannel(channelID, true, float64(time.Since(started).Milliseconds()), 10*time.Second)
		s.httpErr.Add(1)
		if !headersSent {
			_ = s.ledger.AbortGrant(ledger.AbortRequest{RequestID: reqID, AttemptID: attemptID, WalletID: walletID})
			writeErr(w, http.StatusBadGateway, "incomplete_upstream_stream", reqHex)
		} else {
			_, _ = w.Write([]byte("event: gateway_stream_error\ndata: {\"error\":\"incomplete_upstream_stream\"}\n\n"))
			if flusher != nil {
				flusher.Flush()
			}
		}
		return
	}

	ev := ledger.SignedEvidence{
		SessionID: s.sessionID,
		RequestID: reqID,
		AttemptID: attemptID,
		EventID1:  s.nextUUID(),
		EventID2:  s.nextUUID(),
		WalletID:  walletID,
		Usage:     usage,
	}
	ledger.SignEvidence(s.privKey, &ev)
	if _, err := s.ledger.CommitTerminal(&ev); err != nil {
		s.httpErr.Add(1)
		if !headersSent {
			writeErr(w, http.StatusServiceUnavailable, "evidence_unavailable", reqHex)
		} else {
			_, _ = w.Write([]byte("event: gateway_stream_error\ndata: {\"error\":\"evidence_unavailable\"}\n\n"))
			if flusher != nil {
				flusher.Flush()
			}
		}
		return
	}
	if !headersSent {
		w.Header().Set("content-type", "text/event-stream; charset=utf-8")
		w.Header().Set("cache-control", "no-store")
		w.Header().Set("x-request-id", reqHex)
		w.WriteHeader(http.StatusOK)
	}
	_, _ = w.Write(framer.HeldDone())
	if flusher != nil {
		flusher.Flush()
	}
	s.limiter.ObserveChannel(channelID, false, float64(time.Since(started).Milliseconds()), 10*time.Second)
	s.httpOK.Add(1)
}

func (s *Server) pickChannel() (ChannelConfig, bool) {
	n := len(s.cfg.Channels)
	if n == 0 {
		return ChannelConfig{}, false
	}
	start := s.limiter.NextRoundRobin(n)
	for i := 0; i < n; i++ {
		ch := s.cfg.Channels[(start+i)%n]
		if _, ok := s.limiter.AllowChannel(ch.ID); ok {
			return ch, true
		}
	}
	return ChannelConfig{}, false
}

func writeErr(w http.ResponseWriter, status int, code, reqHex string) {
	w.Header().Set("content-type", "application/json")
	if reqHex != "" {
		w.Header().Set("x-request-id", reqHex)
	}
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"error":{"code":"` + code + `"},"request_id":"` + reqHex + `"}`))
}

func writeLedgerErr(w http.ResponseWriter, err error, reqHex string) {
	switch {
	case errors.Is(err, ledger.ErrInsufficientCustomer), errors.Is(err, ledger.ErrWalletFrozen):
		writeErr(w, http.StatusPaymentRequired, err.Error(), reqHex)
	case errors.Is(err, ledger.ErrWalletNotFound):
		writeErr(w, http.StatusUnauthorized, err.Error(), reqHex)
	default:
		writeErr(w, http.StatusServiceUnavailable, err.Error(), reqHex)
	}
}

func safeDial(ctx context.Context, dialer *net.Dialer, network, addr string, allowPrivate bool) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	if allowPrivate {
		return dialer.DialContext(ctx, network, addr)
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil || len(ips) == 0 {
		return nil, errors.New("dns_resolution_failed")
	}
	var chosen netip.Addr
	for _, ip := range ips {
		ip = ip.Unmap()
		if !isPublicAddr(ip) {
			return nil, errors.New("ssrf_blocked_private_ip")
		}
		if !chosen.IsValid() || (ip.Is4() && !chosen.Is4()) {
			chosen = ip
		}
	}
	return dialer.DialContext(ctx, network, net.JoinHostPort(chosen.String(), port))
}

func isPublicAddr(ip netip.Addr) bool {
	if !ip.IsValid() || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
		return false
	}
	if ip.Is4() {
		b := ip.As4()
		if b[0] == 100 && (b[1]&0xc0) == 64 {
			return false
		}
		if b[0] == 0 || b[0] >= 224 {
			return false
		}
	}
	return true
}
