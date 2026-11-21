package ledger

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"super-token-gateway/v3-engine/internal/protocol"
)

const numLedgerShards = 64

var (
	ErrWalletNotFound       = errors.New("wallet_not_found")
	ErrWalletFrozen         = errors.New("wallet_frozen")
	ErrInsufficientCustomer = errors.New("insufficient_customer_funds")
	ErrSupplierUnavailable  = errors.New("supplier_cell_unavailable")
	ErrInsufficientSupplier = errors.New("insufficient_supplier_funds")
	ErrTariffNotFound       = errors.New("tariff_not_found")
	ErrPermitNotFound       = errors.New("permit_not_found")
	ErrPermitMismatch       = errors.New("permit_identity_mismatch")
	ErrEvidenceSigInvalid   = errors.New("evidence_signature_invalid")
	ErrPricingOverflow      = errors.New("pricing_overflow")
	ErrSessionUnknown       = errors.New("session_unknown")
)

type UUID [16]byte

type Wallet struct {
	ID          uint32
	Deposited   int64
	Reserved    int64
	Spent       int64
	CreditLimit int64
	Debt        int64
	Frozen      bool
}

type SupplierCell struct {
	ID        uint32
	Deposited int64
	Reserved  int64
	Spent     int64
	Active    bool
}

type Tariff struct {
	ID                uint32
	CustomerReqNum    int64
	CustomerReqDen    int64
	CustomerInNum     int64
	CustomerInDen     int64
	CustomerCachedNum int64
	CustomerCachedDen int64
	CustomerOutNum    int64
	CustomerOutDen    int64
	SupplierInNum     int64
	SupplierInDen     int64
	SupplierCachedNum int64
	SupplierCachedDen int64
	SupplierOutNum    int64
	SupplierOutDen    int64
}

const (
	PermitGranted uint8 = 1
	PermitSettled uint8 = 2
	PermitAborted uint8 = 3
	PermitUnknown uint8 = 4
)

type Permit struct {
	RequestID    UUID
	AttemptID    UUID
	CommandID    UUID
	WalletID     uint32
	CellID       uint32
	TariffID     uint32
	CustomerHold int64
	SupplierHold int64
	State        uint8
	CreatedAtNs  int64
}

type Terminal struct {
	RequestID       UUID
	AttemptID       UUID
	EventID1        UUID
	EventID2        UUID
	CustomerAtoms   int64
	SupplierAtoms   int64
	InputTokens     int64
	CachedTokens    int64
	OutputTokens    int64
	ReasoningTokens int64
	OutboxSeq1      uint64
	OutboxSeq2      uint64
}

type OutboxEvent struct {
	Seq           uint64
	EventID       UUID
	RequestID     UUID
	Revision      uint8
	CustomerAtoms int64
	SupplierAtoms int64
	Acked         bool
	LeaseUntilNs  int64
}

type GrantRequest struct {
	RequestID    UUID
	AttemptID    UUID
	CommandID    UUID
	WalletID     uint32
	TariffID     uint32
	CustomerHold int64
	SupplierHold int64
}

type GrantResult struct {
	Fresh     bool
	Duplicate bool
	CellID    uint32
	LSN       uint64
}

type SignedEvidence struct {
	SessionID UUID
	RequestID UUID
	AttemptID UUID
	EventID1  UUID
	EventID2  UUID
	WalletID  uint32
	Usage     protocol.Usage
	Signature [64]byte
}

type SettleResult struct {
	Fresh         bool
	Duplicate     bool
	CustomerAtoms int64
	SupplierAtoms int64
	OutboxSeq1    uint64
	OutboxSeq2    uint64
	LSN           uint64
}

type AbortRequest struct {
	RequestID UUID
	AttemptID UUID
	WalletID  uint32
}

type AuditSummary struct {
	Wallets         int
	Cells           int
	Permits         int
	Terminals       int
	ActiveHolds     int64
	ActiveSupplierH int64
	TotalCustSpent  int64
	TotalSuppSpent  int64
	SumTermCust     int64
	SumTermSupp     int64
	TotalOutbox     int
	AckedOutbox     int
	Balanced        bool
}

type ledgerShard struct {
	mu        sync.Mutex
	wallets   map[uint32]*Wallet
	cells     map[uint32]*SupplierCell
	permits   map[UUID]*Permit
	commands  map[UUID]UUID
	terminals map[UUID]*Terminal
	outbox    []OutboxEvent
	bySeq     map[uint64]int
	scanPos   int
}

type LedgerOptions struct {
	WALDir     string
	NumWALs    int
	Epoch      uint64
	SyncDisk   bool
	MaxBatch   int
	BatchWait  time.Duration
	Replicator Replicator
	VerifySigs bool
}

type Ledger struct {
	opts      LedgerOptions
	wals      []*WAL
	shards    [numLedgerShards]ledgerShard
	tariffMu  sync.RWMutex
	tariffs   map[uint32]Tariff
	sessMu    sync.RWMutex
	sessions  map[UUID]ed25519.PublicKey
	lsn       atomic.Uint64
	outboxSeq atomic.Uint64
	claimRR   atomic.Uint64
	grantsOK  atomic.Uint64
	settlesOK atomic.Uint64
	abortsOK  atomic.Uint64
	acksOK    atomic.Uint64
	fenced    atomic.Bool
}

func EvidenceMessage(ev *SignedEvidence) [32]byte {
	var buf [120]byte
	copy(buf[0:16], ev.SessionID[:])
	copy(buf[16:32], ev.RequestID[:])
	copy(buf[32:48], ev.AttemptID[:])
	copy(buf[48:64], ev.EventID1[:])
	copy(buf[64:80], ev.EventID2[:])
	binary.LittleEndian.PutUint32(buf[80:84], ev.WalletID)
	binary.LittleEndian.PutUint64(buf[84:92], uint64(ev.Usage.InputTokens))
	binary.LittleEndian.PutUint64(buf[92:100], uint64(ev.Usage.CachedInputTokens))
	binary.LittleEndian.PutUint64(buf[100:108], uint64(ev.Usage.OutputTokens))
	binary.LittleEndian.PutUint64(buf[108:116], uint64(ev.Usage.ReasoningTokens))
	if ev.Usage.HasUsage {
		buf[116] = 1
	}
	return sha256.Sum256(buf[:])
}

func SignEvidence(priv ed25519.PrivateKey, ev *SignedEvidence) {
	msg := EvidenceMessage(ev)
	sig := ed25519.Sign(priv, msg[:])
	copy(ev.Signature[:], sig)
}

func OpenLedger(opts LedgerOptions) (*Ledger, error) {
	if opts.NumWALs <= 0 {
		opts.NumWALs = 1
	}
	if opts.Epoch == 0 {
		opts.Epoch = 1
	}
	if err := os.MkdirAll(opts.WALDir, 0o700); err != nil {
		return nil, err
	}
	l := &Ledger{
		opts:     opts,
		wals:     make([]*WAL, opts.NumWALs),
		tariffs:  make(map[uint32]Tariff, 16),
		sessions: make(map[UUID]ed25519.PublicKey, 16),
	}
	for i := range l.shards {
		l.shards[i] = ledgerShard{
			wallets:   make(map[uint32]*Wallet, 32),
			cells:     make(map[uint32]*SupplierCell, 4),
			permits:   make(map[UUID]*Permit, 1024),
			commands:  make(map[UUID]UUID, 1024),
			terminals: make(map[UUID]*Terminal, 1024),
			outbox:    make([]OutboxEvent, 0, 2048),
			bySeq:     make(map[uint64]int, 2048),
		}
	}
	var allRecs []WALRecord
	for i := 0; i < opts.NumWALs; i++ {
		wpath := filepath.Join(opts.WALDir, fmt.Sprintf("shard-%02d.wal", i))
		w, recs, err := OpenWAL(WALOptions{
			Path:       wpath,
			Epoch:      opts.Epoch,
			SyncDisk:   opts.SyncDisk,
			MaxBatch:   opts.MaxBatch,
			BatchWait:  opts.BatchWait,
			Replicator: opts.Replicator,
		})
		if err != nil {
			for j := 0; j < i; j++ {
				_ = l.wals[j].Close()
			}
			return nil, err
		}
		l.wals[i] = w
		allRecs = append(allRecs, recs...)
	}
	sort.Slice(allRecs, func(i, j int) bool {
		return allRecs[i].LSN < allRecs[j].LSN
	})
	var maxLSN uint64
	var maxOutbox uint64
	for _, rec := range allRecs {
		if rec.LSN > maxLSN {
			maxLSN = rec.LSN
		}
		seq := l.replayRecord(rec)
		if seq > maxOutbox {
			maxOutbox = seq
		}
	}
	l.lsn.Store(maxLSN)
	l.outboxSeq.Store(maxOutbox)
	return l, nil
}

func (l *Ledger) RegisterTariff(t Tariff) {
	l.tariffMu.Lock()
	l.tariffs[t.ID] = t
	l.tariffMu.Unlock()
}

func (l *Ledger) RegisterSession(sessionID UUID, pub ed25519.PublicKey) {
	cp := make(ed25519.PublicKey, len(pub))
	copy(cp, pub)
	l.sessMu.Lock()
	l.sessions[sessionID] = cp
	l.sessMu.Unlock()
}

func (l *Ledger) walFor(walletID uint32) *WAL {
	return l.wals[walletID%uint32(len(l.wals))]
}

func (l *Ledger) shardFor(walletID uint32) (*ledgerShard, uint32) {
	idx := walletID % numLedgerShards
	return &l.shards[idx], idx
}

func (l *Ledger) CreditWallet(walletID uint32, amount, creditLimit int64, frozen bool) error {
	sh, _ := l.shardFor(walletID)
	lsn := l.lsn.Add(1)
	var payload [32]byte
	binary.LittleEndian.PutUint64(payload[0:8], lsn)
	binary.LittleEndian.PutUint32(payload[8:12], walletID)
	if frozen {
		payload[12] = 1
	}
	binary.LittleEndian.PutUint64(payload[16:24], uint64(amount))
	binary.LittleEndian.PutUint64(payload[24:32], uint64(creditLimit))
	if err := l.walFor(walletID).Append(RecWalletCredit, lsn, payload[:]); err != nil {
		return err
	}
	sh.mu.Lock()
	w := sh.wallets[walletID]
	if w == nil {
		w = &Wallet{ID: walletID}
		sh.wallets[walletID] = w
	}
	w.Deposited += amount
	w.CreditLimit = creditLimit
	w.Frozen = frozen
	sh.mu.Unlock()
	return nil
}

func (l *Ledger) CreditSupplierCell(cellID uint32, amount int64, active bool) error {
	sh := &l.shards[cellID%numLedgerShards]
	lsn := l.lsn.Add(1)
	var payload [24]byte
	binary.LittleEndian.PutUint64(payload[0:8], lsn)
	binary.LittleEndian.PutUint32(payload[8:12], cellID)
	if active {
		payload[12] = 1
	}
	binary.LittleEndian.PutUint64(payload[16:24], uint64(amount))
	if err := l.wals[cellID%uint32(len(l.wals))].Append(RecSupplierCredit, lsn, payload[:]); err != nil {
		return err
	}
	sh.mu.Lock()
	c := sh.cells[cellID]
	if c == nil {
		c = &SupplierCell{ID: cellID}
		sh.cells[cellID] = c
	}
	c.Deposited += amount
	c.Active = active
	sh.mu.Unlock()
	return nil
}

func (l *Ledger) SeedAllSupplierCells(amountPerCell int64) error {
	for i := uint32(0); i < numLedgerShards; i++ {
		if err := l.CreditSupplierCell(i, amountPerCell, true); err != nil {
			return err
		}
	}
	return nil
}

func (l *Ledger) AcquireGrant(req GrantRequest) (GrantResult, error) {
	if l.fenced.Load() {
		return GrantResult{}, ErrFenced
	}
	l.tariffMu.RLock()
	_, ok := l.tariffs[req.TariffID]
	l.tariffMu.RUnlock()
	if !ok {
		return GrantResult{}, ErrTariffNotFound
	}
	sh, cellID := l.shardFor(req.WalletID)
	nowNs := time.Now().UnixNano()
	lsn := l.lsn.Add(1)

	sh.mu.Lock()
	if existingReq, exists := sh.commands[req.CommandID]; exists {
		p := sh.permits[existingReq]
		sh.mu.Unlock()
		if p == nil || p.RequestID != req.RequestID || p.AttemptID != req.AttemptID || p.WalletID != req.WalletID {
			return GrantResult{}, ErrPermitMismatch
		}
		return GrantResult{Fresh: false, Duplicate: true, CellID: p.CellID}, nil
	}
	if p := sh.permits[req.RequestID]; p != nil {
		sh.mu.Unlock()
		if p.AttemptID != req.AttemptID || p.CommandID != req.CommandID || p.WalletID != req.WalletID {
			return GrantResult{}, ErrPermitMismatch
		}
		return GrantResult{Fresh: false, Duplicate: true, CellID: p.CellID}, nil
	}
	w := sh.wallets[req.WalletID]
	if w == nil {
		sh.mu.Unlock()
		return GrantResult{}, ErrWalletNotFound
	}
	if w.Frozen {
		sh.mu.Unlock()
		return GrantResult{}, ErrWalletFrozen
	}
	if w.Reserved+w.Spent+req.CustomerHold > w.Deposited+w.CreditLimit {
		sh.mu.Unlock()
		return GrantResult{}, ErrInsufficientCustomer
	}
	cell := sh.cells[cellID]
	if cell == nil || !cell.Active {
		sh.mu.Unlock()
		return GrantResult{}, ErrSupplierUnavailable
	}
	if cell.Reserved+cell.Spent+req.SupplierHold > cell.Deposited {
		sh.mu.Unlock()
		return GrantResult{}, ErrInsufficientSupplier
	}
	w.Reserved += req.CustomerHold
	cell.Reserved += req.SupplierHold
	permit := &Permit{
		RequestID:    req.RequestID,
		AttemptID:    req.AttemptID,
		CommandID:    req.CommandID,
		WalletID:     req.WalletID,
		CellID:       cellID,
		TariffID:     req.TariffID,
		CustomerHold: req.CustomerHold,
		SupplierHold: req.SupplierHold,
		State:        PermitGranted,
		CreatedAtNs:  nowNs,
	}
	sh.permits[req.RequestID] = permit
	sh.commands[req.CommandID] = req.RequestID
	sh.mu.Unlock()

	var payload [104]byte
	binary.LittleEndian.PutUint64(payload[0:8], lsn)
	binary.LittleEndian.PutUint64(payload[8:16], l.opts.Epoch)
	copy(payload[16:32], req.RequestID[:])
	copy(payload[32:48], req.AttemptID[:])
	copy(payload[48:64], req.CommandID[:])
	binary.LittleEndian.PutUint32(payload[64:68], req.WalletID)
	binary.LittleEndian.PutUint32(payload[68:72], cellID)
	binary.LittleEndian.PutUint32(payload[72:76], req.TariffID)
	binary.LittleEndian.PutUint64(payload[76:84], uint64(req.CustomerHold))
	binary.LittleEndian.PutUint64(payload[84:92], uint64(req.SupplierHold))
	binary.LittleEndian.PutUint64(payload[92:100], uint64(nowNs))

	if err := l.walFor(req.WalletID).Append(RecGrant, lsn, payload[:]); err != nil {
		if errors.Is(err, ErrFenced) {
			l.fenced.Store(true)
		}
		sh.mu.Lock()
		if p := sh.permits[req.RequestID]; p != nil && p.State == PermitGranted {
			if errors.Is(err, ErrQuorumUnavailable) || errors.Is(err, ErrFenced) {
				p.State = PermitUnknown
			} else {
				w.Reserved -= req.CustomerHold
				cell.Reserved -= req.SupplierHold
				delete(sh.permits, req.RequestID)
				delete(sh.commands, req.CommandID)
			}
		}
		sh.mu.Unlock()
		return GrantResult{}, err
	}
	l.grantsOK.Add(1)
	return GrantResult{Fresh: true, CellID: cellID, LSN: lsn}, nil
}

func priceTariff(t Tariff, u protocol.Usage) (customer int64, supplier int64, ok bool) {
	cReq, ok1 := protocol.CeilingRational(1, t.CustomerReqNum, t.CustomerReqDen)
	cIn, ok2 := protocol.CeilingRational(u.InputTokens, t.CustomerInNum, t.CustomerInDen)
	cCached, ok3 := protocol.CeilingRational(u.CachedInputTokens, t.CustomerCachedNum, t.CustomerCachedDen)
	cOut, ok4 := protocol.CeilingRational(u.OutputTokens, t.CustomerOutNum, t.CustomerOutDen)
	sIn, ok5 := protocol.CeilingRational(u.InputTokens, t.SupplierInNum, t.SupplierInDen)
	sCached, ok6 := protocol.CeilingRational(u.CachedInputTokens, t.SupplierCachedNum, t.SupplierCachedDen)
	sOut, ok7 := protocol.CeilingRational(u.OutputTokens, t.SupplierOutNum, t.SupplierOutDen)
	if !(ok1 && ok2 && ok3 && ok4 && ok5 && ok6 && ok7) {
		return 0, 0, false
	}
	return cReq + cIn + cCached + cOut, sIn + sCached + sOut, true
}

func (l *Ledger) CommitTerminal(ev *SignedEvidence) (SettleResult, error) {
	if l.fenced.Load() {
		return SettleResult{}, ErrFenced
	}
	if l.opts.VerifySigs {
		l.sessMu.RLock()
		pub := l.sessions[ev.SessionID]
		l.sessMu.RUnlock()
		if len(pub) != ed25519.PublicKeySize {
			return SettleResult{}, ErrSessionUnknown
		}
		msg := EvidenceMessage(ev)
		if !ed25519.Verify(pub, msg[:], ev.Signature[:]) {
			return SettleResult{}, ErrEvidenceSigInvalid
		}
	}

	sh, _ := l.shardFor(ev.WalletID)
	lsn := l.lsn.Add(1)
	nowNs := time.Now().UnixNano()

	sh.mu.Lock()
	if existing := sh.terminals[ev.RequestID]; existing != nil {
		sh.mu.Unlock()
		if existing.AttemptID != ev.AttemptID || existing.EventID1 != ev.EventID1 || existing.EventID2 != ev.EventID2 {
			return SettleResult{}, ErrPermitMismatch
		}
		return SettleResult{
			Fresh:         false,
			Duplicate:     true,
			CustomerAtoms: existing.CustomerAtoms,
			SupplierAtoms: existing.SupplierAtoms,
			OutboxSeq1:    existing.OutboxSeq1,
			OutboxSeq2:    existing.OutboxSeq2,
		}, nil
	}
	permit := sh.permits[ev.RequestID]
	if permit == nil {
		sh.mu.Unlock()
		return SettleResult{}, ErrPermitNotFound
	}
	if permit.AttemptID != ev.AttemptID || permit.WalletID != ev.WalletID {
		sh.mu.Unlock()
		return SettleResult{}, ErrPermitMismatch
	}
	tariffID := permit.TariffID
	sh.mu.Unlock()

	l.tariffMu.RLock()
	tariff, hasTariff := l.tariffs[tariffID]
	l.tariffMu.RUnlock()
	if !hasTariff {
		return SettleResult{}, ErrTariffNotFound
	}
	custAtoms, suppAtoms, ok := priceTariff(tariff, ev.Usage)
	if !ok {
		return SettleResult{}, ErrPricingOverflow
	}
	seq1 := l.outboxSeq.Add(1)
	seq2 := l.outboxSeq.Add(1)

	sigDigest := sha256.Sum256(ev.Signature[:])
	var payload [176]byte
	binary.LittleEndian.PutUint64(payload[0:8], lsn)
	binary.LittleEndian.PutUint64(payload[8:16], l.opts.Epoch)
	copy(payload[16:32], ev.RequestID[:])
	copy(payload[32:48], ev.AttemptID[:])
	copy(payload[48:64], ev.EventID1[:])
	copy(payload[64:80], ev.EventID2[:])
	binary.LittleEndian.PutUint64(payload[80:88], uint64(custAtoms))
	binary.LittleEndian.PutUint64(payload[88:96], uint64(suppAtoms))
	binary.LittleEndian.PutUint64(payload[96:104], uint64(ev.Usage.InputTokens))
	binary.LittleEndian.PutUint64(payload[104:112], uint64(ev.Usage.CachedInputTokens))
	binary.LittleEndian.PutUint64(payload[112:120], uint64(ev.Usage.OutputTokens))
	binary.LittleEndian.PutUint64(payload[120:128], uint64(ev.Usage.ReasoningTokens))
	binary.LittleEndian.PutUint64(payload[128:136], seq1)
	binary.LittleEndian.PutUint64(payload[136:144], seq2)
	binary.LittleEndian.PutUint64(payload[144:152], uint64(nowNs))
	binary.LittleEndian.PutUint32(payload[152:156], ev.WalletID)
	copy(payload[160:176], sigDigest[0:16])

	if err := l.walFor(ev.WalletID).Append(RecSettle, lsn, payload[:]); err != nil {
		if errors.Is(err, ErrFenced) {
			l.fenced.Store(true)
		}
		return SettleResult{}, err
	}

	sh.mu.Lock()
	if existing := sh.terminals[ev.RequestID]; existing != nil {
		sh.mu.Unlock()
		return SettleResult{
			Fresh:         false,
			Duplicate:     true,
			CustomerAtoms: existing.CustomerAtoms,
			SupplierAtoms: existing.SupplierAtoms,
			OutboxSeq1:    existing.OutboxSeq1,
			OutboxSeq2:    existing.OutboxSeq2,
		}, nil
	}
	permit = sh.permits[ev.RequestID]
	w := sh.wallets[permit.WalletID]
	cell := sh.cells[permit.CellID]
	w.Reserved -= permit.CustomerHold
	w.Spent += custAtoms
	if w.Spent > w.Deposited {
		w.Debt = w.Spent - w.Deposited
	}
	cell.Reserved -= permit.SupplierHold
	cell.Spent += suppAtoms
	permit.State = PermitSettled
	sh.terminals[ev.RequestID] = &Terminal{
		RequestID:       ev.RequestID,
		AttemptID:       ev.AttemptID,
		EventID1:        ev.EventID1,
		EventID2:        ev.EventID2,
		CustomerAtoms:   custAtoms,
		SupplierAtoms:   suppAtoms,
		InputTokens:     ev.Usage.InputTokens,
		CachedTokens:    ev.Usage.CachedInputTokens,
		OutputTokens:    ev.Usage.OutputTokens,
		ReasoningTokens: ev.Usage.ReasoningTokens,
		OutboxSeq1:      seq1,
		OutboxSeq2:      seq2,
	}
	idx1 := len(sh.outbox)
	sh.outbox = append(
		sh.outbox,
		OutboxEvent{
			Seq:           seq1,
			EventID:       ev.EventID1,
			RequestID:     ev.RequestID,
			Revision:      1,
			CustomerAtoms: custAtoms,
			SupplierAtoms: suppAtoms,
		},
		OutboxEvent{
			Seq:           seq2,
			EventID:       ev.EventID2,
			RequestID:     ev.RequestID,
			Revision:      2,
			CustomerAtoms: custAtoms,
			SupplierAtoms: suppAtoms,
		},
	)
	sh.bySeq[seq1] = idx1
	sh.bySeq[seq2] = idx1 + 1
	sh.mu.Unlock()

	l.settlesOK.Add(1)
	return SettleResult{
		Fresh:         true,
		CustomerAtoms: custAtoms,
		SupplierAtoms: suppAtoms,
		OutboxSeq1:    seq1,
		OutboxSeq2:    seq2,
		LSN:           lsn,
	}, nil
}

func (l *Ledger) AbortGrant(req AbortRequest) error {
	sh, _ := l.shardFor(req.WalletID)
	sh.mu.Lock()
	p := sh.permits[req.RequestID]
	if p == nil || p.AttemptID != req.AttemptID {
		sh.mu.Unlock()
		return ErrPermitNotFound
	}
	if p.State == PermitAborted || p.State == PermitSettled {
		sh.mu.Unlock()
		return nil
	}
	sh.mu.Unlock()

	lsn := l.lsn.Add(1)
	var payload [52]byte
	binary.LittleEndian.PutUint64(payload[0:8], lsn)
	binary.LittleEndian.PutUint64(payload[8:16], l.opts.Epoch)
	copy(payload[16:32], req.RequestID[:])
	copy(payload[32:48], req.AttemptID[:])
	binary.LittleEndian.PutUint32(payload[48:52], req.WalletID)
	if err := l.walFor(req.WalletID).Append(RecAbort, lsn, payload[:]); err != nil {
		return err
	}

	sh.mu.Lock()
	p = sh.permits[req.RequestID]
	if p != nil && (p.State == PermitGranted || p.State == PermitUnknown) {
		w := sh.wallets[p.WalletID]
		cell := sh.cells[p.CellID]
		w.Reserved -= p.CustomerHold
		cell.Reserved -= p.SupplierHold
		p.State = PermitAborted
		l.abortsOK.Add(1)
	}
	sh.mu.Unlock()
	return nil
}

func (l *Ledger) ClaimEvents(limit int, lease time.Duration) []OutboxEvent {
	if limit <= 0 {
		limit = 256
	}
	if lease <= 0 {
		lease = 30 * time.Second
	}
	now := time.Now().UnixNano()
	until := now + int64(lease)
	start := int(l.claimRR.Add(1) % numLedgerShards)
	claimed := make([]OutboxEvent, 0, limit)
	for offset := 0; offset < numLedgerShards && len(claimed) < limit; offset++ {
		sh := &l.shards[(start+offset)%numLedgerShards]
		sh.mu.Lock()
		for sh.scanPos < len(sh.outbox) && sh.outbox[sh.scanPos].Acked {
			sh.scanPos++
		}
		for i := sh.scanPos; i < len(sh.outbox) && len(claimed) < limit; i++ {
			ev := &sh.outbox[i]
			if ev.Acked || ev.LeaseUntilNs > now {
				continue
			}
			ev.LeaseUntilNs = until
			claimed = append(claimed, *ev)
		}
		sh.mu.Unlock()
	}
	return claimed
}

func (l *Ledger) AckEvents(seqs []uint64) int {
	if len(seqs) == 0 {
		return 0
	}
	seqSet := make(map[uint64]struct{}, len(seqs))
	for _, s := range seqs {
		seqSet[s] = struct{}{}
	}
	newly := 0
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		for seq := range seqSet {
			if idx, ok := sh.bySeq[seq]; ok {
				if !sh.outbox[idx].Acked {
					sh.outbox[idx].Acked = true
					newly++
				}
				delete(seqSet, seq)
			}
		}
		sh.mu.Unlock()
		if len(seqSet) == 0 {
			break
		}
	}
	if newly > 0 {
		l.acksOK.Add(uint64(newly))
	}
	return newly
}

func (l *Ledger) WalletSnapshot(walletID uint32) (Wallet, bool) {
	sh, _ := l.shardFor(walletID)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	w := sh.wallets[walletID]
	if w == nil {
		return Wallet{}, false
	}
	return *w, true
}

func (l *Ledger) AuditConservation() AuditSummary {
	var s AuditSummary
	s.Balanced = true
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		for _, w := range sh.wallets {
			s.Wallets++
			s.ActiveHolds += w.Reserved
			s.TotalCustSpent += w.Spent
			if w.Reserved < 0 || w.Spent < 0 || w.Reserved+w.Spent > w.Deposited+w.CreditLimit {
				s.Balanced = false
			}
		}
		for _, c := range sh.cells {
			s.Cells++
			s.ActiveSupplierH += c.Reserved
			s.TotalSuppSpent += c.Spent
			if c.Reserved < 0 || c.Spent < 0 || c.Reserved+c.Spent > c.Deposited {
				s.Balanced = false
			}
		}
		s.Permits += len(sh.permits)
		for _, t := range sh.terminals {
			s.Terminals++
			s.SumTermCust += t.CustomerAtoms
			s.SumTermSupp += t.SupplierAtoms
		}
		s.TotalOutbox += len(sh.outbox)
		for j := range sh.outbox {
			if sh.outbox[j].Acked {
				s.AckedOutbox++
			}
		}
		sh.mu.Unlock()
	}
	if s.TotalCustSpent != s.SumTermCust || s.TotalSuppSpent != s.SumTermSupp {
		s.Balanced = false
	}
	return s
}

func (l *Ledger) AggregateWALStats() WALStats {
	var total WALStats
	for _, w := range l.wals {
		st := w.Stats()
		total.WrittenBytes += st.WrittenBytes
		total.WrittenRecs += st.WrittenRecs
		total.SyncBatches += st.SyncBatches
		if st.MaxBatchRecs > total.MaxBatchRecs {
			total.MaxBatchRecs = st.MaxBatchRecs
		}
	}
	return total
}

func (l *Ledger) replayRecord(rec WALRecord) uint64 {
	p := rec.Payload
	switch rec.Type {
	case RecWalletCredit:
		if len(p) < 32 {
			return 0
		}
		walletID := binary.LittleEndian.Uint32(p[8:12])
		frozen := p[12] == 1
		amount := int64(binary.LittleEndian.Uint64(p[16:24]))
		creditLimit := int64(binary.LittleEndian.Uint64(p[24:32]))
		sh, _ := l.shardFor(walletID)
		w := sh.wallets[walletID]
		if w == nil {
			w = &Wallet{ID: walletID}
			sh.wallets[walletID] = w
		}
		w.Deposited += amount
		w.CreditLimit = creditLimit
		w.Frozen = frozen
	case RecSupplierCredit:
		if len(p) < 24 {
			return 0
		}
		cellID := binary.LittleEndian.Uint32(p[8:12])
		active := p[12] == 1
		amount := int64(binary.LittleEndian.Uint64(p[16:24]))
		sh := &l.shards[cellID%numLedgerShards]
		c := sh.cells[cellID]
		if c == nil {
			c = &SupplierCell{ID: cellID}
			sh.cells[cellID] = c
		}
		c.Deposited += amount
		c.Active = active
	case RecGrant:
		if len(p) < 100 {
			return 0
		}
		var reqID, attID, cmdID UUID
		copy(reqID[:], p[16:32])
		copy(attID[:], p[32:48])
		copy(cmdID[:], p[48:64])
		walletID := binary.LittleEndian.Uint32(p[64:68])
		cellID := binary.LittleEndian.Uint32(p[68:72])
		tariffID := binary.LittleEndian.Uint32(p[72:76])
		custHold := int64(binary.LittleEndian.Uint64(p[76:84]))
		suppHold := int64(binary.LittleEndian.Uint64(p[84:92]))
		created := int64(binary.LittleEndian.Uint64(p[92:100]))
		sh, _ := l.shardFor(walletID)
		if sh.permits[reqID] != nil {
			return 0
		}
		w := sh.wallets[walletID]
		c := sh.cells[cellID]
		if w == nil || c == nil {
			return 0
		}
		w.Reserved += custHold
		c.Reserved += suppHold
		sh.permits[reqID] = &Permit{
			RequestID:    reqID,
			AttemptID:    attID,
			CommandID:    cmdID,
			WalletID:     walletID,
			CellID:       cellID,
			TariffID:     tariffID,
			CustomerHold: custHold,
			SupplierHold: suppHold,
			State:        PermitGranted,
			CreatedAtNs:  created,
		}
		sh.commands[cmdID] = reqID
	case RecSettle:
		if len(p) < 160 {
			return 0
		}
		var reqID, attID, ev1, ev2 UUID
		copy(reqID[:], p[16:32])
		copy(attID[:], p[32:48])
		copy(ev1[:], p[48:64])
		copy(ev2[:], p[64:80])
		custAtoms := int64(binary.LittleEndian.Uint64(p[80:88]))
		suppAtoms := int64(binary.LittleEndian.Uint64(p[88:96]))
		inTok := int64(binary.LittleEndian.Uint64(p[96:104]))
		cachedTok := int64(binary.LittleEndian.Uint64(p[104:112]))
		outTok := int64(binary.LittleEndian.Uint64(p[112:120]))
		reasTok := int64(binary.LittleEndian.Uint64(p[120:128]))
		seq1 := binary.LittleEndian.Uint64(p[128:136])
		seq2 := binary.LittleEndian.Uint64(p[136:144])
		walletID := binary.LittleEndian.Uint32(p[152:156])
		sh, _ := l.shardFor(walletID)
		if sh.terminals[reqID] != nil {
			return max(seq1, seq2)
		}
		permit := sh.permits[reqID]
		if permit == nil {
			return max(seq1, seq2)
		}
		w := sh.wallets[permit.WalletID]
		c := sh.cells[permit.CellID]
		w.Reserved -= permit.CustomerHold
		w.Spent += custAtoms
		if w.Spent > w.Deposited {
			w.Debt = w.Spent - w.Deposited
		}
		c.Reserved -= permit.SupplierHold
		c.Spent += suppAtoms
		permit.State = PermitSettled
		sh.terminals[reqID] = &Terminal{
			RequestID:       reqID,
			AttemptID:       attID,
			EventID1:        ev1,
			EventID2:        ev2,
			CustomerAtoms:   custAtoms,
			SupplierAtoms:   suppAtoms,
			InputTokens:     inTok,
			CachedTokens:    cachedTok,
			OutputTokens:    outTok,
			ReasoningTokens: reasTok,
			OutboxSeq1:      seq1,
			OutboxSeq2:      seq2,
		}
		idx1 := len(sh.outbox)
		sh.outbox = append(
			sh.outbox,
			OutboxEvent{Seq: seq1, EventID: ev1, RequestID: reqID, Revision: 1, CustomerAtoms: custAtoms, SupplierAtoms: suppAtoms},
			OutboxEvent{Seq: seq2, EventID: ev2, RequestID: reqID, Revision: 2, CustomerAtoms: custAtoms, SupplierAtoms: suppAtoms},
		)
		sh.bySeq[seq1] = idx1
		sh.bySeq[seq2] = idx1 + 1
		return max(seq1, seq2)
	case RecAbort:
		if len(p) < 52 {
			return 0
		}
		var reqID UUID
		copy(reqID[:], p[16:32])
		walletID := binary.LittleEndian.Uint32(p[48:52])
		sh, _ := l.shardFor(walletID)
		if permit := sh.permits[reqID]; permit != nil && permit.State == PermitGranted {
			sh.wallets[permit.WalletID].Reserved -= permit.CustomerHold
			sh.cells[permit.CellID].Reserved -= permit.SupplierHold
			permit.State = PermitAborted
		}
	}
	return 0
}

func (l *Ledger) Close() error {
	var firstErr error
	for _, w := range l.wals {
		if w != nil {
			if err := w.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
