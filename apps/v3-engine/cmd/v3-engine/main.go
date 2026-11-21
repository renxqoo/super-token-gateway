package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"super-token-gateway/v3-engine/internal/gateway"
	"super-token-gateway/v3-engine/internal/ledger"
	"super-token-gateway/v3-engine/internal/ratelimit"
)

func main() {
	mode := flag.String("mode", "gateway", "gateway or replica")
	listen := flag.String("listen", "127.0.0.1:8090", "listen address")
	upstream := flag.String("upstream", "http://127.0.0.1:9088", "upstream base url")
	walDir := flag.String("wal-dir", ".local/v3-wal", "wal directory or file")
	walShards := flag.Int("wal-shards", 4, "parallel wal shards")
	syncDisk := flag.Bool("sync-disk", true, "fsync wal batches")
	replicas := flag.String("replicas", "", "comma separated replica addresses")
	quorum := flag.Int("quorum", 0, "required replica acks")
	epoch := flag.Uint64("epoch", 1, "primary epoch")
	h2c := flag.Bool("h2c", true, "enable h2c upstream")
	allowPrivate := flag.Bool("allow-private", true, "allow private upstream addresses")
	wallets := flag.Int("wallets", 1000, "initial seeded wallets")
	flag.Parse()

	if *mode == "replica" {
		node, err := ledger.StartReplicaNode(*listen, *walDir, *epoch, *syncDisk)
		if err != nil {
			fmt.Fprintf(os.Stderr, "replica start failed: %v\n", err)
			os.Exit(1)
		}
		out, _ := json.Marshal(map[string]any{"service": "v3-replica", "event": "ready", "addr": node.Addr(), "epoch": *epoch})
		fmt.Println(string(out))
		sig := make(chan os.Signal, 2)
		signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
		<-sig
		_ = node.Close()
		return
	}

	var rep *ledger.QuorumReplicator
	if *replicas != "" && *quorum > 0 {
		addrs := strings.Split(*replicas, ",")
		rep = ledger.NewQuorumReplicator(addrs, *quorum, 1*time.Second)
	}
	l, err := ledger.OpenLedger(ledger.LedgerOptions{
		WALDir:     *walDir,
		NumWALs:    *walShards,
		Epoch:      *epoch,
		SyncDisk:   *syncDisk,
		MaxBatch:   512,
		BatchWait:  200 * time.Microsecond,
		Replicator: rep,
		VerifySigs: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ledger open failed: %v\n", err)
		os.Exit(1)
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
		fmt.Fprintf(os.Stderr, "seed cells failed: %v\n", err)
		os.Exit(1)
	}
	lim := ratelimit.New()
	srv, err := gateway.NewServer(gateway.ServerConfig{
		Pepper:               []byte("v3-engine-local-pepper"),
		MaxBodyBytes:         16 << 20,
		MaxResponseBytes:     32 << 20,
		MaxInFlight:          200000,
		DefaultOutputBound:   4096,
		CustomerHoldAtoms:    100,
		SupplierHoldAtoms:    100,
		AllowPrivateUpstream: *allowPrivate,
		UpstreamH2C:          *h2c,
		Channels: []gateway.ChannelConfig{
			{ID: "primary", BaseURL: *upstream, Weight: 1},
		},
	}, l, lim)
	if err != nil {
		fmt.Fprintf(os.Stderr, "server init failed: %v\n", err)
		os.Exit(1)
	}
	for i := 0; i < *wallets; i++ {
		if err := l.CreditWallet(uint32(i), 1_000_000_000, 0, false); err != nil {
			fmt.Fprintf(os.Stderr, "seed wallet %d failed: %v\n", i, err)
			os.Exit(1)
		}
		srv.RegisterKey(fmt.Sprintf("gw_v3_bench_key_%06d_0000000000000000000000000", i), gateway.APIKeyRecord{
			KeyID:          fmt.Sprintf("key-%d", i),
			WalletID:       uint32(i),
			TariffID:       1,
			RPM:            0,
			TPM:            0,
			MaxConcurrency: 0,
		})
	}
	httpSrv := srv.HTTPServer(*listen)
	errCh := make(chan error, 1)
	go func() {
		errCh <- httpSrv.ListenAndServe()
	}()
	out, _ := json.Marshal(map[string]any{"service": "v3-engine", "event": "ready", "addr": *listen})
	fmt.Println(string(out))

	sig := make(chan os.Signal, 2)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	select {
	case <-sig:
	case err := <-errCh:
		if err != nil {
			fmt.Fprintf(os.Stderr, "listen failed: %v\n", err)
		}
	}
	srv.SetAccepting(false)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
	srv.Close()
	if rep != nil {
		rep.Close()
	}
	_ = l.Close()
}
