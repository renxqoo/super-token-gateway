package native_test

import (
	"context"
	"os"
	"testing"
	"time"

	native "gateway.local/plugin-host"
)

func TestReadyPrecedesCommandsAndIsEmittedOnlyOnce(t *testing.T) {
	p := launch(t, native.DefaultConfig())
	p.load(fixture)
	if p.request(native.Request{Op: "stats"}).Stats.Modules != 1 {
		t.Fatal("READY did not allow a real load")
	}
	p.close()
	if p.ready.Load() != 1 {
		t.Fatal("parent must emit READY exactly once")
	}
}

func TestBlockedReadyOutputHasItsOwnFiniteWriteDeadline(t *testing.T) {
	input, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	reader, output, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer output.Close()
	if err := output.SetWriteDeadline(time.Now().Add(10 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := output.Write(make([]byte, 1<<20)); err == nil {
		t.Fatal("test pipe did not reach backpressure")
	}
	c := native.DefaultConfig()
	c.WriteMS = 30
	s, err := native.New(c, executable)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := s.Serve(context.Background(), input, output); err == nil || err.Error() != "OUTPUT_FAILED" {
		t.Fatalf("blocked READY must fail output, got %v", err)
	}
	if time.Since(started) > 500*time.Millisecond {
		t.Fatal("blocked READY exceeded its bounded write and cleanup allowance")
	}
}
