package lifecycle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestShutdownOrder(t *testing.T) {
	var order []string
	var notReady, stopped, closed atomic.Bool
	comp := Func{N: "c", F: func(ctx context.Context) error {
		<-ctx.Done()
		if !notReady.Load() {
			t.Error("component stopped before readiness flipped")
		}
		stopped.Store(true)
		return nil
	}}
	stop, cancelStop := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancelStop() }()
	err := RunContext(stop, Options{
		SetNotReady:   func() { notReady.Store(true); order = append(order, "notready") },
		ShutdownDelay: 10 * time.Millisecond,
		Closers: []func(context.Context) error{func(context.Context) error {
			if !stopped.Load() {
				t.Error("closer ran before components stopped")
			}
			closed.Store(true)
			return nil
		}},
	}, comp)
	if err != nil || !closed.Load() {
		t.Fatalf("err=%v closed=%v", err, closed.Load())
	}
}

func TestComponentFailureStopsEverything(t *testing.T) {
	boom := errors.New("boom")
	bad := Func{N: "bad", F: func(context.Context) error { return boom }}
	good := Func{N: "good", F: func(ctx context.Context) error { <-ctx.Done(); return nil }}
	err := RunContext(context.Background(), Options{}, bad, good)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}
