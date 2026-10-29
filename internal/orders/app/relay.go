package app

// This file: the transactional-outbox relay and the payment reconciler
// (binary orders-outbox-relay).
//
// Outbox relay loop, step by step (ports.Outbox.Claim does 1 and 4 in ONE
// transaction):
//
//	1. SELECT ... FROM outbox WHERE published_at IS NULL ORDER BY created_at
//	   LIMIT n FOR UPDATE SKIP LOCKED
//	      FOR UPDATE   locks the rows we picked.
//	      SKIP LOCKED  makes other relay replicas SKIP rows we hold instead of
//	                   waiting, so two or more replicas never publish the same
//	                   row at the same time and never block each other.
//	2. PutEvents to EventBridge (entry id = outbox row id = eventId)
//	3. for rows EventBridge accepted: published_at = now(); for failures:
//	   attempts + 1
//	4. COMMIT (releases the locks)
//
// If the process dies between 2 and 4 the transaction rolls back, the rows stay
// unpublished and are published AGAIN by someone: duplicates are possible,
// losses are not. Consumers dedupe on eventId.
//
// Ordering caveat: events of one order are written in order, but with several
// replicas or after failures they may be published slightly out of order.
// Consumers must not depend on cross-event ordering.

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/olagookundavid/gatepass-app/internal/orders/domain"
	"github.com/olagookundavid/gatepass-app/internal/orders/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/telemetry"
)

// Relay publishes outbox rows and reconciles lost payment requests.
type Relay struct {
	Outbox    ports.Outbox
	Publisher ports.Publisher
	Payments  ports.Payments
	Orders    ports.Orders
	Batch     int           // rows per cycle, default 25
	Poll      time.Duration // idle sleep, default 1s
	// ReconcileAfter: pending orders without a recorded payment enqueue older than
	// this are queued by the reconciler (default 30s).
	ReconcileAfter time.Duration
	Retention      time.Duration // published outbox rows kept this long (default 7 days)
	Now            func() time.Time

	unpublished atomic.Int64
}

func (r *Relay) defaults() {
	if r.Batch <= 0 {
		r.Batch = 25
	}
	if r.Poll <= 0 {
		r.Poll = time.Second
	}
	if r.ReconcileAfter <= 0 {
		r.ReconcileAfter = 30 * time.Second
	}
	if r.Retention <= 0 {
		r.Retention = 7 * 24 * time.Hour
	}
	if r.Now == nil {
		r.Now = func() time.Time { return time.Now().UTC() }
	}
}

var (
	publishLatency = telemetry.Histogram("outbox_publish_latency_seconds", "s", "Time between an outbox row being written and published")
	publishedTotal = telemetry.Counter("outbox_published_total", "Outbox rows published")
	publishFailed  = telemetry.Counter("outbox_publish_failures_total", "Outbox rows EventBridge rejected")
)

// RunOnce publishes one batch. It returns how many rows were claimed.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	r.defaults()
	n, err := r.Outbox.Claim(ctx, r.Batch, func(rows []ports.OutboxRow) ([]string, []string, error) {
		errs := r.Publisher.Publish(ctx, rows)
		var ok, bad []string
		now := r.Now()
		for i, row := range rows {
			if errs[i] == nil {
				ok = append(ok, row.ID)
				publishLatency.Record(ctx, now.Sub(row.CreatedAt).Seconds())
				continue
			}
			bad = append(bad, row.ID)
			slog.WarnContext(ctx, "outbox row not published", "outbox_id", row.ID, "event_type", row.EventType, "attempts", row.Attempts+1, "error", errs[i].Error())
		}
		publishedTotal.Add(ctx, int64(len(ok)))
		publishFailed.Add(ctx, int64(len(bad)))
		return ok, bad, nil
	})
	return n, err
}

// ReconcilePayments queues payment requests that were lost between the order
// commit and the SQS send (API crash, SQS outage).
func (r *Relay) ReconcilePayments(ctx context.Context) (int, error) {
	r.defaults()
	return r.Outbox.ClaimUnenqueuedPayments(ctx, r.ReconcileAfter, 25, func(reqs []domain.PaymentRequest) ([]string, error) {
		var done []string
		for _, req := range reqs {
			if err := r.Payments.Enqueue(ctx, req); err != nil {
				slog.WarnContext(ctx, "reconciler could not queue payment", "order_id", req.OrderID, "error", err.Error())
				continue
			}
			done = append(done, req.OrderID)
		}
		return done, nil
	})
}

// Name implements lifecycle.Component.
func (r *Relay) Name() string { return "outbox-relay" }

// Run loops until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	r.defaults()
	telemetry.Gauge("outbox_unpublished", "Outbox rows waiting to be published", func(context.Context) int64 { return r.unpublished.Load() })

	failures := 0
	lastCleanup := time.Time{}
	for ctx.Err() == nil {
		n, err := r.RunOnce(ctx)
		if _, rerr := r.ReconcilePayments(ctx); rerr != nil && err == nil {
			err = rerr
		}
		if c, cerr := r.Outbox.CountUnpublished(ctx); cerr == nil {
			r.unpublished.Store(c)
		}
		if time.Since(lastCleanup) > 10*time.Minute {
			lastCleanup = time.Now()
			if deleted, cerr := r.Outbox.Cleanup(ctx, r.Retention); cerr != nil {
				slog.WarnContext(ctx, "outbox cleanup failed", "error", cerr.Error())
			} else if deleted > 0 {
				slog.InfoContext(ctx, "outbox cleanup", "deleted", deleted)
			}
		}

		var wait time.Duration
		switch {
		case err != nil:
			failures++
			// Exponential backoff, capped: a database or EventBridge outage must not
			// turn this loop into a hot retry loop.
			wait = min(time.Second<<min(failures, 5), 30*time.Second)
			slog.ErrorContext(ctx, "relay cycle failed", "error", err.Error(), "retry_in", wait.String())
		case n >= r.Batch:
			failures, wait = 0, 0 // a full batch means more is waiting: go again immediately
		default:
			failures, wait = 0, r.Poll
		}
		if wait > 0 {
			select {
			case <-time.After(wait):
			case <-ctx.Done():
			}
		}
	}
	return nil
}
