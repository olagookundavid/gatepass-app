// Package postgres (orders adapter) implements ports.Orders and ports.Outbox on
// Amazon RDS for PostgreSQL.
//
// HEXAGONAL BOUNDARY: adapter. This is where the transactional guarantees live:
//
//   - CreateOrder: order + history + outbox + idempotency record in ONE transaction;
//   - Transition:  SELECT ... FOR UPDATE on the order row, then status + history +
//     outbox in ONE transaction. The row lock serialises concurrent changes to the
//     same order (a user cancelling while the payment worker pays);
//   - Claim:       SELECT ... FOR UPDATE SKIP LOCKED over the outbox, so several
//     relay replicas share the work without publishing a row twice at once.
//
// AWS service: RDS PostgreSQL (credentials from Secrets Manager, see platform/pgdb).
// Queries use $n placeholders only.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/olagookundavid/gatepass-app/internal/orders/domain"
	"github.com/olagookundavid/gatepass-app/internal/orders/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/ebpub"
	"github.com/olagookundavid/gatepass-app/internal/platform/pgdb"
)

// Repo implements ports.Orders and ports.Outbox.
type Repo struct{ DB *pgdb.Pool }

var (
	_ ports.Orders = (*Repo)(nil)
	_ ports.Outbox = (*Repo)(nil)
)

const orderColumns = `id::text, user_id, hold_id::text, event_id::text, ticket_type_id::text, quantity, amount_cents, currency,
	status, created_at, updated_at, version`

func scanOrder(row pgx.Row) (domain.Order, error) {
	var o domain.Order
	var st string
	err := row.Scan(&o.ID, &o.UserID, &o.HoldID, &o.EventID, &o.TicketTypeID, &o.Quantity, &o.AmountCents, &o.Currency, &st, &o.CreatedAt, &o.UpdatedAt, &o.Version)
	o.Status = domain.Status(st)
	return o, err
}

func isInvalidText(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "22P02"
}

func uniqueConstraint(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == "23505" {
		return pe.ConstraintName
	}
	return ""
}

// insertOutbox writes one outbox row. The row id is the eventId consumers dedupe on.
func insertOutbox(ctx context.Context, q pgdb.Querier, o domain.Order, eventType, reason, traceparent string, at time.Time) error {
	payload, err := json.Marshal(domain.EventDataFor(o, reason))
	if err != nil {
		return err
	}
	_, err = q.Exec(ctx, `INSERT INTO outbox (id, aggregate_id, event_type, payload, traceparent, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)`, ebpub.NewEventID(), o.ID, eventType, payload, traceparent, at)
	return err
}

func insertHistory(ctx context.Context, q pgdb.Querier, orderID string, st domain.Status, reason string, at time.Time) error {
	_, err := q.Exec(ctx, `INSERT INTO order_status_history (order_id, status, reason, at) VALUES ($1, $2, $3, $4)`, orderID, string(st), reason, at)
	return err
}

// CreateOrder writes everything or nothing.
//
// The idempotency row goes FIRST: if two identical requests race, both try to
// insert the same primary key (user_id, key); Postgres makes the second wait for
// the first to commit and then fail with a unique violation, before it has written
// anything else. The loser maps that to "replay the stored response".
func (r *Repo) CreateOrder(ctx context.Context, n ports.NewOrder) error {
	o := n.Order
	err := pgdb.InTx(ctx, r.DB, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO idempotency_keys (key, user_id, request_hash, response_code, response_body, created_at, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`, n.IdemKey, o.UserID, n.Idempotency.RequestHash, n.Idempotency.ResponseCode,
			n.Idempotency.ResponseBody, o.CreatedAt, n.IdemExpires); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO orders (id, user_id, hold_id, event_id, ticket_type_id, quantity, amount_cents, currency, status, created_at, updated_at, version)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, o.ID, o.UserID, o.HoldID, o.EventID, o.TicketTypeID, o.Quantity,
			o.AmountCents, o.Currency, string(o.Status), o.CreatedAt, o.UpdatedAt, o.Version); err != nil {
			return err
		}
		if err := insertHistory(ctx, tx, o.ID, o.Status, "", o.CreatedAt); err != nil {
			return err
		}
		return insertOutbox(ctx, tx, o, domain.EventOrderPlaced, "", n.Traceparent, o.CreatedAt)
	})
	switch uniqueConstraint(err) {
	case "":
		return err
	case "idempotency_keys_pkey":
		return domain.ErrIdempotencyInUse
	default: // orders_pkey, orders_hold_id_key
		return domain.ErrOrderExists
	}
}

// FindIdempotency returns the stored response for (user, key) if still retained.
func (r *Repo) FindIdempotency(ctx context.Context, userID, key string) (ports.IdempotencyRecord, bool, error) {
	var rec ports.IdempotencyRecord
	err := r.DB.QueryRow(ctx, `SELECT request_hash, response_code, response_body FROM idempotency_keys
		WHERE user_id = $1 AND key = $2 AND expires_at > now()`, userID, key).Scan(&rec.RequestHash, &rec.ResponseCode, &rec.ResponseBody)
	if errors.Is(err, pgx.ErrNoRows) {
		return ports.IdempotencyRecord{}, false, nil
	}
	return rec, err == nil, err
}

// Get loads one order.
func (r *Repo) Get(ctx context.Context, id string) (domain.Order, error) {
	o, err := scanOrder(r.DB.QueryRow(ctx, `SELECT `+orderColumns+` FROM orders WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) || isInvalidText(err) {
		return domain.Order{}, domain.ErrNotFound
	}
	return o, err
}

// ListByUser pages newest first with keyset pagination on (created_at, id).
func (r *Repo) ListByUser(ctx context.Context, userID string, after *ports.OrderCursor, limit int) ([]domain.Order, error) {
	var cAt, cID any
	if after != nil {
		cAt, cID = after.CreatedAt, after.ID
	}
	rows, err := r.DB.Query(ctx, `SELECT `+orderColumns+` FROM orders
		WHERE user_id = $1 AND ($2::timestamptz IS NULL OR (created_at, id) < ($2, $3::uuid))
		ORDER BY created_at DESC, id DESC LIMIT $4`, userID, cAt, cID, limit)
	if err != nil {
		if isInvalidText(err) {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()
	var out []domain.Order
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// History returns the audit trail in order.
func (r *Repo) History(ctx context.Context, id string) ([]domain.HistoryEntry, error) {
	rows, err := r.DB.Query(ctx, `SELECT status, reason, at FROM order_status_history WHERE order_id = $1 ORDER BY id`, id)
	if err != nil {
		if isInvalidText(err) {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()
	out := []domain.HistoryEntry{}
	for rows.Next() {
		var h domain.HistoryEntry
		var st string
		if err := rows.Scan(&st, &h.Reason, &h.At); err != nil {
			return nil, err
		}
		h.Status = domain.Status(st)
		out = append(out, h)
	}
	return out, rows.Err()
}

// Transition applies a state change under a row lock.
//
// Without FOR UPDATE: the payment worker reads "pending", the user cancels (and
// commits), the worker writes "paid" - the cancellation is silently overwritten
// and the OrderCancelled event contradicts the final state. With FOR UPDATE the
// second transaction waits for the first to commit, then reads the NEW status and
// the domain rules decide.
func (r *Repo) Transition(ctx context.Context, id string, traceparent string, decide func(domain.Order) (domain.Change, error)) (domain.Order, bool, error) {
	var out domain.Order
	changed := false
	err := pgdb.InTx(ctx, r.DB, func(tx pgx.Tx) error {
		cur, err := scanOrder(tx.QueryRow(ctx, `SELECT `+orderColumns+` FROM orders WHERE id = $1 FOR UPDATE`, id))
		if errors.Is(err, pgx.ErrNoRows) || isInvalidText(err) {
			return domain.ErrNotFound
		}
		if err != nil {
			return err
		}
		out = cur
		ch, err := decide(cur)
		if err != nil || !ch.Changed {
			return err
		}
		// clock_timestamp() is the database's wall clock AT THIS INSTANT, i.e. after
		// FOR UPDATE finished waiting. (now() would be the transaction START, before
		// the wait, and could order a later commit before an earlier one.) Using one
		// clock for every writer also removes clock skew between pods.
		var now time.Time
		if err := tx.QueryRow(ctx, `UPDATE orders SET status = $2, updated_at = clock_timestamp(), version = version + 1
			WHERE id = $1 RETURNING updated_at`, id, string(ch.NewStatus)).Scan(&now); err != nil {
			return err
		}
		out.Status, out.UpdatedAt, out.Version = ch.NewStatus, now, cur.Version+1
		if err := insertHistory(ctx, tx, id, ch.NewStatus, ch.Reason, now); err != nil {
			return err
		}
		changed = true
		return insertOutbox(ctx, tx, out, ch.EventType, ch.Reason, traceparent, now)
	})
	if err != nil {
		return domain.Order{}, false, err
	}
	return out, changed, nil
}

// MarkPaymentEnqueued records that the payment request was queued.
func (r *Repo) MarkPaymentEnqueued(ctx context.Context, id string, at time.Time) error {
	_, err := r.DB.Exec(ctx, `UPDATE orders SET payment_enqueued_at = $2 WHERE id = $1`, id, at)
	return err
}

// ---- outbox ----------------------------------------------------------------------

// Claim implements the relay's critical section (see app/relay.go).
func (r *Repo) Claim(ctx context.Context, batch int, fn func([]ports.OutboxRow) ([]string, []string, error)) (int, error) {
	claimed := 0
	err := pgdb.InTx(ctx, r.DB, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id::text, aggregate_id::text, event_type, payload, traceparent, created_at, attempts
			FROM outbox WHERE published_at IS NULL ORDER BY created_at, id LIMIT $1 FOR UPDATE SKIP LOCKED`, batch)
		if err != nil {
			return err
		}
		var list []ports.OutboxRow
		for rows.Next() {
			var x ports.OutboxRow
			if err := rows.Scan(&x.ID, &x.AggregateID, &x.EventType, &x.Payload, &x.Traceparent, &x.CreatedAt, &x.Attempts); err != nil {
				rows.Close()
				return err
			}
			list = append(list, x)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		claimed = len(list)
		if claimed == 0 {
			return nil
		}
		published, failed, err := fn(list)
		if err != nil {
			return err
		}
		if len(published) > 0 {
			if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now(), attempts = attempts + 1 WHERE id = ANY($1::uuid[])`, published); err != nil {
				return err
			}
		}
		if len(failed) > 0 {
			if _, err := tx.Exec(ctx, `UPDATE outbox SET attempts = attempts + 1 WHERE id = ANY($1::uuid[])`, failed); err != nil {
				return err
			}
		}
		return nil
	})
	return claimed, err
}

// CountUnpublished feeds the outbox_unpublished gauge (uses the partial index).
func (r *Repo) CountUnpublished(ctx context.Context) (int64, error) {
	var n int64
	err := r.DB.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
	return n, err
}

// Cleanup removes old published rows and expired idempotency keys in small
// batches (a huge DELETE would hold locks and bloat the WAL).
func (r *Repo) Cleanup(ctx context.Context, retention time.Duration) (int64, error) {
	var total int64
	tag, err := r.DB.Exec(ctx, `DELETE FROM outbox WHERE id IN (
		SELECT id FROM outbox WHERE published_at IS NOT NULL AND published_at < now() - make_interval(secs => $1) LIMIT 1000)`, retention.Seconds())
	if err != nil {
		return total, err
	}
	total += tag.RowsAffected()
	tag, err = r.DB.Exec(ctx, `DELETE FROM idempotency_keys WHERE (user_id, key) IN (
		SELECT user_id, key FROM idempotency_keys WHERE expires_at < now() LIMIT 1000)`)
	if err != nil {
		return total, err
	}
	return total + tag.RowsAffected(), nil
}

// ClaimUnenqueuedPayments finds pending orders whose payment request was never
// recorded as queued (see Service.CreateOrder step 4).
func (r *Repo) ClaimUnenqueuedPayments(ctx context.Context, olderThan time.Duration, batch int, fn func([]domain.PaymentRequest) ([]string, error)) (int, error) {
	claimed := 0
	err := pgdb.InTx(ctx, r.DB, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id::text, amount_cents FROM orders
			WHERE status = 'pending_payment' AND payment_enqueued_at IS NULL AND created_at < now() - make_interval(secs => $1)
			ORDER BY created_at LIMIT $2 FOR UPDATE SKIP LOCKED`, olderThan.Seconds(), batch)
		if err != nil {
			return err
		}
		var reqs []domain.PaymentRequest
		for rows.Next() {
			req := domain.PaymentRequest{Attempt: 1}
			if err := rows.Scan(&req.OrderID, &req.AmountCents); err != nil {
				rows.Close()
				return err
			}
			reqs = append(reqs, req)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		claimed = len(reqs)
		if claimed == 0 {
			return nil
		}
		done, err := fn(reqs)
		if err != nil {
			return err
		}
		if len(done) > 0 {
			_, err = tx.Exec(ctx, `UPDATE orders SET payment_enqueued_at = now() WHERE id = ANY($1::uuid[])`, done)
		}
		return err
	})
	return claimed, err
}
