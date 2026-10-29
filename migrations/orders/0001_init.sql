-- Orders database: orders, status history, idempotency keys, transactional outbox.
-- Run by `orders-api migrate` as the migration user (not the application user).
-- Grants to the application user come from default privileges in the one-time
-- bootstrap SQL (see HANDOFF.md section 4).

-- +goose Up
CREATE TABLE orders (
    id             uuid PRIMARY KEY,                -- UUIDv7
    user_id        text        NOT NULL,            -- Cognito sub
    hold_id        uuid        NOT NULL UNIQUE,     -- one order per hold, ever (defence in depth next to the consume call)
    event_id       uuid        NOT NULL,
    ticket_type_id uuid        NOT NULL,
    quantity       integer     NOT NULL CHECK (quantity BETWEEN 1 AND 8),
    amount_cents   bigint      NOT NULL CHECK (amount_cents >= 0),
    currency       char(3)     NOT NULL,
    status         text        NOT NULL CHECK (status IN ('pending_payment', 'paid', 'payment_failed', 'cancelled')),
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now(),
    version        integer     NOT NULL DEFAULT 1,
    -- Set when the payment request was put on the payments queue. NULL for
    -- pending orders older than a short grace period means "the enqueue after
    -- commit was lost": the outbox relay's reconciler sends it (see app/relay.go).
    payment_enqueued_at timestamptz
);
CREATE INDEX orders_unenqueued_idx ON orders (created_at) WHERE status = 'pending_payment' AND payment_enqueued_at IS NULL;
-- "my orders", newest first (keyset on (created_at, id) within a user).
CREATE INDEX orders_user_created_idx ON orders (user_id, created_at DESC, id DESC);
-- nightly report scans yesterday's orders.
CREATE INDEX orders_created_idx ON orders (created_at);

CREATE TABLE order_status_history (
    id       bigserial PRIMARY KEY,
    order_id uuid        NOT NULL REFERENCES orders (id),
    status   text        NOT NULL,
    reason   text        NOT NULL DEFAULT '',
    at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX order_status_history_order_idx ON order_status_history (order_id, id);

-- One row per (user, Idempotency-Key). Written in the SAME transaction as the
-- order, so "key exists" <=> "the order exists".
CREATE TABLE idempotency_keys (
    key           text        NOT NULL,
    user_id       text        NOT NULL,
    request_hash  text        NOT NULL,
    response_code integer     NOT NULL,
    response_body jsonb       NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    expires_at    timestamptz NOT NULL,
    PRIMARY KEY (user_id, key)
);
CREATE INDEX idempotency_keys_expires_idx ON idempotency_keys (expires_at);

-- Transactional outbox: events to publish, written in the same transaction as
-- the state change they describe. The relay publishes them to EventBridge.
CREATE TABLE outbox (
    id           uuid PRIMARY KEY,                  -- UUIDv7 == eventId of the published event
    aggregate_id uuid        NOT NULL,              -- order id
    event_type   text        NOT NULL,
    payload      jsonb       NOT NULL,              -- the event `data`
    traceparent  text        NOT NULL DEFAULT '',
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    attempts     integer     NOT NULL DEFAULT 0
);
-- Partial index: only unpublished rows are indexed, so it stays tiny however
-- many millions of rows are already published.
CREATE INDEX outbox_unpublished_idx ON outbox (created_at, id) WHERE published_at IS NULL;
