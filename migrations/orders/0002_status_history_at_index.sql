-- Nightly sales report scans order_status_history by (status, at).
-- Expand-only change: a new index, nothing dropped or renamed.

-- +goose Up
CREATE INDEX order_status_history_status_at_idx ON order_status_history (status, at);
