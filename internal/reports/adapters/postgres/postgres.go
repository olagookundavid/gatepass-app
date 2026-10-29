// Package postgres (reports adapter) implements ports.SalesSource against the
// orders database, connecting as the READ-ONLY user `orders_report` with RDS IAM
// authentication (see platform/pgdb.IAMAuth).
//
// HEXAGONAL BOUNDARY: adapter.
//
// "Sales of a day" = orders whose transition to `paid` happened in the window.
// The paid moment comes from order_status_history (the order row only knows its
// CURRENT status). Index used: order_status_history_status_at_idx (migration 0002).
//
// IAM: rds-db:connect on arn:aws:rds-db:<region>:<account-id>:dbuser:<db-resource-id>/orders_report.
// The DB user has SELECT only; a bug or injection here cannot change data.
package postgres

import (
	"context"

	"github.com/olagookundavid/gatepass-app/internal/platform/pgdb"
	"github.com/olagookundavid/gatepass-app/internal/reports/domain"
	"github.com/olagookundavid/gatepass-app/internal/reports/ports"
)

// Source implements ports.SalesSource.
type Source struct{ DB *pgdb.Pool }

var _ ports.SalesSource = (*Source)(nil)

// SalesBetween aggregates paid orders per event and currency.
func (s *Source) SalesBetween(ctx context.Context, w domain.Window) ([]domain.SalesRow, error) {
	rows, err := s.DB.Query(ctx, `
		SELECT o.event_id::text, o.currency,
		       count(*)                                  AS orders,
		       COALESCE(sum(o.quantity), 0)              AS tickets,
		       COALESCE(sum(o.amount_cents), 0)          AS gross_cents,
		       count(*) FILTER (WHERE o.status = 'paid') AS still_paid
		FROM order_status_history h
		JOIN orders o ON o.id = h.order_id
		WHERE h.status = 'paid' AND h.at >= $1 AND h.at < $2
		GROUP BY o.event_id, o.currency
		ORDER BY o.event_id, o.currency`, w.Start, w.End)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.SalesRow
	for rows.Next() {
		var r domain.SalesRow
		if err := rows.Scan(&r.EventID, &r.Currency, &r.Orders, &r.Tickets, &r.GrossCents, &r.StillPaidCount); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
