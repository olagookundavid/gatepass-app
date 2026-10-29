// Package postgres (catalog adapter) implements ports.EventRepository and
// ports.VenueRepository on Amazon RDS for PostgreSQL through pgx.
//
// HEXAGONAL BOUNDARY: this is OUTSIDE the hexagon. It knows SQL; the domain and
// application do not. It translates database facts into domain errors
// (no rows -> domain.ErrNotFound; 0 rows updated -> ErrVersionConflict).
//
// AWS service touched: RDS PostgreSQL. Connection and IAM-token handling live in
// platform/pgdb; queries here use only $n placeholders (never string concatenation
// of user input -> no SQL injection).
package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/olagookundavid/gatepass-app/internal/catalog/domain"
	"github.com/olagookundavid/gatepass-app/internal/catalog/ports"
	"github.com/olagookundavid/gatepass-app/internal/platform/pgdb"
)

// Repo implements ports.EventRepository; VenueRepo (below) implements ports.VenueRepository.
type Repo struct{ DB *pgdb.Pool }

var _ ports.EventRepository = (*Repo)(nil)

// eventColumns is selected in the same order Scan reads them.
const eventColumns = `e.id::text, e.venue_id::text, e.title, e.description, e.starts_at, e.ends_at, e.status,
	e.image_url, e.created_by, e.created_at, e.updated_at, e.version`

func scanEvent(row pgx.Row) (domain.Event, error) {
	var e domain.Event
	var st string
	err := row.Scan(&e.ID, &e.VenueID, &e.Title, &e.Description, &e.StartsAt, &e.EndsAt, &st,
		&e.ImageURL, &e.CreatedBy, &e.CreatedAt, &e.UpdatedAt, &e.Version)
	e.Status = domain.Status(st)
	return e, err
}

// isInvalidText reports SQLSTATE 22P02 (e.g. "abc" is not a uuid): treated as not found.
func isInvalidText(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "22P02"
}

// Create inserts event + ticket types atomically: either both exist or neither.
func (r *Repo) Create(ctx context.Context, e domain.Event) error {
	return pgdb.InTx(ctx, r.DB, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO events
			(id, venue_id, title, description, starts_at, ends_at, status, image_url, created_by, created_at, updated_at, version)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
			e.ID, e.VenueID, e.Title, e.Description, e.StartsAt, e.EndsAt, string(e.Status), e.ImageURL, e.CreatedBy,
			e.CreatedAt, e.UpdatedAt, e.Version); err != nil {
			return err
		}
		return insertTicketTypes(ctx, tx, e.ID, e.TicketTypes)
	})
}

func insertTicketTypes(ctx context.Context, q pgdb.Querier, eventID string, tts []domain.TicketType) error {
	for _, t := range tts {
		if _, err := q.Exec(ctx, `INSERT INTO ticket_types (id, event_id, name, price_cents, currency, capacity)
			VALUES ($1,$2,$3,$4,$5,$6)`, t.ID, eventID, t.Name, t.PriceCents, t.Currency, t.Capacity); err != nil {
			return err
		}
	}
	return nil
}

// Get loads one event with its ticket types.
func (r *Repo) Get(ctx context.Context, id string) (domain.Event, error) {
	return getEvent(ctx, r.DB, id)
}

func getEvent(ctx context.Context, q pgdb.Querier, id string) (domain.Event, error) {
	e, err := scanEvent(q.QueryRow(ctx, `SELECT `+eventColumns+` FROM events e WHERE e.id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) || isInvalidText(err) {
		return domain.Event{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Event{}, err
	}
	tts, err := ticketTypesFor(ctx, q, []string{id})
	if err != nil {
		return domain.Event{}, err
	}
	e.TicketTypes = tts[id]
	if e.TicketTypes == nil {
		e.TicketTypes = []domain.TicketType{}
	}
	return e, nil
}

// ticketTypesFor loads ticket types for many events in ONE query (avoids the
// classic N+1: one query per listed event).
func ticketTypesFor(ctx context.Context, q pgdb.Querier, ids []string) (map[string][]domain.TicketType, error) {
	out := map[string][]domain.TicketType{}
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := q.Query(ctx, `SELECT id::text, event_id::text, name, price_cents, currency, capacity
		FROM ticket_types WHERE event_id = ANY($1::uuid[]) ORDER BY name, id`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var t domain.TicketType
		var eventID string
		if err := rows.Scan(&t.ID, &eventID, &t.Name, &t.PriceCents, &t.Currency, &t.Capacity); err != nil {
			return nil, err
		}
		out[eventID] = append(out[eventID], t)
	}
	return out, rows.Err()
}

// Save implements optimistic locking: the UPDATE only matches when the stored
// version equals the version the caller read. Zero rows matched means either the
// event is gone or somebody else saved first; a second query tells them apart.
func (r *Repo) Save(ctx context.Context, e domain.Event, expectedVersion int, replaceTicketTypes bool) (domain.Event, error) {
	var saved domain.Event
	err := pgdb.InTx(ctx, r.DB, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE events SET venue_id=$2, title=$3, description=$4, starts_at=$5, ends_at=$6,
			status=$7, image_url=$8, updated_at=$9, version = version + 1
			WHERE id = $1 AND version = $10`,
			e.ID, e.VenueID, e.Title, e.Description, e.StartsAt, e.EndsAt, string(e.Status), e.ImageURL, e.UpdatedAt, expectedVersion)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM events WHERE id = $1)`, e.ID).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				return domain.ErrNotFound
			}
			return domain.ErrVersionConflict
		}
		if replaceTicketTypes {
			if _, err := tx.Exec(ctx, `DELETE FROM ticket_types WHERE event_id = $1`, e.ID); err != nil {
				return err
			}
			if err := insertTicketTypes(ctx, tx, e.ID, e.TicketTypes); err != nil {
				return err
			}
		}
		saved, err = getEvent(ctx, tx, e.ID)
		return err
	})
	return saved, err
}

// ListPublished uses KEYSET pagination: "give me the rows after the last one you
// saw" (WHERE (starts_at, id) > (last_starts_at, last_id)). With OFFSET the
// database would read and discard all earlier rows on every page, and rows
// inserted between requests would shift pages. Keyset uses the index
// events_status_starts_idx directly and is stable.
func (r *Repo) ListPublished(ctx context.Context, f ports.EventFilter, after *ports.EventCursor, limit int) ([]domain.Event, error) {
	var cursorStarts any
	var cursorID any
	if after != nil {
		cursorStarts, cursorID = after.StartsAt, after.ID
	}
	rows, err := r.DB.Query(ctx, `SELECT `+eventColumns+`
		FROM events e JOIN venues v ON v.id = e.venue_id
		WHERE e.status = 'published'
		  AND ($1::text = '' OR lower(v.city) = lower($1))
		  AND ($2::timestamptz IS NULL OR e.starts_at >= $2)
		  AND ($3::timestamptz IS NULL OR e.starts_at <= $3)
		  AND ($4::timestamptz IS NULL OR (e.starts_at, e.id) > ($4, $5::uuid))
		ORDER BY e.starts_at, e.id
		LIMIT $6`, f.City, f.From, f.To, cursorStarts, cursorID, limit)
	if err != nil {
		if isInvalidText(err) {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()
	var events []domain.Event
	var ids []string
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, e)
		ids = append(ids, e.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	tts, err := ticketTypesFor(ctx, r.DB, ids)
	if err != nil {
		return nil, err
	}
	for i := range events {
		events[i].TicketTypes = tts[events[i].ID]
		if events[i].TicketTypes == nil {
			events[i].TicketTypes = []domain.TicketType{}
		}
	}
	return events, nil
}

// ---- venues ----

// createVenue inserts a venue (kept separate from Create, which inserts events).
func (r *Repo) createVenue(ctx context.Context, v domain.Venue) error {
	_, err := r.DB.Exec(ctx, `INSERT INTO venues (id, name, city, address, capacity, created_at) VALUES ($1,$2,$3,$4,$5,$6)`,
		v.ID, v.Name, v.City, v.Address, v.Capacity, v.CreatedAt)
	return err
}

// VenueRepo adapts Repo to ports.VenueRepository (two repositories cannot both have a Create method on one type).
type VenueRepo struct{ R *Repo }

var _ ports.VenueRepository = VenueRepo{}

// Create implements ports.VenueRepository.
func (v VenueRepo) Create(ctx context.Context, venue domain.Venue) error {
	return v.R.createVenue(ctx, venue)
}

// Get implements ports.VenueRepository.
func (v VenueRepo) Get(ctx context.Context, id string) (domain.Venue, error) {
	var out domain.Venue
	err := v.R.DB.QueryRow(ctx, `SELECT id::text, name, city, address, capacity, created_at FROM venues WHERE id = $1`, id).
		Scan(&out.ID, &out.Name, &out.City, &out.Address, &out.Capacity, &out.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) || isInvalidText(err) {
		return domain.Venue{}, domain.ErrNotFound
	}
	return out, err
}

// List implements ports.VenueRepository (keyset on id; UUIDv7 ids sort by creation time).
func (v VenueRepo) List(ctx context.Context, after *ports.VenueCursor, limit int) ([]domain.Venue, error) {
	var cursor any
	if after != nil {
		cursor = after.ID
	}
	rows, err := v.R.DB.Query(ctx, `SELECT id::text, name, city, address, capacity, created_at FROM venues
		WHERE ($1::uuid IS NULL OR id > $1::uuid) ORDER BY id LIMIT $2`, cursor, limit)
	if err != nil {
		if isInvalidText(err) {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()
	var out []domain.Venue
	for rows.Next() {
		var x domain.Venue
		if err := rows.Scan(&x.ID, &x.Name, &x.City, &x.Address, &x.Capacity, &x.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
