-- Catalog database: venues, events, ticket types.
-- Run by `catalog-api migrate` as the migration user (not the application user).
-- Application-user grants come from default privileges set in the one-time
-- bootstrap SQL (see HANDOFF.md section 4), so this file contains no role names.

-- +goose Up
CREATE TABLE venues (
    id         uuid PRIMARY KEY,                 -- UUIDv7 (time ordered)
    name       text        NOT NULL,
    city       text        NOT NULL,
    address    text        NOT NULL,
    capacity   integer     NOT NULL CHECK (capacity > 0),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX venues_city_idx ON venues (lower(city));

CREATE TABLE events (
    id          uuid PRIMARY KEY,                -- UUIDv7
    venue_id    uuid        NOT NULL REFERENCES venues (id),
    title       text        NOT NULL,
    description text        NOT NULL DEFAULT '',
    starts_at   timestamptz NOT NULL,
    ends_at     timestamptz NOT NULL,
    status      text        NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'published', 'cancelled')),
    image_url   text        NOT NULL DEFAULT '',
    created_by  text        NOT NULL,            -- Cognito sub of the admin
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    version     integer     NOT NULL DEFAULT 1,  -- optimistic locking (If-Match)
    CHECK (ends_at > starts_at)
);
-- Listing published events by time (keyset pagination on starts_at, id).
CREATE INDEX events_status_starts_idx ON events (status, starts_at, id);
CREATE INDEX events_venue_idx ON events (venue_id);

CREATE TABLE ticket_types (
    id          uuid PRIMARY KEY,
    event_id    uuid    NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    name        text    NOT NULL,
    price_cents bigint  NOT NULL CHECK (price_cents >= 0),
    currency    char(3) NOT NULL,
    capacity    integer NOT NULL CHECK (capacity > 0)
);
CREATE INDEX ticket_types_event_idx ON ticket_types (event_id);
