// Package redis (catalog adapter) implements ports.Cache on Amazon ElastiCache
// (Valkey/Redis) with go-redis.
//
// HEXAGONAL BOUNDARY: adapter. The application sees only ports.Cache and treats
// every error as "cache unavailable".
//
// Keys:
//
//	catalog:event:<eventId>            JSON of domain.Event            TTL 5 min
//	catalog:events:list:g<gen>:...     JSON of a first list page       TTL 60 s
//	catalog:events:gen                 integer, bumped to invalidate all list pages
//
// Why JSON strings and not Redis hashes: the value is always read and written as
// a whole, and the same bytes can be returned unchanged.
//
// AWS service touched: ElastiCache. IAM (only with IAM auth): elasticache:Connect.
// Failure looks like: dial timeout / i/o timeout (cache down or security group),
// WRONGPASS (bad token), connection reset (failover). All are returned as errors.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/olagookundavid/gatepass-app/internal/catalog/domain"
	"github.com/olagookundavid/gatepass-app/internal/catalog/ports"
)

const genKey = "catalog:events:gen"

// Cache implements ports.Cache.
type Cache struct{ C goredis.UniversalClient }

var _ ports.Cache = (*Cache)(nil)

func eventKey(id string) string { return "catalog:event:" + id }

// GetEvent returns (nil,false,nil) on a miss.
func (c *Cache) GetEvent(ctx context.Context, id string) (*domain.Event, bool, error) {
	b, err := c.C.Get(ctx, eventKey(id)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var e domain.Event
	if err := json.Unmarshal(b, &e); err != nil {
		// A corrupt entry (e.g. from an older schema) is treated as a miss.
		return nil, false, nil
	}
	return &e, true, nil
}

// SetEvent stores the event with a TTL (a cache without TTL is a memory leak
// and a staleness bug waiting to happen).
func (c *Cache) SetEvent(ctx context.Context, e domain.Event, ttl time.Duration) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return c.C.Set(ctx, eventKey(e.ID), b, ttl).Err()
}

// DeleteEvent removes one event.
func (c *Cache) DeleteEvent(ctx context.Context, id string) error {
	return c.C.Del(ctx, eventKey(id)).Err()
}

// GetList returns the cached page bytes.
func (c *Cache) GetList(ctx context.Context, key string) ([]byte, bool, error) {
	b, err := c.C.Get(ctx, key).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, false, nil
	}
	return b, err == nil, err
}

// SetList stores a page with a TTL.
func (c *Cache) SetList(ctx context.Context, key string, payload []byte, ttl time.Duration) error {
	return c.C.Set(ctx, key, payload, ttl).Err()
}

// ListGeneration reads the current generation (0 when never invalidated).
func (c *Cache) ListGeneration(ctx context.Context) (int64, error) {
	s, err := c.C.Get(ctx, genKey).Result()
	if errors.Is(err, goredis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(s, 10, 64)
}

// InvalidateLists atomically increments the generation (INCR is atomic in
// Redis, so concurrent invalidations never lose an increment).
func (c *Cache) InvalidateLists(ctx context.Context) error {
	return c.C.Incr(ctx, genKey).Err()
}
