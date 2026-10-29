// Package redis (reservations adapter) implements ports.RateLimiter on
// ElastiCache (Valkey/Redis): a per-user SLIDING WINDOW limiter.
//
// HEXAGONAL BOUNDARY: adapter.
//
// How the sliding window works: each allowed request adds a member to a sorted
// set `rl:<key>` scored by its timestamp. To decide, we delete members older than
// the window, count what is left, and only if count < limit add the new member.
// Compared with a fixed window ("10 per calendar minute") it cannot be gamed by
// sending 10 requests at 12:00:59 and 10 more at 12:01:00.
//
// Why a Lua script: "remove old, count, add" must be ONE atomic step. Done as
// separate commands, two concurrent requests could both count 9 and both add,
// ending at 11. Redis runs a script without interleaving other commands.
// The script uses the server clock (TIME) so pods with skewed clocks agree.
//
// Failure: any Redis error is returned to the caller, which FAILS OPEN (allows the
// request, logs, counts a metric) - see app.CreateHold.
package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/olagookundavid/gatepass-app/internal/reservations/ports"
)

var slidingWindow = goredis.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local window = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - window)
if redis.call('ZCARD', KEYS[1]) < limit then
  redis.call('ZADD', KEYS[1], now, ARGV[3])
  redis.call('PEXPIRE', KEYS[1], window)
  return 1
end
return 0
`)

// Limiter implements ports.RateLimiter.
type Limiter struct{ C goredis.Scripter }

var _ ports.RateLimiter = (*Limiter)(nil)

// Allow records the request and reports whether it is within the limit.
func (l *Limiter) Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, error) {
	var b [8]byte
	_, _ = rand.Read(b[:])
	res, err := slidingWindow.Run(ctx, l.C, []string{"rl:" + key}, window.Milliseconds(), limit, hex.EncodeToString(b[:])).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}
