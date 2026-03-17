// Package redis provides a Redis-backed distributed rate limiter.
package redis

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"github.com/elloloop/crawler/internal/ratelimit"
)

// tokenBucketScript is a Lua script that atomically performs a token bucket check.
// KEYS[1] = rate bucket key
// ARGV[1] = max tokens (burst)
// ARGV[2] = refill rate (tokens per second)
// ARGV[3] = current time in microseconds
// Returns: 0 if allowed, or wait time in microseconds.
var tokenBucketScript = redis.NewScript(`
local key = KEYS[1]
local max_tokens = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local bucket = redis.call('HMGET', key, 'tokens', 'last_time')
local tokens = tonumber(bucket[1])
local last_time = tonumber(bucket[2])

if tokens == nil then
    tokens = max_tokens
    last_time = now
end

local elapsed = (now - last_time) / 1000000.0
local new_tokens = tokens + elapsed * refill_rate
if new_tokens > max_tokens then
    new_tokens = max_tokens
end

if new_tokens >= 1 then
    new_tokens = new_tokens - 1
    redis.call('HMSET', key, 'tokens', tostring(new_tokens), 'last_time', tostring(now))
    redis.call('EXPIRE', key, 300)
    return 0
else
    local wait = (1 - new_tokens) / refill_rate * 1000000
    redis.call('HMSET', key, 'tokens', tostring(new_tokens), 'last_time', tostring(now))
    redis.call('EXPIRE', key, 300)
    return math.ceil(wait)
end
`)

type redisLimiter struct {
	client     *redis.Client
	defaultRPS float64
}

// New creates a Redis-backed distributed rate limiter.
func New(addr, password string, defaultRPS float64) (ratelimit.Limiter, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return &redisLimiter{
		client:     client,
		defaultRPS: defaultRPS,
	}, nil
}

func rateBucketKey(domain string) string { return "rate:bucket:" + domain }
func rateConfigKey(domain string) string { return "rate:config:" + domain }

func (l *redisLimiter) Acquire(ctx context.Context, domain string) error {
	rps, burst := l.getEffectiveRate(domain)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		now := time.Now().UnixMicro()
		result, err := tokenBucketScript.Run(ctx, l.client, []string{rateBucketKey(domain)},
			burst, rps, now,
		).Int64()

		if err != nil {
			// Graceful fallback: if Redis errors, allow the request and log warning
			log.Printf("WARN: rate limiter redis error for %s, allowing request: %v", domain, err)
			return nil
		}

		if result == 0 {
			return nil
		}

		// Wait for the returned duration
		waitDur := time.Duration(result) * time.Microsecond
		timer := time.NewTimer(waitDur)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (l *redisLimiter) getEffectiveRate(domain string) (rps float64, burst int) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	limit, err := l.GetLimit(domain)
	if err != nil || limit == nil || limit.RequestsPerSecond == 0 {
		rps = l.defaultRPS
	} else {
		rps = limit.RequestsPerSecond
	}

	if limit != nil && limit.BurstSize > 0 {
		burst = int(limit.BurstSize)
	} else {
		burst = int(rps)
		if burst < 1 {
			burst = 1
		}
	}

	_ = ctx // consumed by GetLimit via its own context
	return rps, burst
}

func (l *redisLimiter) SetLimit(domain string, limit *crawlerv1.DomainRateLimit) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	data, err := protojson.Marshal(limit)
	if err != nil {
		return fmt.Errorf("marshal rate limit: %w", err)
	}

	return l.client.HSet(ctx, rateConfigKey(domain), "data", string(data)).Err()
}

func (l *redisLimiter) GetLimit(domain string) (*crawlerv1.DomainRateLimit, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	data, err := l.client.HGet(ctx, rateConfigKey(domain), "data").Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	limit := &crawlerv1.DomainRateLimit{}
	if err := protojson.Unmarshal([]byte(data), limit); err != nil {
		return nil, fmt.Errorf("unmarshal rate limit: %w", err)
	}
	return limit, nil
}

func (l *redisLimiter) ListLimits() ([]*crawlerv1.DomainRateLimit, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var limits []*crawlerv1.DomainRateLimit
	var cursor uint64
	for {
		keys, nextCursor, err := l.client.Scan(ctx, cursor, "rate:config:*", 100).Result()
		if err != nil {
			return nil, fmt.Errorf("scan rate configs: %w", err)
		}
		for _, key := range keys {
			data, err := l.client.HGet(ctx, key, "data").Result()
			if err != nil {
				continue
			}
			limit := &crawlerv1.DomainRateLimit{}
			if err := protojson.Unmarshal([]byte(data), limit); err != nil {
				continue
			}
			limits = append(limits, limit)
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return limits, nil
}

func (l *redisLimiter) ResetLimit(domain string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pipe := l.client.Pipeline()
	pipe.Del(ctx, rateConfigKey(domain))
	pipe.Del(ctx, rateBucketKey(domain))
	_, err := pipe.Exec(ctx)
	return err
}

func (l *redisLimiter) Close() error {
	return l.client.Close()
}
