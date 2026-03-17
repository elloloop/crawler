package redis

import (
	"context"
	"os"
	"testing"
	"time"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestRateLimitSerialization(t *testing.T) {
	limit := &crawlerv1.DomainRateLimit{
		Domain:            "example.com",
		RequestsPerSecond: 10.5,
		RequestsPerMinute: 600,
		MaxConcurrent:     5,
		BurstSize:         20,
	}

	data, err := protojson.Marshal(limit)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	decoded := &crawlerv1.DomainRateLimit{}
	if err := protojson.Unmarshal(data, decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Domain != limit.Domain {
		t.Errorf("domain: got %q, want %q", decoded.Domain, limit.Domain)
	}
	if decoded.RequestsPerSecond != limit.RequestsPerSecond {
		t.Errorf("rps: got %f, want %f", decoded.RequestsPerSecond, limit.RequestsPerSecond)
	}
	if decoded.BurstSize != limit.BurstSize {
		t.Errorf("burst_size: got %d, want %d", decoded.BurstSize, limit.BurstSize)
	}
}

func TestKeyGeneration(t *testing.T) {
	if got := rateBucketKey("example.com"); got != "rate:bucket:example.com" {
		t.Errorf("rateBucketKey: got %q", got)
	}
	if got := rateConfigKey("example.com"); got != "rate:config:example.com" {
		t.Errorf("rateConfigKey: got %q", got)
	}
}

func TestIntegrationLimiter(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set, skipping integration test")
	}
	password := os.Getenv("REDIS_PASSWORD")

	l, err := New(addr, password, 10.0)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer l.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	domain := "test-domain-" + t.Name() + ".com"

	t.Run("SetGetLimit", func(t *testing.T) {
		limit := &crawlerv1.DomainRateLimit{
			Domain:            domain,
			RequestsPerSecond: 5.0,
			BurstSize:         10,
		}

		if err := l.SetLimit(domain, limit); err != nil {
			t.Fatalf("SetLimit: %v", err)
		}

		got, err := l.GetLimit(domain)
		if err != nil {
			t.Fatalf("GetLimit: %v", err)
		}
		if got.RequestsPerSecond != 5.0 {
			t.Errorf("rps: got %f, want 5.0", got.RequestsPerSecond)
		}
	})

	t.Run("ListLimits", func(t *testing.T) {
		limits, err := l.ListLimits()
		if err != nil {
			t.Fatalf("ListLimits: %v", err)
		}
		found := false
		for _, lim := range limits {
			if lim.Domain == domain {
				found = true
				break
			}
		}
		if !found {
			t.Error("domain not found in list")
		}
	})

	t.Run("Acquire", func(t *testing.T) {
		if err := l.Acquire(ctx, domain); err != nil {
			t.Fatalf("Acquire: %v", err)
		}
	})

	t.Run("ResetLimit", func(t *testing.T) {
		if err := l.ResetLimit(domain); err != nil {
			t.Fatalf("ResetLimit: %v", err)
		}

		got, err := l.GetLimit(domain)
		if err != nil {
			t.Fatalf("GetLimit after reset: %v", err)
		}
		if got != nil {
			t.Errorf("expected nil limit after reset, got %v", got)
		}
	})
}
