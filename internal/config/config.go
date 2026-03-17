package config

import (
	"os"
	"runtime"
	"strconv"
)

type Config struct {
	// Deployment
	Mode     string // "local" or "distributed"
	GRPCPort string
	Workers  int
	DataDir  string

	// Redis (distributed mode)
	RedisAddr     string
	RedisPassword string

	// R2/S3 (distributed mode)
	R2Endpoint        string
	R2AccessKeyID     string
	R2AccessKeySecret string
	R2Bucket          string
}

func Load() *Config {
	return &Config{
		Mode:     envOr("CRAWLER_MODE", "local"),
		GRPCPort: envOr("CRAWLER_GRPC_PORT", "50051"),
		Workers:  envInt("CRAWLER_WORKERS", runtime.NumCPU()),
		DataDir:  envOr("CRAWLER_DATA_DIR", "./data"),

		RedisAddr:     envOr("REDIS_ADDR", "localhost:6379"),
		RedisPassword: envOr("REDIS_PASSWORD", ""),

		R2Endpoint:        envOr("R2_ENDPOINT", ""),
		R2AccessKeyID:     envOr("R2_ACCESS_KEY_ID", ""),
		R2AccessKeySecret: envOr("R2_ACCESS_KEY_SECRET", ""),
		R2Bucket:          envOr("R2_BUCKET", "crawler-data"),
	}
}

func (c *Config) IsLocal() bool {
	return c.Mode == "local"
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
