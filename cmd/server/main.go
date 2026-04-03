package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"github.com/elloloop/crawler/internal/config"
	memorylimiter "github.com/elloloop/crawler/internal/ratelimit/memory"
	memoryqueue "github.com/elloloop/crawler/internal/queue/memory"
	"github.com/elloloop/crawler/internal/server"
	"github.com/elloloop/crawler/internal/state/sqlite"
	localstorage "github.com/elloloop/crawler/internal/storage/local"
	"github.com/elloloop/crawler/internal/strategy"
)

func main() {
	cfg := config.Load()

	// Create data directory
	dataDir := cfg.DataDir
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Fatalf("create data dir: %v", err)
	}

	// Create SQLite state
	dbPath := filepath.Join(dataDir, "crawler.db")
	st, err := sqlite.New(dbPath)
	if err != nil {
		log.Fatalf("create state: %v", err)
	}
	defer st.Close()

	// Create local storage
	pagesDir := filepath.Join(dataDir, "pages")
	stor, err := localstorage.New(pagesDir)
	if err != nil {
		log.Fatalf("create storage: %v", err)
	}
	defer stor.Close()

	// Create memory queue
	q := memoryqueue.New(100000)

	// Create memory rate limiter (default 1.0 rps)
	lim := memorylimiter.New(1.0)

	// Create server with empty strategies map
	strategies := make(map[string]strategy.Strategy)
	srv := server.New(st, stor, q, lim, strategies)

	// Create gRPC server and register services
	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcSrv := grpc.NewServer()
	reflection.Register(grpcSrv)

	crawlerv1.RegisterProjectServiceServer(grpcSrv, srv)
	crawlerv1.RegisterCrawlServiceServer(grpcSrv, srv)
	crawlerv1.RegisterRateLimitServiceServer(grpcSrv, srv)
	crawlerv1.RegisterRobotsServiceServer(grpcSrv, srv)

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		fmt.Println("\nshutting down...")
		grpcSrv.GracefulStop()
	}()

	log.Printf("crawler gRPC server listening on :%s", cfg.GRPCPort)
	if err := grpcSrv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
