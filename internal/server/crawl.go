package server

import (
	"context"
	"net/url"
	"strconv"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"github.com/elloloop/crawler/internal/worker"
)

func (s *Server) StartCrawl(ctx context.Context, req *crawlerv1.StartCrawlRequest) (*crawlerv1.StartCrawlResponse, error) {
	// Get project to access config
	project, err := s.state.GetProject(ctx, req.GetProjectId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "project not found: %v", err)
	}

	// Use config override if provided, otherwise use project default
	cfg := req.GetConfigOverride()
	if cfg == nil {
		cfg = project.GetDefaultConfig()
	}
	if cfg == nil {
		return nil, status.Errorf(codes.InvalidArgument, "no crawl config provided and project has no default config")
	}

	crawlID := uuid.New().String()
	now := timestamppb.Now()

	job := &crawlerv1.CrawlJob{
		Id:        crawlID,
		ProjectId: req.GetProjectId(),
		Config:    cfg,
		Status:    crawlerv1.CrawlStatus_CRAWL_STATUS_RUNNING,
		Stats:     &crawlerv1.CrawlStats{},
		StartedAt: now,
	}

	if err := s.state.SaveCrawlJob(ctx, job); err != nil {
		return nil, status.Errorf(codes.Internal, "save crawl job: %v", err)
	}

	// Build a domain -> strategy map from project SiteConfigs
	domainStrategy := make(map[string]crawlerv1.CrawlStrategy)
	for _, sc := range project.GetSiteConfigs() {
		if sc.GetDomain() != "" && sc.GetStrategy() != crawlerv1.CrawlStrategy_CRAWL_STRATEGY_UNSPECIFIED {
			domainStrategy[sc.GetDomain()] = sc.GetStrategy()
		}
	}

	// Push seed URLs to queue
	defaultStrategy := cfg.GetStrategy()
	for _, seedURL := range cfg.GetSeedUrls() {
		strat := defaultStrategy
		// Check if there's a domain-specific strategy
		if u, err := url.Parse(seedURL); err == nil && u.Host != "" {
			if ds, ok := domainStrategy[u.Host]; ok {
				strat = ds
			}
		}

		event := &crawlerv1.URLFrontierEvent{
			Url:       seedURL,
			Depth:     0,
			ProjectId: req.GetProjectId(),
			CrawlId:   crawlID,
			Strategy:  strat,
		}
		if err := s.queue.Push(ctx, event); err != nil {
			return nil, status.Errorf(codes.Internal, "push to queue: %v", err)
		}
	}

	// Start worker pool
	concurrency := int(cfg.GetConcurrency())
	if concurrency <= 0 {
		concurrency = 4
	}

	s.poolMu.Lock()
	s.pool = worker.New(s.queue, s.storage, s.state, s.limiter, s.strategies, concurrency)
	s.pool.Start(context.Background())
	s.poolMu.Unlock()

	return &crawlerv1.StartCrawlResponse{Job: job}, nil
}

func (s *Server) StopCrawl(ctx context.Context, req *crawlerv1.StopCrawlRequest) (*crawlerv1.StopCrawlResponse, error) {
	job, err := s.state.GetCrawlJob(ctx, req.GetCrawlId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "crawl job not found: %v", err)
	}

	s.poolMu.Lock()
	if s.pool != nil {
		s.pool.Stop()
		s.pool = nil
	}
	s.poolMu.Unlock()

	job.Status = crawlerv1.CrawlStatus_CRAWL_STATUS_CANCELLED
	job.CompletedAt = timestamppb.Now()

	if err := s.state.SaveCrawlJob(ctx, job); err != nil {
		return nil, status.Errorf(codes.Internal, "save crawl job: %v", err)
	}

	return &crawlerv1.StopCrawlResponse{Job: job}, nil
}

func (s *Server) PauseCrawl(ctx context.Context, req *crawlerv1.PauseCrawlRequest) (*crawlerv1.PauseCrawlResponse, error) {
	job, err := s.state.GetCrawlJob(ctx, req.GetCrawlId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "crawl job not found: %v", err)
	}

	s.poolMu.Lock()
	if s.pool != nil {
		s.pool.Stop()
		s.pool = nil
	}
	s.poolMu.Unlock()

	job.Status = crawlerv1.CrawlStatus_CRAWL_STATUS_PAUSED

	if err := s.state.SaveCrawlJob(ctx, job); err != nil {
		return nil, status.Errorf(codes.Internal, "save crawl job: %v", err)
	}

	return &crawlerv1.PauseCrawlResponse{Job: job}, nil
}

func (s *Server) ResumeCrawl(ctx context.Context, req *crawlerv1.ResumeCrawlRequest) (*crawlerv1.ResumeCrawlResponse, error) {
	job, err := s.state.GetCrawlJob(ctx, req.GetCrawlId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "crawl job not found: %v", err)
	}

	job.Status = crawlerv1.CrawlStatus_CRAWL_STATUS_RUNNING

	if err := s.state.SaveCrawlJob(ctx, job); err != nil {
		return nil, status.Errorf(codes.Internal, "save crawl job: %v", err)
	}

	return &crawlerv1.ResumeCrawlResponse{Job: job}, nil
}

func (s *Server) GetCrawlStatus(ctx context.Context, req *crawlerv1.GetCrawlStatusRequest) (*crawlerv1.GetCrawlStatusResponse, error) {
	job, err := s.state.GetCrawlJob(ctx, req.GetCrawlId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "crawl job not found: %v", err)
	}
	return &crawlerv1.GetCrawlStatusResponse{Job: job}, nil
}

func (s *Server) ListCrawls(ctx context.Context, req *crawlerv1.ListCrawlsRequest) (*crawlerv1.ListCrawlsResponse, error) {
	jobs, err := s.state.ListCrawlJobs(ctx, req.GetProjectId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list crawl jobs: %v", err)
	}
	return &crawlerv1.ListCrawlsResponse{Jobs: jobs}, nil
}

func (s *Server) GetCrawlStats(ctx context.Context, req *crawlerv1.GetCrawlStatsRequest) (*crawlerv1.GetCrawlStatsResponse, error) {
	s.poolMu.Lock()
	pool := s.pool
	s.poolMu.Unlock()

	if pool != nil {
		stats := pool.GetStats()
		return &crawlerv1.GetCrawlStatsResponse{Stats: stats.Snapshot()}, nil
	}

	// Fall back to stored job stats
	job, err := s.state.GetCrawlJob(ctx, req.GetCrawlId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "crawl job not found: %v", err)
	}
	return &crawlerv1.GetCrawlStatsResponse{Stats: job.GetStats()}, nil
}

func (s *Server) ListPages(ctx context.Context, req *crawlerv1.ListPagesRequest) (*crawlerv1.ListPagesResponse, error) {
	limit := int(req.GetPageSize())
	if limit <= 0 {
		limit = 50
	}

	offset := 0
	if req.GetPageToken() != "" {
		parsed, err := strconv.Atoi(req.GetPageToken())
		if err == nil {
			offset = parsed
		}
	}

	pages, err := s.storage.ListPages(ctx, req.GetCrawlId(), limit, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list pages: %v", err)
	}

	var nextToken string
	if len(pages) == limit {
		nextToken = strconv.Itoa(offset + limit)
	}

	return &crawlerv1.ListPagesResponse{
		Pages:         pages,
		NextPageToken: nextToken,
	}, nil
}

func (s *Server) GetPage(ctx context.Context, req *crawlerv1.GetPageRequest) (*crawlerv1.GetPageResponse, error) {
	page, err := s.storage.GetPage(ctx, req.GetCrawlId(), req.GetUrl())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "page not found: %v", err)
	}
	return &crawlerv1.GetPageResponse{Page: page}, nil
}

func (s *Server) ExportPages(req *crawlerv1.ExportPagesRequest, stream grpc.ServerStreamingServer[crawlerv1.ExportPagesResponse]) error {
	const batchSize = 100
	offset := 0

	for {
		pages, err := s.storage.ListPages(stream.Context(), req.GetCrawlId(), batchSize, offset)
		if err != nil {
			return status.Errorf(codes.Internal, "list pages: %v", err)
		}
		if len(pages) == 0 {
			break
		}
		for _, page := range pages {
			if err := stream.Send(&crawlerv1.ExportPagesResponse{Page: page}); err != nil {
				return err
			}
		}
		offset += len(pages)
		if len(pages) < batchSize {
			break
		}
	}
	return nil
}
