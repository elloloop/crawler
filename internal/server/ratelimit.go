package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
)

func (s *Server) SetDomainRateLimit(_ context.Context, req *crawlerv1.SetDomainRateLimitRequest) (*crawlerv1.SetDomainRateLimitResponse, error) {
	rl := req.GetRateLimit()
	if rl == nil {
		return nil, status.Errorf(codes.InvalidArgument, "rate_limit is required")
	}
	if err := s.limiter.SetLimit(rl.GetDomain(), rl); err != nil {
		return nil, status.Errorf(codes.Internal, "set rate limit: %v", err)
	}
	return &crawlerv1.SetDomainRateLimitResponse{RateLimit: rl}, nil
}

func (s *Server) GetDomainRateLimit(_ context.Context, req *crawlerv1.GetDomainRateLimitRequest) (*crawlerv1.GetDomainRateLimitResponse, error) {
	rl, err := s.limiter.GetLimit(req.GetDomain())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get rate limit: %v", err)
	}
	return &crawlerv1.GetDomainRateLimitResponse{RateLimit: rl}, nil
}

func (s *Server) ListDomainRateLimits(_ context.Context, _ *crawlerv1.ListDomainRateLimitsRequest) (*crawlerv1.ListDomainRateLimitsResponse, error) {
	limits, err := s.limiter.ListLimits()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list rate limits: %v", err)
	}
	return &crawlerv1.ListDomainRateLimitsResponse{RateLimits: limits}, nil
}

func (s *Server) ResetDomainRateLimit(_ context.Context, req *crawlerv1.ResetDomainRateLimitRequest) (*crawlerv1.ResetDomainRateLimitResponse, error) {
	if err := s.limiter.ResetLimit(req.GetDomain()); err != nil {
		return nil, status.Errorf(codes.Internal, "reset rate limit: %v", err)
	}
	return &crawlerv1.ResetDomainRateLimitResponse{}, nil
}
