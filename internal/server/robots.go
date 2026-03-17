package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
)

func (s *Server) GetRobotsInfo(_ context.Context, _ *crawlerv1.GetRobotsInfoRequest) (*crawlerv1.GetRobotsInfoResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *Server) RefreshRobots(_ context.Context, _ *crawlerv1.RefreshRobotsRequest) (*crawlerv1.RefreshRobotsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *Server) ListCachedRobots(_ context.Context, _ *crawlerv1.ListCachedRobotsRequest) (*crawlerv1.ListCachedRobotsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}
