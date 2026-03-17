package server

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
)

func (s *Server) CreateProject(ctx context.Context, req *crawlerv1.CreateProjectRequest) (*crawlerv1.CreateProjectResponse, error) {
	now := timestamppb.Now()
	project := &crawlerv1.Project{
		Id:            uuid.New().String(),
		Name:          req.GetName(),
		Description:   req.GetDescription(),
		DefaultConfig: req.GetDefaultConfig(),
		SiteConfigs:   req.GetSiteConfigs(),
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	if err := s.state.SaveProject(ctx, project); err != nil {
		return nil, status.Errorf(codes.Internal, "save project: %v", err)
	}

	return &crawlerv1.CreateProjectResponse{Project: project}, nil
}

func (s *Server) GetProject(ctx context.Context, req *crawlerv1.GetProjectRequest) (*crawlerv1.GetProjectResponse, error) {
	project, err := s.state.GetProject(ctx, req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "get project: %v", err)
	}
	return &crawlerv1.GetProjectResponse{Project: project}, nil
}

func (s *Server) ListProjects(ctx context.Context, _ *crawlerv1.ListProjectsRequest) (*crawlerv1.ListProjectsResponse, error) {
	projects, err := s.state.ListProjects(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list projects: %v", err)
	}
	return &crawlerv1.ListProjectsResponse{Projects: projects}, nil
}

func (s *Server) UpdateProject(ctx context.Context, req *crawlerv1.UpdateProjectRequest) (*crawlerv1.UpdateProjectResponse, error) {
	existing, err := s.state.GetProject(ctx, req.GetId())
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "project not found: %v", err)
	}

	if req.GetName() != "" {
		existing.Name = req.GetName()
	}
	if req.GetDescription() != "" {
		existing.Description = req.GetDescription()
	}
	if req.GetDefaultConfig() != nil {
		existing.DefaultConfig = req.GetDefaultConfig()
	}
	if req.GetSiteConfigs() != nil {
		existing.SiteConfigs = req.GetSiteConfigs()
	}
	existing.UpdatedAt = timestamppb.Now()

	if err := s.state.SaveProject(ctx, existing); err != nil {
		return nil, status.Errorf(codes.Internal, "save project: %v", err)
	}

	return &crawlerv1.UpdateProjectResponse{Project: existing}, nil
}

func (s *Server) DeleteProject(ctx context.Context, req *crawlerv1.DeleteProjectRequest) (*crawlerv1.DeleteProjectResponse, error) {
	if err := s.state.DeleteProject(ctx, req.GetId()); err != nil {
		return nil, status.Errorf(codes.Internal, "delete project: %v", err)
	}
	return &crawlerv1.DeleteProjectResponse{}, nil
}
