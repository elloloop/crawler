// Package redis provides a Redis-backed State implementation.
package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"github.com/elloloop/crawler/internal/state"
)

type redisState struct {
	client *redis.Client
}

// New creates a Redis-backed state store.
func New(addr, password string) (state.State, error) {
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

	return &redisState{client: client}, nil
}

// --- Key helpers ---

func projectKey(id string) string      { return "project:" + id }
func projectsSetKey() string            { return "projects" }
func crawlKey(id string) string         { return "crawl:" + id }
func projectCrawlsKey(pid string) string { return "project:" + pid + ":crawls" }
func seenKey(crawlID string) string     { return "seen:" + crawlID }
func etagKey(url string) string         { return "etag:" + url }

// --- Projects ---

func (s *redisState) SaveProject(ctx context.Context, project *crawlerv1.Project) error {
	data, err := protojson.Marshal(project)
	if err != nil {
		return fmt.Errorf("marshal project: %w", err)
	}

	pipe := s.client.Pipeline()
	pipe.HSet(ctx, projectKey(project.Id), "data", string(data))
	pipe.SAdd(ctx, projectsSetKey(), project.Id)
	_, err = pipe.Exec(ctx)
	return err
}

func (s *redisState) GetProject(ctx context.Context, id string) (*crawlerv1.Project, error) {
	data, err := s.client.HGet(ctx, projectKey(id), "data").Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("project not found: %s", id)
		}
		return nil, err
	}

	project := &crawlerv1.Project{}
	if err := protojson.Unmarshal([]byte(data), project); err != nil {
		return nil, fmt.Errorf("unmarshal project: %w", err)
	}
	return project, nil
}

func (s *redisState) ListProjects(ctx context.Context) ([]*crawlerv1.Project, error) {
	ids, err := s.client.SMembers(ctx, projectsSetKey()).Result()
	if err != nil {
		return nil, err
	}

	projects := make([]*crawlerv1.Project, 0, len(ids))
	for _, id := range ids {
		p, err := s.GetProject(ctx, id)
		if err != nil {
			continue // skip missing/corrupt entries
		}
		projects = append(projects, p)
	}
	return projects, nil
}

func (s *redisState) DeleteProject(ctx context.Context, id string) error {
	pipe := s.client.Pipeline()
	pipe.Del(ctx, projectKey(id))
	pipe.SRem(ctx, projectsSetKey(), id)
	_, err := pipe.Exec(ctx)
	return err
}

// --- Crawl jobs ---

func (s *redisState) SaveCrawlJob(ctx context.Context, job *crawlerv1.CrawlJob) error {
	data, err := protojson.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal crawl job: %w", err)
	}

	pipe := s.client.Pipeline()
	pipe.HSet(ctx, crawlKey(job.Id), "data", string(data))
	pipe.SAdd(ctx, projectCrawlsKey(job.ProjectId), job.Id)
	_, err = pipe.Exec(ctx)
	return err
}

func (s *redisState) GetCrawlJob(ctx context.Context, id string) (*crawlerv1.CrawlJob, error) {
	data, err := s.client.HGet(ctx, crawlKey(id), "data").Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("crawl job not found: %s", id)
		}
		return nil, err
	}

	job := &crawlerv1.CrawlJob{}
	if err := protojson.Unmarshal([]byte(data), job); err != nil {
		return nil, fmt.Errorf("unmarshal crawl job: %w", err)
	}
	return job, nil
}

func (s *redisState) ListCrawlJobs(ctx context.Context, projectID string) ([]*crawlerv1.CrawlJob, error) {
	ids, err := s.client.SMembers(ctx, projectCrawlsKey(projectID)).Result()
	if err != nil {
		return nil, err
	}

	jobs := make([]*crawlerv1.CrawlJob, 0, len(ids))
	for _, id := range ids {
		j, err := s.GetCrawlJob(ctx, id)
		if err != nil {
			continue
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// --- URL deduplication ---

func (s *redisState) HasSeen(ctx context.Context, crawlID string, url string) (bool, error) {
	return s.client.SIsMember(ctx, seenKey(crawlID), url).Result()
}

func (s *redisState) MarkSeen(ctx context.Context, crawlID string, url string) error {
	return s.client.SAdd(ctx, seenKey(crawlID), url).Err()
}

// --- ETag/Last-Modified cache ---

func (s *redisState) GetETag(ctx context.Context, url string) (string, string, error) {
	result, err := s.client.HGetAll(ctx, etagKey(url)).Result()
	if err != nil {
		return "", "", err
	}
	return result["etag"], result["last_modified"], nil
}

func (s *redisState) SetETag(ctx context.Context, url string, etag string, lastModified string) error {
	return s.client.HSet(ctx, etagKey(url), map[string]interface{}{
		"etag":          etag,
		"last_modified": lastModified,
	}).Err()
}

func (s *redisState) Close() error {
	return s.client.Close()
}
