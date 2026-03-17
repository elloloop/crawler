// Package r2 provides an S3-compatible (Cloudflare R2) Storage implementation.
package r2

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/protobuf/encoding/protojson"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"github.com/elloloop/crawler/internal/storage"
)

type r2Storage struct {
	client *s3.Client
	bucket string
}

// New creates an S3-compatible storage client for Cloudflare R2 (or any S3-compatible endpoint).
func New(endpoint, accessKeyID, secretAccessKey, bucket, region string) (storage.Storage, error) {
	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       region,
		Credentials:  credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, ""),
		UsePathStyle: true,
	})

	return &r2Storage{
		client: client,
		bucket: bucket,
	}, nil
}

// objectKey computes the S3 key for a page: {project_id}/{crawl_id}/{sha256(url)}.json
func objectKey(projectID, crawlID, pageURL string) string {
	h := sha256.Sum256([]byte(pageURL))
	return fmt.Sprintf("%s/%s/%x.json", projectID, crawlID, h)
}

// ObjectKey is exported for testing key generation.
func ObjectKey(projectID, crawlID, pageURL string) string {
	return objectKey(projectID, crawlID, pageURL)
}

// crawlPrefix returns the S3 prefix for listing pages of a crawl.
func crawlPrefix(projectID, crawlID string) string {
	return fmt.Sprintf("%s/%s/", projectID, crawlID)
}

func (s *r2Storage) WritePage(ctx context.Context, page *crawlerv1.Page) error {
	data, err := protojson.Marshal(page)
	if err != nil {
		return fmt.Errorf("marshal page: %w", err)
	}

	key := objectKey(page.ProjectId, page.CrawlId, page.Url)
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}
	return nil
}

func (s *r2Storage) ListPages(ctx context.Context, crawlID string, limit int, offset int) ([]*crawlerv1.Page, error) {
	// We need to discover the project ID from the objects. We list with a partial
	// prefix scan. Since we store as {project_id}/{crawl_id}/..., we list all
	// objects and filter by crawl_id in the key path.
	// A more efficient approach: list all objects, filtering by crawlID segment.
	var pages []*crawlerv1.Page
	var continuationToken *string
	skipped := 0

	for {
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			ContinuationToken: continuationToken,
			MaxKeys:           aws.Int32(1000),
		}

		output, err := s.client.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("list objects: %w", err)
		}

		for _, obj := range output.Contents {
			key := aws.ToString(obj.Key)
			// Key format: {project_id}/{crawl_id}/{hash}.json
			parts := strings.SplitN(key, "/", 3)
			if len(parts) != 3 || parts[1] != crawlID {
				continue
			}

			if skipped < offset {
				skipped++
				continue
			}

			page, err := s.getObjectAsPage(ctx, key)
			if err != nil {
				continue
			}
			pages = append(pages, page)

			if len(pages) >= limit {
				return pages, nil
			}
		}

		if !aws.ToBool(output.IsTruncated) {
			break
		}
		continuationToken = output.NextContinuationToken
	}

	return pages, nil
}

func (s *r2Storage) GetPage(ctx context.Context, crawlID string, pageURL string) (*crawlerv1.Page, error) {
	// We need the project ID to compute the key. Since we don't have it directly,
	// we search for the object by listing with crawlID and matching the URL hash.
	urlHash := fmt.Sprintf("%x", sha256.Sum256([]byte(pageURL)))

	var continuationToken *string
	for {
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			ContinuationToken: continuationToken,
			MaxKeys:           aws.Int32(1000),
		}

		output, err := s.client.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("list objects: %w", err)
		}

		for _, obj := range output.Contents {
			key := aws.ToString(obj.Key)
			parts := strings.SplitN(key, "/", 3)
			if len(parts) != 3 || parts[1] != crawlID {
				continue
			}
			if !strings.HasPrefix(parts[2], urlHash) {
				continue
			}
			return s.getObjectAsPage(ctx, key)
		}

		if !aws.ToBool(output.IsTruncated) {
			break
		}
		continuationToken = output.NextContinuationToken
	}

	return nil, fmt.Errorf("page not found: %s", pageURL)
}

func (s *r2Storage) Export(ctx context.Context, crawlID string, w io.Writer) error {
	var continuationToken *string

	for {
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			ContinuationToken: continuationToken,
			MaxKeys:           aws.Int32(1000),
		}

		output, err := s.client.ListObjectsV2(ctx, input)
		if err != nil {
			return fmt.Errorf("list objects: %w", err)
		}

		for _, obj := range output.Contents {
			key := aws.ToString(obj.Key)
			parts := strings.SplitN(key, "/", 3)
			if len(parts) != 3 || parts[1] != crawlID {
				continue
			}

			page, err := s.getObjectAsPage(ctx, key)
			if err != nil {
				continue
			}
			data, err := protojson.Marshal(page)
			if err != nil {
				continue
			}
			if _, err := w.Write(data); err != nil {
				return err
			}
			if _, err := w.Write([]byte("\n")); err != nil {
				return err
			}
		}

		if !aws.ToBool(output.IsTruncated) {
			break
		}
		continuationToken = output.NextContinuationToken
	}

	return nil
}

func (s *r2Storage) Close() error {
	// The S3 client does not need explicit closing.
	return nil
}

func (s *r2Storage) getObjectAsPage(ctx context.Context, key string) (*crawlerv1.Page, error) {
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get object %s: %w", key, err)
	}
	defer output.Body.Close()

	data, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, fmt.Errorf("read object %s: %w", key, err)
	}

	page := &crawlerv1.Page{}
	if err := protojson.Unmarshal(data, page); err != nil {
		return nil, fmt.Errorf("unmarshal page %s: %w", key, err)
	}
	return page, nil
}
