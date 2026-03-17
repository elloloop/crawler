// Package local provides a filesystem-backed JSONL Storage implementation.
package local

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	crawlerv1 "github.com/elloloop/crawler/gen/go/crawler/v1"
	"github.com/elloloop/crawler/internal/storage"
	"google.golang.org/protobuf/encoding/protojson"
)

type localStorage struct {
	baseDir string
	mu      sync.Mutex
	files   map[string]*os.File
}

// New creates a JSONL filesystem storage rooted at baseDir.
func New(baseDir string) (storage.Storage, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("create storage dir: %w", err)
	}
	return &localStorage{
		baseDir: baseDir,
		files:   make(map[string]*os.File),
	}, nil
}

func (s *localStorage) WritePage(_ context.Context, page *crawlerv1.Page) error {
	f, err := s.getFile(page.CrawlId)
	if err != nil {
		return err
	}

	data, err := protojson.Marshal(page)
	if err != nil {
		return fmt.Errorf("marshal page: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := f.Write(data); err != nil {
		return err
	}
	_, err = f.WriteString("\n")
	return err
}

func (s *localStorage) ListPages(_ context.Context, crawlID string, limit int, offset int) ([]*crawlerv1.Page, error) {
	pages, err := s.readAll(crawlID)
	if err != nil {
		return nil, err
	}
	if offset >= len(pages) {
		return nil, nil
	}
	end := offset + limit
	if end > len(pages) {
		end = len(pages)
	}
	return pages[offset:end], nil
}

func (s *localStorage) GetPage(_ context.Context, crawlID string, pageURL string) (*crawlerv1.Page, error) {
	pages, err := s.readAll(crawlID)
	if err != nil {
		return nil, err
	}
	for _, p := range pages {
		if p.Url == pageURL || p.FinalUrl == pageURL {
			return p, nil
		}
	}
	return nil, fmt.Errorf("page not found: %s", pageURL)
}

func (s *localStorage) Export(_ context.Context, crawlID string, w io.Writer) error {
	path := s.filePath(crawlID)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	_, err = io.Copy(w, f)
	return err
}

func (s *localStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.files {
		f.Close()
	}
	s.files = nil
	return nil
}

func (s *localStorage) getFile(crawlID string) (*os.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f, ok := s.files[crawlID]; ok {
		return f, nil
	}
	path := s.filePath(crawlID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	s.files[crawlID] = f
	return f, nil
}

func (s *localStorage) filePath(crawlID string) string {
	return filepath.Join(s.baseDir, crawlID+".jsonl")
}

func (s *localStorage) readAll(crawlID string) ([]*crawlerv1.Page, error) {
	path := s.filePath(crawlID)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var pages []*crawlerv1.Page
	dec := json.NewDecoder(strings.NewReader(string(data)))
	for dec.More() {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			continue
		}
		page := &crawlerv1.Page{}
		if err := protojson.Unmarshal(raw, page); err != nil {
			continue
		}
		pages = append(pages, page)
	}
	return pages, nil
}
