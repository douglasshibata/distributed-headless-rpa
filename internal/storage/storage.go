package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ArtifactType categorizes execution outputs.
type ArtifactType string

const (
	ArtifactScreenshot ArtifactType = "screenshot"
	ArtifactDOMDump    ArtifactType = "dom_dump"
	ArtifactDownload   ArtifactType = "download"
	ArtifactLog        ArtifactType = "log"
)

// ArtifactMeta records metadata about a persisted artifact.
type ArtifactMeta struct {
	TaskID    string       `json:"task_id"`
	Type      ArtifactType `json:"type"`
	FileName  string       `json:"file_name"`
	Location  string       `json:"location"`
	SizeBytes int64        `json:"size_bytes"`
	CreatedAt time.Time    `json:"created_at"`
}

// Storage manages artifact uploads and local/object storage persistence.
type Storage interface {
	SaveArtifact(taskID string, artType ArtifactType, filename string, data []byte) (string, error)
	GetArtifact(location string) ([]byte, error)
	ListArtifacts(taskID string) ([]ArtifactMeta, error)
}

// FileSystemStorage stores artifacts in a structured directory tree simulating S3 bucket paths.
type FileSystemStorage struct {
	baseDir string
	mu      sync.RWMutex
	records map[string][]ArtifactMeta
}

// NewFileSystemStorage initializes local storage root.
func NewFileSystemStorage(baseDir string) (*FileSystemStorage, error) {
	if baseDir == "" {
		baseDir = filepath.Join(os.TempDir(), "rpa-fleet-artifacts")
	}
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed creating storage dir: %w", err)
	}

	return &FileSystemStorage{
		baseDir: baseDir,
		records: make(map[string][]ArtifactMeta),
	}, nil
}

// SaveArtifact writes an artifact to disk under <baseDir>/<taskID>/<filename>.
func (s *FileSystemStorage) SaveArtifact(taskID string, artType ArtifactType, filename string, data []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Sanitize filename
	cleanName := filepath.Base(filename)
	if cleanName == "." || cleanName == "/" || cleanName == "" {
		cleanName = fmt.Sprintf("artifact_%d", time.Now().UnixNano())
	}

	targetDir := filepath.Join(s.baseDir, taskID)
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return "", fmt.Errorf("failed creating task artifact directory: %w", err)
	}

	targetPath := filepath.Join(targetDir, cleanName)
	if err := os.WriteFile(targetPath, data, 0644); err != nil {
		return "", fmt.Errorf("failed saving artifact file: %w", err)
	}

	meta := ArtifactMeta{
		TaskID:    taskID,
		Type:      artType,
		FileName:  cleanName,
		Location:  targetPath,
		SizeBytes: int64(len(data)),
		CreatedAt: time.Now().UTC(),
	}

	s.records[taskID] = append(s.records[taskID], meta)
	return targetPath, nil
}

// GetArtifact reads artifact content from the given path.
func (s *FileSystemStorage) GetArtifact(location string) ([]byte, error) {
	cleanPath := filepath.Clean(location)
	if !strings.HasPrefix(cleanPath, s.baseDir) {
		// Verify containment
		return nil, fmt.Errorf("unauthorized artifact path: %s", location)
	}
	return os.ReadFile(cleanPath)
}

// ListArtifacts retrieves all artifact metadata for a task ID.
func (s *FileSystemStorage) ListArtifacts(taskID string) ([]ArtifactMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	items, exists := s.records[taskID]
	if !exists {
		return []ArtifactMeta{}, nil
	}
	res := make([]ArtifactMeta, len(items))
	copy(res, items)
	return res, nil
}
