package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"distributed-headless-rpa/internal/models"
	"distributed-headless-rpa/internal/runner"
	"distributed-headless-rpa/internal/storage"
	"distributed-headless-rpa/internal/vault"
)

// LeaseManager coordinates distributed locks so tasks are not picked up by multiple nodes.
type LeaseManager interface {
	AcquireLease(ctx context.Context, taskID, workerID string, ttl time.Duration) (bool, error)
	ReleaseLease(ctx context.Context, taskID, workerID string) error
	RenewLease(ctx context.Context, taskID, workerID string, ttl time.Duration) error
}

// MemoryLeaseManager is an in-memory lock manager simulating Redis or NATS KV leases.
type MemoryLeaseManager struct {
	mu     sync.Mutex
	leases map[string]leaseEntry
}

type leaseEntry struct {
	workerID  string
	expiresAt time.Time
}

// NewMemoryLeaseManager creates an in-memory lease manager.
func NewMemoryLeaseManager() *MemoryLeaseManager {
	return &MemoryLeaseManager{
		leases: make(map[string]leaseEntry),
	}
}

func (m *MemoryLeaseManager) AcquireLease(ctx context.Context, taskID, workerID string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	entry, exists := m.leases[taskID]
	if exists && entry.expiresAt.After(now) && entry.workerID != workerID {
		return false, nil // Already locked by another worker
	}

	m.leases[taskID] = leaseEntry{
		workerID:  workerID,
		expiresAt: now.Add(ttl),
	}
	return true, nil
}

func (m *MemoryLeaseManager) ReleaseLease(ctx context.Context, taskID, workerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, exists := m.leases[taskID]
	if exists && entry.workerID == workerID {
		delete(m.leases, taskID)
	}
	return nil
}

func (m *MemoryLeaseManager) RenewLease(ctx context.Context, taskID, workerID string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, exists := m.leases[taskID]
	if !exists || entry.workerID != workerID {
		return errors.New("lease expired or not owned")
	}
	entry.expiresAt = time.Now().Add(ttl)
	m.leases[taskID] = entry
	return nil
}

// DomainRateLimiter tracks and enforces concurrent request limits per target domain.
type DomainRateLimiter struct {
	mu          sync.Mutex
	maxPerHost  int
	activeHosts map[string]int
}

// NewDomainRateLimiter creates a domain rate-limiter.
func NewDomainRateLimiter(maxPerHost int) *DomainRateLimiter {
	if maxPerHost <= 0 {
		maxPerHost = 5 // Default max 5 concurrent executions to the same target domain
	}
	return &DomainRateLimiter{
		maxPerHost:  maxPerHost,
		activeHosts: make(map[string]int),
	}
}

// TryAcquire attempts to increment concurrent slot for target domain.
func (rl *DomainRateLimiter) TryAcquire(targetDomain string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	domain := strings.ToLower(strings.TrimSpace(targetDomain))
	count := rl.activeHosts[domain]
	if count >= rl.maxPerHost {
		return false
	}
	rl.activeHosts[domain] = count + 1
	return true
}

// Release releases a concurrent slot for target domain.
func (rl *DomainRateLimiter) Release(targetDomain string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	domain := strings.ToLower(strings.TrimSpace(targetDomain))
	if count, ok := rl.activeHosts[domain]; ok && count > 0 {
		rl.activeHosts[domain] = count - 1
		if rl.activeHosts[domain] == 0 {
			delete(rl.activeHosts, domain)
		}
	}
}

// ControlPlaneServer orchestrates task ingestion, queueing, rate limiting, and status queries.
type ControlPlaneServer struct {
	mu           sync.RWMutex
	jobs         map[string]*models.JobContract
	results      map[string]*models.JobResult
	leaseManager LeaseManager
	rateLimiter  *DomainRateLimiter
	vaultConn    vault.Connector
	storage      storage.Storage
	runner       *runner.Runner
	jobQueue     chan *models.JobContract
	httpClient   *http.Client
}

// Config provides setup parameters for the control plane.
type Config struct {
	MaxConcurrentPerHost int
	QueueCapacity        int
	WorkerID             string
}

// NewControlPlaneServer builds and starts the Control Plane.
func NewControlPlaneServer(cfg Config, vConn vault.Connector, store storage.Storage, r *runner.Runner) *ControlPlaneServer {
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 1000
	}
	srv := &ControlPlaneServer{
		jobs:         make(map[string]*models.JobContract),
		results:      make(map[string]*models.JobResult),
		leaseManager: NewMemoryLeaseManager(),
		rateLimiter:  NewDomainRateLimiter(cfg.MaxConcurrentPerHost),
		vaultConn:    vConn,
		storage:      store,
		runner:       r,
		jobQueue:     make(chan *models.JobContract, cfg.QueueCapacity),
		httpClient:   &http.Client{Timeout: 10 * time.Second},
	}

	// Start background queue worker
	go srv.queueWorkerLoop()

	return srv
}

// SubmitJob ingests a new task into the control plane.
func (s *ControlPlaneServer) SubmitJob(job *models.JobContract) (*models.JobResult, error) {
	if job == nil || job.TaskID == "" {
		return nil, errors.New("invalid job contract: missing task_id")
	}

	// Auto-extract domain from entrypoint if target_domain is omitted
	if job.TargetDomain == "" && job.Entrypoint != "" {
		if parsed, err := url.Parse(job.Entrypoint); err == nil {
			job.TargetDomain = parsed.Hostname()
		}
	}

	s.mu.Lock()
	s.jobs[job.TaskID] = job
	initialRes := &models.JobResult{
		TaskID:    job.TaskID,
		Status:    models.StatusPending,
		StartTime: time.Now().UTC(),
	}
	s.results[job.TaskID] = initialRes
	s.mu.Unlock()

	select {
	case s.jobQueue <- job:
		return initialRes, nil
	default:
		return nil, errors.New("fleet ingestion queue is full")
	}
}

// GetJobResult retrieves the result or status of a job by task ID.
func (s *ControlPlaneServer) GetJobResult(taskID string) (*models.JobResult, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res, exists := s.results[taskID]
	return res, exists
}

// queueWorkerLoop processes queued jobs adhering to domain rate-limits and leases.
func (s *ControlPlaneServer) queueWorkerLoop() {
	workerID := "fleet-worker-daemon-1"

	for job := range s.jobQueue {
		// Acquire distributed lease
		acquired, err := s.leaseManager.AcquireLease(context.Background(), job.TaskID, workerID, 5*time.Minute)
		if err != nil || !acquired {
			log.Printf("[ControlPlane] Failed acquiring lease for task %s, re-queuing", job.TaskID)
			time.Sleep(200 * time.Millisecond)
			s.jobQueue <- job
			continue
		}

		// Enforce domain rate limit
		domain := job.TargetDomain
		for !s.rateLimiter.TryAcquire(domain) {
			// Rate limit saturated for this host; backoff briefly
			time.Sleep(100 * time.Millisecond)
		}

		// Execute job via runner
		go func(j *models.JobContract) {
			defer s.rateLimiter.Release(domain)
			defer func() {
				_ = s.leaseManager.ReleaseLease(context.Background(), j.TaskID, workerID)
			}()

			res := s.runner.ExecuteJob(context.Background(), *j)

			s.mu.Lock()
			s.results[j.TaskID] = res
			s.mu.Unlock()

			// Dispatch webhook callback if configured
			if j.CallbackWebhook != "" {
				s.dispatchCallback(j.CallbackWebhook, res)
			}
		}(job)
	}
}

// dispatchCallback notifies the caller via webhook payload.
func (s *ControlPlaneServer) dispatchCallback(webhookURL string, res *models.JobResult) {
	payload := models.CallbackPayload{
		TaskID:       res.TaskID,
		Status:       res.Status,
		DurationMs:   res.DurationMs,
		Artifacts:    res.Artifacts,
		ErrorMessage: res.ErrorMessage,
		StepsSummary: res.Steps,
		CompletedAt:  res.EndTime,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[ControlPlane] Failed marshaling callback payload: %v", err)
		return
	}

	req, err := http.NewRequest(http.MethodPost, webhookURL, bytes.NewReader(body))
	if err != nil {
		log.Printf("[ControlPlane] Failed creating webhook request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Distributed-RPA-Fleet-Dispatcher/1.0")

	// Retry once on failure
	for attempt := 1; attempt <= 2; attempt++ {
		resp, err := s.httpClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// SetupHTTPRoutes registers standard REST endpoints.
func (s *ControlPlaneServer) SetupHTTPRoutes() *http.ServeMux {
	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":    "healthy",
			"timestamp": time.Now().UTC(),
		})
	})

	// Job ingestion: POST /api/v1/jobs
	// Job query: GET /api/v1/jobs/{id}
	mux.HandleFunc("/api/v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			var job models.JobContract
			if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
				http.Error(w, fmt.Sprintf("invalid request payload: %v", err), http.StatusBadRequest)
				return
			}
			res, err := s.SubmitJob(&job)
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(res)
			return
		}
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	})

	mux.HandleFunc("/api/v1/jobs/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			taskID := strings.TrimPrefix(r.URL.Path, "/api/v1/jobs/")
			if taskID == "" {
				http.Error(w, "missing task_id in path", http.StatusBadRequest)
				return
			}
			res, exists := s.GetJobResult(taskID)
			if !exists {
				http.Error(w, "task not found", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(res)
			return
		}
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	})

	return mux
}
