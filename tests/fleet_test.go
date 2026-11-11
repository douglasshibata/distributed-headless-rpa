package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"distributed-headless-rpa/internal/controlplane"
	"distributed-headless-rpa/internal/models"
	"distributed-headless-rpa/internal/runner"
	"distributed-headless-rpa/internal/storage"
	"distributed-headless-rpa/internal/vault"
)

func TestJobContractSerialization(t *testing.T) {
	rawJSON := `{
		"task_id": "job_94821a3f",
		"target_domain": "supplier-portal.internal",
		"entrypoint": "https://supplier-portal.internal/invoices/export",
		"auth_ref": "vault://creds/supplier_portal_prod",
		"timeout_seconds": 90,
		"execution_policy": {
			"headless": true,
			"block_media": true,
			"proxy_profile": "datacenter_latam_pool",
			"retries": 2
		},
		"pipeline_steps": [
			{ "action": "navigate", "url": "https://supplier-portal.internal/invoices/export" },
			{ "action": "wait_selector", "selector": "#invoice-table", "timeout_ms": 10000 },
			{ "action": "input", "selector": "#date-range", "value": "LAST_30_DAYS" },
			{ "action": "click", "selector": "#btn-csv-download" },
			{ "action": "intercept_download", "destination_bucket": "s3://rpa-exports/invoices/" }
		],
		"callback_webhook": "https://api.internal/v1/jobs/callback"
	}`

	var job models.JobContract
	if err := json.Unmarshal([]byte(rawJSON), &job); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if job.TaskID != "job_94821a3f" {
		t.Errorf("expected TaskID job_94821a3f, got %s", job.TaskID)
	}
	if len(job.PipelineSteps) != 5 {
		t.Fatalf("expected 5 steps, got %d", len(job.PipelineSteps))
	}
	if job.PipelineSteps[0].Action != models.ActionNavigate {
		t.Errorf("expected step 0 navigate, got %s", job.PipelineSteps[0].Action)
	}
	if job.ExecutionPolicy.BlockMedia != true {
		t.Errorf("expected BlockMedia true")
	}
}

func TestVaultConnectorMemoryWipe(t *testing.T) {
	conn, err := vault.NewMemoryVaultConnector(nil)
	if err != nil {
		t.Fatalf("failed initializing vault: %v", err)
	}

	conn.RegisterSecret("vault://creds/banking", map[string]string{
		"username": "admin_bot",
		"password": "SuperSecretPassword123!",
	})

	sec, err := conn.FetchSecret("vault://creds/banking")
	if err != nil {
		t.Fatalf("failed fetching secret: %v", err)
	}

	pwBuf := sec.Get("password")
	if pwBuf == nil || pwBuf.String() != "SuperSecretPassword123!" {
		t.Fatalf("expected password buffer match, got %v", pwBuf)
	}

	// Wipe secret
	sec.Wipe()
	if pwBuf.String() != "" {
		t.Errorf("expected empty string after wipe, got %s", pwBuf.String())
	}
	if pwBuf.Bytes() != nil {
		t.Errorf("expected nil bytes after wipe")
	}
}

func TestEncryptedSessionCaching(t *testing.T) {
	conn, err := vault.NewMemoryVaultConnector(nil)
	if err != nil {
		t.Fatalf("failed initializing vault: %v", err)
	}

	domain := "bank.internal"
	mockSession := []byte(`{"session_cookie":"abc123token","expires_in":3600}`)

	if err := conn.SaveSession(domain, mockSession); err != nil {
		t.Fatalf("failed saving session: %v", err)
	}

	restored, err := conn.GetCachedSession(domain)
	if err != nil {
		t.Fatalf("failed retrieving session: %v", err)
	}

	if string(restored) != string(mockSession) {
		t.Fatalf("restored session mismatch: got %s, expected %s", string(restored), string(mockSession))
	}
}

func TestStorageArtifactPersist(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "test-rpa-storage")
	defer os.RemoveAll(tmpDir)

	store, err := storage.NewFileSystemStorage(tmpDir)
	if err != nil {
		t.Fatalf("failed creating storage: %v", err)
	}

	taskID := "task-101"
	loc, err := store.SaveArtifact(taskID, storage.ArtifactDownload, "export.csv", []byte("id,value\n1,100\n"))
	if err != nil {
		t.Fatalf("failed saving artifact: %v", err)
	}

	data, err := store.GetArtifact(loc)
	if err != nil {
		t.Fatalf("failed getting artifact: %v", err)
	}
	if !strings.Contains(string(data), "1,100") {
		t.Errorf("unexpected artifact content: %s", string(data))
	}

	list, err := store.ListArtifacts(taskID)
	if err != nil || len(list) != 1 {
		t.Fatalf("expected 1 artifact meta, got %d, err: %v", len(list), err)
	}
}

func TestControlPlaneLeaseAndRateLimit(t *testing.T) {
	rl := controlplane.NewDomainRateLimiter(2)
	domain := "supplier.com"

	if !rl.TryAcquire(domain) {
		t.Errorf("expected slot 1 to acquire")
	}
	if !rl.TryAcquire(domain) {
		t.Errorf("expected slot 2 to acquire")
	}
	if rl.TryAcquire(domain) {
		t.Errorf("expected slot 3 to be rejected due to rate limit")
	}

	rl.Release(domain)
	if !rl.TryAcquire(domain) {
		t.Errorf("expected slot to acquire after release")
	}

	lm := controlplane.NewMemoryLeaseManager()
	ctx := context.Background()

	acquired, err := lm.AcquireLease(ctx, "task-1", "worker-A", time.Minute)
	if err != nil || !acquired {
		t.Errorf("worker-A should acquire lease")
	}

	acquired, err = lm.AcquireLease(ctx, "task-1", "worker-B", time.Minute)
	if err != nil || acquired {
		t.Errorf("worker-B should not acquire locked lease")
	}

	_ = lm.ReleaseLease(ctx, "task-1", "worker-A")
	acquired, err = lm.AcquireLease(ctx, "task-1", "worker-B", time.Minute)
	if err != nil || !acquired {
		t.Errorf("worker-B should acquire after release")
	}
}

func TestRunnerExecutionEndToEnd(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`
			<!DOCTYPE html>
			<html>
			<head><title>Supplier Portal</title></head>
			<body>
				<div id="invoice-table">
					<span class="row">Invoice 101</span>
				</div>
				<input id="date-range" type="text" />
				<button id="btn-csv-download" onclick="window.downloadTriggered = true">Download</button>
			</body>
			</html>
		`))
	}))
	defer server.Close()

	tmpDir := filepath.Join(os.TempDir(), "test-rpa-runner-artifacts")
	defer os.RemoveAll(tmpDir)

	store, err := storage.NewFileSystemStorage(tmpDir)
	if err != nil {
		t.Fatalf("failed creating storage: %v", err)
	}

	vConn, err := vault.NewMemoryVaultConnector(nil)
	if err != nil {
		t.Fatalf("failed creating vault: %v", err)
	}

	r := runner.NewRunner(runner.Config{
		WorkerID:       "test-runner-01",
		WarmPoolSize:   1,
		StealthEnabled: true,
	}, vConn, store)

	job := models.JobContract{
		TaskID:         "test_e2e_job_001",
		TargetDomain:   "127.0.0.1",
		Entrypoint:     server.URL,
		TimeoutSeconds: 30,
		ExecutionPolicy: models.ExecutionPolicy{
			Headless:   true,
			BlockMedia: true,
		},
		PipelineSteps: []models.PipelineStep{
			{Action: models.ActionNavigate, URL: server.URL},
			{Action: models.ActionWaitSelector, Selector: "#invoice-table", FallbackSelectors: []string{".row"}, TimeoutMs: 5000},
			{Action: models.ActionInput, Selector: "#date-range", Value: "LAST_30_DAYS"},
			{Action: models.ActionClick, Selector: "#btn-csv-download"},
			{Action: models.ActionInterceptDownload, DestinationBucket: "s3://exports/", OutputFile: "test_invoice.csv"},
			{Action: models.ActionScreenshot, OutputFile: "portal_screen.png"},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	result := r.ExecuteJob(ctx, job)
	if result.Status != models.StatusCompleted {
		t.Fatalf("expected job COMPLETED, got %s, err: %s", result.Status, result.ErrorMessage)
	}

	if len(result.Steps) != len(job.PipelineSteps) {
		t.Errorf("expected %d steps executed, got %d", len(job.PipelineSteps), len(result.Steps))
	}

	if len(result.Artifacts) < 2 {
		t.Errorf("expected at least 2 artifacts (download + screenshot), got %d", len(result.Artifacts))
	}
}

func TestRunnerFallbackSelectorAndTriage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`
			<!DOCTYPE html>
			<html>
			<body>
				<div class="fallback-invoice-grid">Found via fallback!</div>
			</body>
			</html>
		`))
	}))
	defer server.Close()

	tmpDir := filepath.Join(os.TempDir(), "test-rpa-fallback-artifacts")
	defer os.RemoveAll(tmpDir)

	store, _ := storage.NewFileSystemStorage(tmpDir)
	vConn, _ := vault.NewMemoryVaultConnector(nil)

	r := runner.NewRunner(runner.Config{
		WorkerID:       "fallback-runner",
		StealthEnabled: true,
	}, vConn, store)

	// Test 1: Fallback selector succeeds
	jobSuccess := models.JobContract{
		TaskID:         "test_fallback_success",
		Entrypoint:     server.URL,
		TimeoutSeconds: 15,
		ExecutionPolicy: models.ExecutionPolicy{
			Headless: true,
		},
		PipelineSteps: []models.PipelineStep{
			{Action: models.ActionNavigate, URL: server.URL},
			{
				Action:            models.ActionWaitSelector,
				Selector:          "#primary-selector-missing",
				FallbackSelectors: []string{".fallback-invoice-grid"},
				TimeoutMs:         3000,
			},
		},
	}

	res := r.ExecuteJob(context.Background(), jobSuccess)
	if res.Status != models.StatusCompleted {
		t.Errorf("expected fallback selector to succeed, got status: %s (err: %s)", res.Status, res.ErrorMessage)
	}

	// Test 2: Complete failure triggers triage screenshot and DOM dump upload
	jobFail := models.JobContract{
		TaskID:         "test_triage_capture",
		Entrypoint:     server.URL,
		TimeoutSeconds: 15,
		ExecutionPolicy: models.ExecutionPolicy{
			Headless: true,
		},
		PipelineSteps: []models.PipelineStep{
			{Action: models.ActionNavigate, URL: server.URL},
			{
				Action:    models.ActionWaitSelector,
				Selector:  "#completely-nonexistent-element",
				TimeoutMs: 500,
			},
		},
	}

	resFail := r.ExecuteJob(context.Background(), jobFail)
	if resFail.Status != models.StatusFailed {
		t.Errorf("expected job failure, got: %s", resFail.Status)
	}

	// Triage artifacts should be uploaded: screenshot + DOM dump
	if len(resFail.Artifacts) == 0 {
		t.Errorf("expected triage artifacts to be recorded on failure")
	}
}
