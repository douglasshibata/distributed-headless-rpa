package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-headless-rpa/internal/models"
	"distributed-headless-rpa/internal/runner"
	"distributed-headless-rpa/internal/storage"
	"distributed-headless-rpa/internal/vault"
)

func main() {
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = fmt.Sprintf("fleet-worker-%d", os.Getpid())
	}

	storageDir := os.Getenv("STORAGE_DIR")
	store, err := storage.NewFileSystemStorage(storageDir)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}

	vConn, err := vault.NewMemoryVaultConnector(nil)
	if err != nil {
		log.Fatalf("Failed to initialize vault: %v", err)
	}

	r := runner.NewRunner(runner.Config{
		WorkerID:       workerID,
		WarmPoolSize:   2,
		StealthEnabled: true,
	}, vConn, store)

	log.Printf("[WorkerDaemon] %s ready. Listening for dispatch tasks over broker...", workerID)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	demoJob := models.JobContract{
		TaskID:         "startup_self_check",
		TargetDomain:   "internal.health",
		Entrypoint:     "about:blank",
		TimeoutSeconds: 10,
		ExecutionPolicy: models.ExecutionPolicy{
			Headless:   true,
			BlockMedia: true,
		},
		PipelineSteps: []models.PipelineStep{
			{Action: models.ActionNavigate, URL: "about:blank"},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	res := r.ExecuteJob(ctx, demoJob)
	cancel()
	log.Printf("[WorkerDaemon] Self-check completed with status: %s (duration %dms)", res.Status, res.DurationMs)

	select {
	case <-stop:
		log.Printf("[WorkerDaemon] Shutting down %s...", workerID)
	}
}
