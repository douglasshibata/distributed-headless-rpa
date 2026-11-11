package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-headless-rpa/internal/controlplane"
	"distributed-headless-rpa/internal/runner"
	"distributed-headless-rpa/internal/storage"
	"distributed-headless-rpa/internal/vault"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
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
		WorkerID:       "controlplane-inline-runner",
		WarmPoolSize:   1,
		StealthEnabled: true,
	}, vConn, store)

	cp := controlplane.NewControlPlaneServer(controlplane.Config{
		MaxConcurrentPerHost: 5,
		QueueCapacity:        1000,
	}, vConn, store, r)

	mux := cp.SetupHTTPRoutes()
	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		log.Printf("Starting Distributed RPA Fleet Control Plane on :%s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server listen failed: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("Shutting down Control Plane gracefully...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	fmt.Println("Control Plane stopped.")
}
