package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ingestor/internal/config"
	"github.com/ingestor/internal/router"
	"github.com/sirupsen/logrus"
)

func main() {
	// Setup logger
	log := logrus.New()
	log.SetFormatter(&logrus.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(logrus.InfoLevel)

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Setup router and server
	r := router.SetupRouter(cfg, log)
	srv := router.SetupServer(r, cfg)

	// Graceful shutdown setup
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Start server in goroutine
	go func() {
		log.Infof("Starting server on %s", cfg.ServerAddr)
		if err := srv.ListenAndServe(); err != nil {
			if err.Error() != "http: Server closed" {
				log.Errorf("Server error: %v", err)
			}
		}
	}()

	// Wait for interrupt signal
	<-quit
	log.Info("Shutting down server...")

	// Create shutdown context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Shutdown gracefully
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Info("Server exited")
}