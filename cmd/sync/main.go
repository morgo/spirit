package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/alecthomas/kong"
	"github.com/block/spirit/pkg/sync"
)

type CLI struct {
	ConfigFile string `arg:"" name:"config" help:"Path to YAML configuration file" type:"existingfile"`
}

func (c *CLI) Run() error {
	// Set up logging
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Load configuration
	logger.Info("loading configuration", "file", c.ConfigFile)
	config, err := sync.LoadConfig(c.ConfigFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Validate configuration
	if err := config.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	logger.Info("starting sync", "name", config.Sync.Name)

	// Create sync runner
	runner, err := sync.NewRunner(config)
	if err != nil {
		return fmt.Errorf("failed to create runner: %w", err)
	}
	defer runner.Close()

	// Set logger on runner
	runner.SetLogger(logger)

	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Run in goroutine so we can handle signals
	errChan := make(chan error, 1)
	go func() {
		errChan <- runner.Run(ctx)
	}()

	// Wait for either completion or signal
	select {
	case err := <-errChan:
		if err != nil {
			logger.Error("sync failed", "error", err)
			return err
		}
		logger.Info("sync completed successfully")
		return nil
	case sig := <-sigChan:
		logger.Info("received signal, shutting down gracefully", "signal", sig)
		cancel()

		// Wait for runner to finish shutdown
		if err := <-errChan; err != nil && err != context.Canceled {
			logger.Error("error during shutdown", "error", err)
			return err
		}

		logger.Info("shutdown complete")
		return nil
	}
}

func main() {
	var cli CLI
	ctx := kong.Parse(&cli,
		kong.Name("spirit-sync"),
		kong.Description("Spirit Continual Sync - MySQL to PostgreSQL/Iceberg replication"),
		kong.UsageOnError(),
	)

	err := ctx.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
