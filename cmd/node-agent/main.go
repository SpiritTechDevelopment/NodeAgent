package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/SpiritTechDevelopment/NodeAgent/internal/agent"
)

var version = "dev"

func main() {
	os.Exit(run())
}

func run() int {
	config, err := agent.LoadConfig(os.Getenv, version)
	if err != nil {
		slog.Error("не удалось загрузить конфигурацию", slog.String("error", err.Error()))
		return 1
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: config.LogLevel}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	application, err := agent.New(ctx, config, logger)
	if err != nil {
		logger.Error("не удалось собрать node-agent", slog.String("error", err.Error()))
		return 1
	}
	defer func() {
		if err := application.Close(); err != nil {
			logger.Error("не удалось закрыть ресурсы node-agent", slog.String("error", err.Error()))
		}
	}()

	logger.Info("node-agent запущен",
		slog.String("node_id", config.NodeID),
		slog.String("version", config.AgentVersion),
		slog.String("listen", config.ListenAddress),
	)
	if err := application.Run(ctx); err != nil {
		logger.Error("node-agent завершился с ошибкой", slog.String("error", err.Error()))
		return 1
	}
	logger.Info("node-agent остановлен")
	return 0
}
