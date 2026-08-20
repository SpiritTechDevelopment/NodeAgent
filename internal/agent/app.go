package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SpiritTechDevelopment/NodeAgent/internal/grpcserver"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/httpserver"
	agentmetrics "github.com/SpiritTechDevelopment/NodeAgent/internal/metrics"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/service"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/state"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/usage"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/xray"
)

var (
	// ErrAlreadyRunning означает, что жизненный цикл приложения уже запущен.
	ErrAlreadyRunning = errors.New("node agent application is already running")
)

type managedServer interface {
	Serve() error
	Shutdown(context.Context) error
}

type server struct {
	name    string
	managed managedServer
}

type worker struct {
	name string
	run  func(context.Context, func(error)) error
}

type processResult struct {
	name string
	err  error
}

// App владеет серверами, фоновыми циклами и локальными ресурсами процесса.
type App struct {
	servers         []server
	workers         []worker
	closers         []io.Closer
	shutdownTimeout time.Duration
	logger          *slog.Logger
	started         atomic.Bool
	stopOnce        sync.Once
	stopErr         error
	closeOnce       sync.Once
	closeErr        error
}

// New собирает production-зависимости и открывает локальные ресурсы агента.
func New(ctx context.Context, config Config, logger *slog.Logger) (*App, error) {
	if ctx == nil {
		return nil, errors.New("application context is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if config.ShutdownTimeout <= 0 {
		return nil, errors.New("shutdown timeout must be positive")
	}

	store, err := state.Open(ctx, config.StateDatabasePath)
	if err != nil {
		return nil, fmt.Errorf("open local state: %w", err)
	}
	closeStore := true
	defer func() {
		if closeStore {
			_ = store.Close()
		}
	}()

	xrayClient, err := xray.New(config.Xray)
	if err != nil {
		return nil, fmt.Errorf("create Xray client: %w", err)
	}
	xrayConfig, err := xray.NewConfigFile(
		config.XrayConfigPath,
		config.Xray.InboundTag,
		config.FallbackOutboundTag,
	)
	if err != nil {
		return nil, fmt.Errorf("open durable Xray config: %w", err)
	}
	closeXray := true
	defer func() {
		if closeXray {
			_ = xrayClient.Close()
		}
	}()

	collector, err := usage.New(store, xrayClient)
	if err != nil {
		return nil, fmt.Errorf("create usage collector: %w", err)
	}
	status, err := newStatusProvider(store, xrayClient, collector)
	if err != nil {
		return nil, fmt.Errorf("create status provider: %w", err)
	}
	applicationService, err := service.New(service.Config{
		NodeID:              config.NodeID,
		AgentVersion:        config.AgentVersion,
		LocalOutboundTag:    config.LocalOutboundTag,
		FallbackOutboundTag: config.FallbackOutboundTag,
		MaxInventoryUsers:   config.MaximumInventoryUsers,
	}, service.Dependencies{
		Status:     status,
		State:      store,
		Xray:       xrayClient,
		XrayConfig: xrayConfig,
		Usage:      collector,
	})
	if err != nil {
		return nil, fmt.Errorf("create application service: %w", err)
	}
	metricsRegistry, err := agentmetrics.New(status, store, collector, applicationService)
	if err != nil {
		return nil, fmt.Errorf("create metrics registry: %w", err)
	}

	grpcListener, err := new(net.ListenConfig).Listen(ctx, "tcp", config.ListenAddress)
	if err != nil {
		return nil, fmt.Errorf("listen for management gRPC: %w", err)
	}
	grpcServer, err := grpcserver.New(grpcListener, config.GRPC, applicationService)
	if err != nil {
		_ = grpcListener.Close()
		return nil, fmt.Errorf("create management gRPC server: %w", err)
	}
	httpListener, err := new(net.ListenConfig).Listen(ctx, "tcp", config.HTTPListenAddress)
	if err != nil {
		_ = grpcListener.Close()
		return nil, fmt.Errorf("listen for service HTTP: %w", err)
	}
	httpServer, err := httpserver.New(httpListener, status, metricsRegistry.Handler())
	if err != nil {
		_ = httpListener.Close()
		_ = grpcListener.Close()
		return nil, fmt.Errorf("create service HTTP server: %w", err)
	}

	closeStore = false
	closeXray = false
	return &App{
		servers: []server{
			{name: "gRPC server", managed: grpcServer},
			{name: "HTTP server", managed: httpServer},
		},
		workers: []worker{
			{name: "usage collector", run: collector.Run},
			{name: "local self-heal", run: applicationService.RunSelfHeal},
			{name: "metrics collector", run: metricsRegistry.Run},
		},
		closers:         []io.Closer{xrayClient, store},
		shutdownTimeout: config.ShutdownTimeout,
		logger:          logger,
	}, nil
}

// Run обслуживает серверы и фоновые циклы до отмены ctx или отказа компонента.
func (app *App) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("run context is required")
	}
	if !app.started.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	componentCount := len(app.workers) + len(app.servers)
	results := make(chan processResult, componentCount)
	for _, item := range app.servers {
		item := item
		go func() {
			results <- processResult{name: item.name, err: item.managed.Serve()}
		}()
	}
	for _, item := range app.workers {
		item := item
		go func() {
			report := func(err error) {
				app.logger.Warn("фоновая операция завершилась с ошибкой",
					slog.String("component", item.name),
					slog.String("error", err.Error()),
				)
			}
			results <- processResult{name: item.name, err: item.run(runCtx, report)}
		}()
	}

	var (
		errs     []error
		received int
	)
	select {
	case <-ctx.Done():
	case result := <-results:
		received++
		if result.err == nil {
			errs = append(errs, fmt.Errorf("%s stopped unexpectedly", result.name))
		} else if !errors.Is(result.err, context.Canceled) {
			errs = append(errs, fmt.Errorf("%s: %w", result.name, result.err))
		}
	}
	cancel()
	if err := app.shutdown(); err != nil {
		errs = append(errs, err)
	}
	for received < componentCount {
		result := <-results
		received++
		if result.err != nil && !errors.Is(result.err, context.Canceled) {
			errs = append(errs, fmt.Errorf("%s: %w", result.name, result.err))
		}
	}
	return errors.Join(errs...)
}

// Close освобождает Xray-соединение и SQLite после завершения Run.
func (app *App) Close() error {
	app.closeOnce.Do(func() {
		var errs []error
		if err := app.shutdown(); err != nil {
			errs = append(errs, err)
		}
		for _, closer := range app.closers {
			if err := closer.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		app.closeErr = errors.Join(errs...)
	})
	return app.closeErr
}

func (app *App) shutdown() error {
	app.stopOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), app.shutdownTimeout)
		defer cancel()
		results := make(chan processResult, len(app.servers))
		for _, item := range app.servers {
			item := item
			go func() {
				results <- processResult{name: item.name, err: item.managed.Shutdown(ctx)}
			}()
		}
		var errs []error
		for range app.servers {
			result := <-results
			if result.err != nil {
				errs = append(errs, fmt.Errorf("shutdown %s: %w", result.name, result.err))
			}
		}
		app.stopErr = errors.Join(errs...)
	})
	return app.stopErr
}
