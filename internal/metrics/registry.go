// Package metrics экспортирует локальные метрики агента в формате Prometheus.
package metrics

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/SpiritTechDevelopment/NodeAgent/internal/service"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/state"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/usage"
)

const defaultRefreshInterval = 5 * time.Second

var (
	// ErrAlreadyRunning означает, что цикл обновления метрик уже запущен.
	ErrAlreadyRunning = errors.New("metrics refresh loop is already running")
)

type outboxSource interface {
	UsageOutboxStats(context.Context) (state.UsageOutboxStats, error)
}

type usageSource interface {
	Status() usage.CollectionStatus
}

type observationSource interface {
	LastBackendPollAt() time.Time
	LocalReconcileErrors() uint64
	Reconciliation() service.ReconciliationStatus
}

// Registry хранит Prometheus-метрики и обновляет снимки локальных зависимостей.
type Registry struct {
	status       service.StatusProvider
	outbox       outboxSource
	usage        usageSource
	observations observationSource

	registry           *prometheus.Registry
	agentUp            prometheus.Gauge
	xrayUp             prometheus.Gauge
	usageOutboxBatches prometheus.Gauge
	usageOutboxBytes   prometheus.Gauge
	needsBootstrap     prometheus.Gauge
	refreshInterval    time.Duration
	running            atomic.Bool
}

// New создаёт изолированный реестр обязательных метрик агента.
func New(
	status service.StatusProvider,
	outbox outboxSource,
	usageStatus usageSource,
	observations observationSource,
) (*Registry, error) {
	if status == nil || outbox == nil || usageStatus == nil || observations == nil {
		return nil, errors.New("metrics dependencies are required")
	}

	result := &Registry{
		status:       status,
		outbox:       outbox,
		usage:        usageStatus,
		observations: observations,
		registry:     prometheus.NewRegistry(),
		agentUp:      newGauge("up", "Единица означает, что процесс агента запущен."),
		xrayUp:       newGauge("xray_up", "Единица означает, что локальный API Xray доступен."),
		usageOutboxBatches: newGauge(
			"usage_outbox_batches",
			"Число неподтверждённых пачек в usage outbox.",
		),
		usageOutboxBytes: newGauge(
			"usage_outbox_bytes",
			"Суммарный размер payload неподтверждённых пачек usage.",
		),
		needsBootstrap: newGauge(
			"needs_bootstrap",
			"Единица означает, что агенту требуется полная авторитетная сверка.",
		),
		refreshInterval: defaultRefreshInterval,
	}
	result.agentUp.Set(1)

	lastCollection := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "spirit",
		Subsystem: "agent",
		Name:      "usage_last_collection_timestamp_seconds",
		Help:      "Unix-время последнего успешного сбора usage.",
	}, func() float64 {
		return timestampSeconds(result.usage.Status().LastSuccessAt)
	})
	lastBackendPoll := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "spirit",
		Subsystem: "agent",
		Name:      "last_backend_poll_timestamp_seconds",
		Help:      "Unix-время последнего вызова GetNodeState со стороны backend.",
	}, func() float64 {
		return timestampSeconds(result.observations.LastBackendPollAt())
	})
	localReconcileErrors := prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: "spirit",
		Subsystem: "agent",
		Name:      "local_reconcile_errors_total",
		Help:      "Общее число ошибок локального self-heal.",
	}, func() float64 {
		return float64(result.observations.LocalReconcileErrors())
	})
	desiredUsers := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "spirit", Subsystem: "agent", Name: "desired_users",
		Help: "Expected number of backend-owned users in Xray.",
	}, func() float64 { return float64(result.observations.Reconciliation().DesiredUsers) })
	desiredRules := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "spirit", Subsystem: "agent", Name: "desired_routing_rules",
		Help: "Expected number of per-user routing rules in Xray.",
	}, func() float64 { return float64(result.observations.Reconciliation().DesiredUsers) })
	appliedUsers := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "spirit", Subsystem: "agent", Name: "applied_users",
		Help: "Number of desired users matching the observed Xray runtime.",
	}, func() float64 { return float64(result.observations.Reconciliation().AppliedUsers) })
	appliedRules := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "spirit", Subsystem: "agent", Name: "applied_routing_rules",
		Help: "Number of desired per-user routes selected by Xray.",
	}, func() float64 { return float64(result.observations.Reconciliation().AppliedRules) })
	stateDrift := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "spirit", Subsystem: "agent", Name: "state_drift",
		Help: "Number of missing or mismatched desired users and routing rules.",
	}, func() float64 { return float64(result.observations.Reconciliation().Drift) })
	lastSuccessfulReconcile := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "spirit", Subsystem: "agent", Name: "last_successful_reconcile_timestamp_seconds",
		Help: "Unix time of the last successful desired/runtime reconciliation.",
	}, func() float64 {
		return timestampSeconds(result.observations.Reconciliation().LastSuccessAt)
	})
	result.registry.MustRegister(
		result.agentUp,
		result.xrayUp,
		lastCollection,
		result.usageOutboxBatches,
		result.usageOutboxBytes,
		lastBackendPoll,
		localReconcileErrors,
		desiredUsers,
		desiredRules,
		appliedUsers,
		appliedRules,
		stateDrift,
		lastSuccessfulReconcile,
		result.needsBootstrap,
	)
	return result, nil
}

// Handler возвращает обработчик Prometheus, не обращающийся к SQLite или Xray.
func (registry *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(registry.registry, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
	})
}

// Refresh обновляет метрики, требующие обращения к локальным зависимостям.
func (registry *Registry) Refresh(ctx context.Context) error {
	if ctx == nil {
		return errors.New("metrics refresh context is required")
	}
	var errs []error
	status, err := registry.status.Status(ctx)
	if err != nil {
		registry.xrayUp.Set(0)
		errs = append(errs, errors.New("refresh agent status metrics"))
	} else {
		registry.xrayUp.Set(boolFloat(status.Xray.Reachable))
		registry.needsBootstrap.Set(boolFloat(status.NeedsBootstrap))
	}
	outbox, err := registry.outbox.UsageOutboxStats(ctx)
	if err != nil {
		errs = append(errs, errors.New("refresh usage outbox metrics"))
	} else {
		registry.usageOutboxBatches.Set(float64(outbox.Batches))
		registry.usageOutboxBytes.Set(float64(outbox.PayloadBytes))
	}
	return errors.Join(errs...)
}

// Run периодически обновляет снимки метрик до отмены ctx.
func (registry *Registry) Run(ctx context.Context, report func(error)) error {
	if ctx == nil {
		return errors.New("metrics run context is required")
	}
	if !registry.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	defer registry.running.Store(false)
	if registry.refreshInterval <= 0 {
		return errors.New("metrics refresh interval must be positive")
	}

	refresh := func() {
		if err := registry.Refresh(ctx); err != nil && ctx.Err() == nil && report != nil {
			report(err)
		}
	}
	refresh()
	ticker := time.NewTicker(registry.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			refresh()
		}
	}
}

func newGauge(name, help string) prometheus.Gauge {
	return prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "spirit",
		Subsystem: "agent",
		Name:      name,
		Help:      help,
	})
}

func timestampSeconds(value time.Time) float64 {
	if value.IsZero() {
		return 0
	}
	return float64(value.Unix()) + float64(value.Nanosecond())/float64(time.Second)
}

func boolFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
