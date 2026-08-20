package metrics

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SpiritTechDevelopment/NodeAgent/internal/service"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/state"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/usage"
)

func TestRegistryExportsRequiredMetrics(t *testing.T) {
	lastCollection := time.Date(2026, time.August, 14, 12, 0, 1, 500_000_000, time.UTC)
	lastPoll := time.Date(2026, time.August, 14, 12, 0, 2, 250_000_000, time.UTC)
	registry, err := New(
		fakeStatusSource{status: service.Status{
			NeedsBootstrap: true,
			Xray:           service.XrayStatus{Reachable: true},
		}},
		fakeOutboxSource{stats: state.UsageOutboxStats{Batches: 7, PayloadBytes: 4096}},
		fakeUsageSource{status: usage.CollectionStatus{LastSuccessAt: lastCollection}},
		fakeObservationSource{
			lastPoll: lastPoll, reconcileErrors: 3,
			reconciliation: service.ReconciliationStatus{
				DesiredUsers: 4, AppliedUsers: 3, AppliedRules: 2, Drift: 3,
				LastSuccessAt: lastPoll,
			},
		},
	)
	if err != nil {
		t.Fatalf("New() вернул ошибку: %v", err)
	}
	if err := registry.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh() вернул ошибку: %v", err)
	}

	request := httptest.NewRequest("GET", "/metrics", nil)
	response := httptest.NewRecorder()
	registry.Handler().ServeHTTP(response, request)
	if response.Code != 200 {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	for _, line := range []string{
		"spirit_agent_up 1",
		"spirit_agent_xray_up 1",
		"spirit_agent_usage_last_collection_timestamp_seconds 1.7867088015e+09",
		"spirit_agent_usage_outbox_batches 7",
		"spirit_agent_usage_outbox_bytes 4096",
		"spirit_agent_last_backend_poll_timestamp_seconds 1.78670880225e+09",
		"spirit_agent_local_reconcile_errors_total 3",
		"spirit_agent_desired_users 4",
		"spirit_agent_desired_routing_rules 4",
		"spirit_agent_applied_users 3",
		"spirit_agent_applied_routing_rules 2",
		"spirit_agent_state_drift 3",
		"spirit_agent_last_successful_reconcile_timestamp_seconds 1.78670880225e+09",
		"spirit_agent_needs_bootstrap 1",
	} {
		if !strings.Contains(response.Body.String(), line+"\n") {
			t.Errorf("метрики не содержат %q:\n%s", line, response.Body.String())
		}
	}
}

func TestNewRejectsMissingDependencies(t *testing.T) {
	if _, err := New(nil, fakeOutboxSource{}, fakeUsageSource{}, fakeObservationSource{}); err == nil {
		t.Fatal("New() принял nil status provider")
	}
}

func TestRegistryKeepsServingWhenRefreshFails(t *testing.T) {
	registry, err := New(
		fakeStatusSource{err: errors.New("status failed")},
		fakeOutboxSource{err: errors.New("outbox failed")},
		fakeUsageSource{},
		fakeObservationSource{},
	)
	if err != nil {
		t.Fatalf("New() вернул ошибку: %v", err)
	}
	if err := registry.Refresh(context.Background()); err == nil {
		t.Fatal("Refresh() не вернул совокупную ошибку")
	}
	response := httptest.NewRecorder()
	registry.Handler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
	if response.Code != 200 || !strings.Contains(response.Body.String(), "spirit_agent_up 1\n") {
		t.Fatalf("метрики недоступны после ошибки: status=%d body=%s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "spirit_agent_xray_up 0\n") {
		t.Fatalf("Xray не помечен недоступным:\n%s", response.Body.String())
	}
}

func TestRegistryRunRejectsDuplicateAndStops(t *testing.T) {
	registry, err := New(
		fakeStatusSource{},
		fakeOutboxSource{},
		fakeUsageSource{},
		fakeObservationSource{},
	)
	if err != nil {
		t.Fatalf("New() вернул ошибку: %v", err)
	}
	registry.refreshInterval = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- registry.Run(ctx, nil) }()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for !registry.running.Load() {
		select {
		case <-deadline.C:
			t.Fatal("Run() не перешёл в состояние running")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err := registry.Run(ctx, nil); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("повторный Run() error = %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() после отмены вернул ошибку: %v", err)
	}
}

func TestRegistryRunReportsRefreshFailure(t *testing.T) {
	registry, err := New(
		fakeStatusSource{err: errors.New("status failed")},
		fakeOutboxSource{},
		fakeUsageSource{},
		fakeObservationSource{},
	)
	if err != nil {
		t.Fatalf("New() вернул ошибку: %v", err)
	}
	registry.refreshInterval = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	reported := make(chan error, 1)
	done := make(chan error, 1)
	go func() {
		done <- registry.Run(ctx, func(err error) {
			reported <- err
			cancel()
		})
	}()
	if err := <-reported; err == nil {
		t.Fatal("Run() сообщил nil вместо ошибки refresh")
	}
	if err := <-done; err != nil {
		t.Fatalf("Run() после отмены вернул ошибку: %v", err)
	}
}

type fakeStatusSource struct {
	status service.Status
	err    error
}

func (source fakeStatusSource) Status(context.Context) (service.Status, error) {
	return source.status, source.err
}

type fakeOutboxSource struct {
	stats state.UsageOutboxStats
	err   error
}

func (source fakeOutboxSource) UsageOutboxStats(context.Context) (state.UsageOutboxStats, error) {
	return source.stats, source.err
}

type fakeUsageSource struct {
	status usage.CollectionStatus
}

func (source fakeUsageSource) Status() usage.CollectionStatus {
	return source.status
}

type fakeObservationSource struct {
	lastPoll        time.Time
	reconcileErrors uint64
	reconciliation  service.ReconciliationStatus
}

func (source fakeObservationSource) LastBackendPollAt() time.Time {
	return source.lastPoll
}

func (source fakeObservationSource) LocalReconcileErrors() uint64 {
	return source.reconcileErrors
}

func (source fakeObservationSource) Reconciliation() service.ReconciliationStatus {
	return source.reconciliation
}
