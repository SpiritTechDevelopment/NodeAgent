package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/SpiritTechDevelopment/NodeAgent/internal/state"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/usage"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/xray"
)

func TestStatusProviderMapsLocalDependencies(t *testing.T) {
	provider, err := newStatusProvider(
		fakeStateStatus{metadata: state.Metadata{Initialized: true}},
		fakeXrayStatus{health: xray.Health{Uptime: 75 * time.Second}},
		fakeUsageStatus{},
	)
	if err != nil {
		t.Fatalf("newStatusProvider() вернул ошибку: %v", err)
	}
	status, err := provider.Status(context.Background())
	if err != nil {
		t.Fatalf("Status() вернул ошибку: %v", err)
	}
	if !status.StateWritable || status.NeedsBootstrap || !status.UsageCollectionSafe ||
		!status.Xray.Reachable || status.Xray.Uptime != 75*time.Second || !status.Ready() {
		t.Fatalf("status = %+v", status)
	}
}

func TestStatusProviderReturnsDegradedSanitizedXrayState(t *testing.T) {
	provider, err := newStatusProvider(
		fakeStateStatus{},
		fakeXrayStatus{err: errors.New("credential_uuid=secret")},
		fakeUsageStatus{status: usage.CollectionStatus{ConsecutiveFailures: 2}},
	)
	if err != nil {
		t.Fatalf("newStatusProvider() вернул ошибку: %v", err)
	}
	status, err := provider.Status(context.Background())
	if err != nil {
		t.Fatalf("Status() вернул ошибку: %v", err)
	}
	if status.Xray.Reachable || status.UsageCollectionSafe || status.Ready() {
		t.Fatalf("degraded status = %+v", status)
	}
	if status.Xray.LastError != xrayUnavailableDiagnostic ||
		strings.Contains(status.Xray.LastError, "credential_uuid") {
		t.Fatalf("Xray diagnostic = %q", status.Xray.LastError)
	}
}

func TestStatusProviderHidesMetadataError(t *testing.T) {
	provider, err := newStatusProvider(
		fakeStateStatus{metadataErr: errors.New("state secret leaked")},
		fakeXrayStatus{},
		fakeUsageStatus{},
	)
	if err != nil {
		t.Fatalf("newStatusProvider() вернул ошибку: %v", err)
	}
	_, err = provider.Status(context.Background())
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("Status() error = %v", err)
	}
}

type fakeStateStatus struct {
	metadata    state.Metadata
	metadataErr error
	writableErr error
}

func (source fakeStateStatus) Metadata(context.Context) (state.Metadata, error) {
	return source.metadata, source.metadataErr
}

func (source fakeStateStatus) Writable(context.Context) error {
	return source.writableErr
}

type fakeXrayStatus struct {
	health xray.Health
	err    error
}

func (source fakeXrayStatus) Health(context.Context) (xray.Health, error) {
	return source.health, source.err
}

type fakeUsageStatus struct {
	status usage.CollectionStatus
}

func (source fakeUsageStatus) Status() usage.CollectionStatus {
	return source.status
}
