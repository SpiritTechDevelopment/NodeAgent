package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodeagentv1 "github.com/SpiritTechDevelopment/NodeAgent/internal/gen/spiritvpn/nodeagent/v1"
)

type stubStatusProvider struct {
	status Status
	err    error
}

func (provider stubStatusProvider) Status(context.Context) (Status, error) {
	return provider.status, provider.err
}

func TestHealthReadiness(t *testing.T) {
	ready := Status{
		StateWritable:       true,
		UsageCollectionSafe: true,
		Xray:                XrayStatus{Reachable: true},
	}

	tests := []struct {
		name   string
		modify func(*Status)
		want   bool
	}{
		{name: "ready", modify: func(*Status) {}, want: true},
		{name: "state not writable", modify: func(status *Status) { status.StateWritable = false }},
		{name: "bootstrap required", modify: func(status *Status) { status.NeedsBootstrap = true }},
		{name: "usage collection unsafe", modify: func(status *Status) { status.UsageCollectionSafe = false }},
		{name: "Xray unreachable", modify: func(status *Status) { status.Xray.Reachable = false }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := ready
			test.modify(&snapshot)
			service := newTestService(t, stubStatusProvider{status: snapshot})

			response, err := service.Health(context.Background(), &nodeagentv1.HealthRequest{})
			if err != nil {
				t.Fatalf("Health returned error: %v", err)
			}
			if response.GetReady() != test.want {
				t.Fatalf("Health ready = %t, want %t", response.GetReady(), test.want)
			}
		})
	}
}

func TestHealthMapsStatus(t *testing.T) {
	lastClosedBucketEnd := time.Date(2026, time.August, 14, 12, 30, 0, 123_000_000, time.UTC)
	snapshot := Status{
		StateWritable:       true,
		NeedsBootstrap:      true,
		UsageCollectionSafe: true,
		Xray: XrayStatus{
			Reachable: true,
			Uptime:    90*time.Second + 900*time.Millisecond,
			LastError: "sanitized Xray diagnostic",
		},
		Activity: ActivityStatus{
			Enabled:             true,
			Healthy:             false,
			LastClosedBucketEnd: lastClosedBucketEnd,
			OutboxBatches:       12,
			LastError:           "sanitized activity diagnostic",
		},
	}
	service, err := New(
		Config{NodeID: " node-test ", AgentVersion: " test-version ", LocalOutboundTag: "direct"},
		newTestDependencies(t, stubStatusProvider{status: snapshot}),
	)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}

	response, err := service.Health(context.Background(), nil)
	if err != nil {
		t.Fatalf("Health returned error: %v", err)
	}

	if response.GetNodeId() != "node-test" {
		t.Errorf("node_id = %q, want node-test", response.GetNodeId())
	}
	if response.GetAgentVersion() != "test-version" {
		t.Errorf("agent_version = %q, want test-version", response.GetAgentVersion())
	}
	if response.GetReady() {
		t.Error("ready = true while bootstrap is required")
	}
	if !response.GetNeedsBootstrap() {
		t.Error("needs_bootstrap = false, want true")
	}
	if !response.GetXray().GetReachable() || response.GetXray().GetUptimeSeconds() != 90 {
		t.Errorf("Xray state = %+v, want reachable with 90 seconds uptime", response.GetXray())
	}
	if response.GetXray().GetLastError() != snapshot.Xray.LastError {
		t.Errorf("Xray last_error = %q, want %q", response.GetXray().GetLastError(), snapshot.Xray.LastError)
	}
	activity := response.GetActivity()
	if !activity.GetEnabled() || activity.GetHealthy() {
		t.Errorf("activity enabled=%t healthy=%t, want true and false", activity.GetEnabled(), activity.GetHealthy())
	}
	if activity.GetLastClosedBucketEndUnixMs() != lastClosedBucketEnd.UnixMilli() {
		t.Errorf("activity bucket end = %d, want %d", activity.GetLastClosedBucketEndUnixMs(), lastClosedBucketEnd.UnixMilli())
	}
	if activity.GetOutboxBatches() != 12 || activity.GetLastError() != snapshot.Activity.LastError {
		t.Errorf("activity state = %+v, want mapped outbox and diagnostic", activity)
	}
}

func TestHealthHidesProviderError(t *testing.T) {
	service := newTestService(t, stubStatusProvider{err: errors.New("credential_uuid must not escape")})

	_, err := service.Health(context.Background(), &nodeagentv1.HealthRequest{})
	if got := status.Code(err); got != codes.Unavailable {
		t.Fatalf("Health code = %s, want %s", got, codes.Unavailable)
	}
	if strings.Contains(err.Error(), "credential_uuid") {
		t.Fatalf("Health exposed provider error: %v", err)
	}
}

func TestNewValidatesRequiredDependencies(t *testing.T) {
	validDependencies := newTestDependencies(t, stubStatusProvider{})
	tests := []struct {
		name         string
		config       Config
		dependencies Dependencies
		want         string
	}{
		{
			name:         "missing node ID",
			config:       Config{AgentVersion: "test", LocalOutboundTag: "direct"},
			dependencies: validDependencies,
			want:         "node ID",
		},
		{
			name:         "missing agent version",
			config:       Config{NodeID: "node-test", LocalOutboundTag: "direct"},
			dependencies: validDependencies,
			want:         "agent version",
		},
		{
			name:         "missing local outbound",
			config:       Config{NodeID: "node-test", AgentVersion: "test"},
			dependencies: validDependencies,
			want:         "outbound tag",
		},
		{
			name: "invalid fallback outbound",
			config: Config{
				NodeID: "node-test", AgentVersion: "test", LocalOutboundTag: "direct",
				FallbackOutboundTag: " block ",
			},
			dependencies: validDependencies,
			want:         "outbound tag",
		},
		{
			name: "negative inventory limit",
			config: Config{
				NodeID: "node-test", AgentVersion: "test", LocalOutboundTag: "direct",
				MaxInventoryUsers: -1,
			},
			dependencies: validDependencies,
			want:         "maximum inventory users",
		},
		{
			name: "missing status provider",
			config: Config{
				NodeID: "node-test", AgentVersion: "test", LocalOutboundTag: "direct",
			},
			dependencies: Dependencies{
				State: validDependencies.State,
				Xray:  validDependencies.Xray,
				Usage: validDependencies.Usage,
			},
			want: "status provider",
		},
		{
			name: "missing state store",
			config: Config{
				NodeID: "node-test", AgentVersion: "test", LocalOutboundTag: "direct",
			},
			dependencies: Dependencies{
				Status: validDependencies.Status,
				Xray:   validDependencies.Xray,
				Usage:  validDependencies.Usage,
			},
			want: "state store",
		},
		{
			name: "missing Xray controller",
			config: Config{
				NodeID: "node-test", AgentVersion: "test", LocalOutboundTag: "direct",
			},
			dependencies: Dependencies{
				Status: validDependencies.Status,
				State:  validDependencies.State,
				Usage:  validDependencies.Usage,
			},
			want: "Xray user controller",
		},
		{
			name: "missing usage finalizer",
			config: Config{
				NodeID: "node-test", AgentVersion: "test", LocalOutboundTag: "direct",
			},
			dependencies: Dependencies{
				Status: validDependencies.Status,
				State:  validDependencies.State,
				Xray:   validDependencies.Xray,
			},
			want: "usage finalizer",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := New(test.config, test.dependencies)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("New error = %v, want text %q", err, test.want)
			}
		})
	}
}

func newTestService(t *testing.T, provider StatusProvider) *Service {
	t.Helper()
	service, err := New(
		Config{NodeID: "node-test", AgentVersion: "test", LocalOutboundTag: "direct"},
		newTestDependencies(t, provider),
	)
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	return service
}
