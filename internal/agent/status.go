package agent

import (
	"context"
	"errors"

	"github.com/SpiritTechDevelopment/NodeAgent/internal/service"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/state"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/usage"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/xray"
)

const xrayUnavailableDiagnostic = "локальный API Xray недоступен"

type stateStatusSource interface {
	Metadata(context.Context) (state.Metadata, error)
	Writable(context.Context) error
}

type xrayStatusSource interface {
	Health(context.Context) (xray.Health, error)
}

type usageStatusSource interface {
	Status() usage.CollectionStatus
}

type statusProvider struct {
	state stateStatusSource
	xray  xrayStatusSource
	usage usageStatusSource
}

func newStatusProvider(
	stateSource stateStatusSource,
	xraySource xrayStatusSource,
	usageSource usageStatusSource,
) (*statusProvider, error) {
	if stateSource == nil || xraySource == nil || usageSource == nil {
		return nil, errors.New("status dependencies are required")
	}
	return &statusProvider{state: stateSource, xray: xraySource, usage: usageSource}, nil
}

func (provider *statusProvider) Status(ctx context.Context) (service.Status, error) {
	metadata, err := provider.state.Metadata(ctx)
	if err != nil {
		return service.Status{}, errors.New("read local state metadata")
	}
	status := service.Status{
		NeedsBootstrap:      metadata.NeedsBootstrap(),
		UsageCollectionSafe: provider.usage.Status().ConsecutiveFailures == 0,
	}
	if err := provider.state.Writable(ctx); err == nil {
		status.StateWritable = true
	}
	health, err := provider.xray.Health(ctx)
	if err != nil {
		status.Xray.LastError = xrayUnavailableDiagnostic
		return status, nil
	}
	status.Xray.Reachable = true
	status.Xray.Uptime = health.Uptime
	return status, nil
}
