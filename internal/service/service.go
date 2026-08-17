package service

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodeagentv1 "github.com/SpiritTechDevelopment/NodeAgent/internal/gen/spiritvpn/nodeagent/v1"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/state"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/xray"
)

const (
	healthUnavailableMessage = "node agent health is unavailable"
)

var _ nodeagentv1.NodeAgentServiceServer = (*Service)(nil)

// Config задаёт параметры прикладного сервиса агента ноды.
type Config struct {
	// NodeID содержит стабильный идентификатор ноды, заданный инфраструктурой.
	NodeID string
	// AgentVersion содержит версию запущенного бинарного файла агента ноды.
	AgentVersion string
	// LocalOutboundTag содержит Xray outbound для пустого egress_key.
	LocalOutboundTag string
	// FallbackOutboundTag содержит Xray outbound по умолчанию для запросов без совпавшего правила.
	// Пустое значение выбирает стандартный тег block.
	FallbackOutboundTag string
	// MaxInventoryUsers ограничивает число пользователей в полном инвентаре и reconcile-запросе.
	// Ноль выбирает расчётный предел 2000 пользователей.
	MaxInventoryUsers int
}

// Dependencies объединяет локальные зависимости прикладного сервиса.
type Dependencies struct {
	// Status предоставляет данные для Health.
	Status StatusProvider
	// State хранит durable intent и журнал идемпотентности.
	State *state.Store
	// Xray управляет пользователями и их персональными правилами.
	Xray XrayUserController
	// Usage сохраняет остаток трафика перед заменой или удалением пользователя.
	Usage UsageFinalizer
}

// Service реализует прикладной gRPC-сервис агента ноды.
type Service struct {
	nodeagentv1.UnimplementedNodeAgentServiceServer

	nodeID              string
	agentVersion        string
	localOutboundTag    string
	fallbackOutboundTag string
	maxInventoryUsers   int
	status              StatusProvider
	state               *state.Store
	xray                XrayUserController
	usage               UsageFinalizer
	mutations           sync.Mutex
	reconciling         atomic.Bool
	selfHealRunning     atomic.Bool
	lastBackendPoll     atomic.Int64
	localReconcileErrs  atomic.Uint64
	selfHealInterval    time.Duration
	xrayProbeInterval   time.Duration
	now                 func() time.Time
}

// LastBackendPollAt возвращает время последнего вызова GetNodeState.
func (s *Service) LastBackendPollAt() time.Time {
	value := s.lastBackendPoll.Load()
	if value == 0 {
		return time.Time{}
	}
	return time.Unix(0, value).UTC()
}

// LocalReconcileErrors возвращает число ошибок локального self-heal.
func (s *Service) LocalReconcileErrors() uint64 {
	return s.localReconcileErrs.Load()
}

// New проверяет config и создаёт прикладной сервис агента ноды.
func New(config Config, dependencies Dependencies) (*Service, error) {
	nodeID := strings.TrimSpace(config.NodeID)
	if nodeID == "" {
		return nil, errors.New("node ID is required")
	}
	agentVersion := strings.TrimSpace(config.AgentVersion)
	if agentVersion == "" {
		return nil, errors.New("agent version is required")
	}
	localOutboundTag := config.LocalOutboundTag
	if err := xray.ValidateOutboundTag(localOutboundTag); err != nil {
		return nil, err
	}
	fallbackOutboundTag := config.FallbackOutboundTag
	if fallbackOutboundTag == "" {
		fallbackOutboundTag = defaultFallbackOutboundTag
	}
	if err := xray.ValidateOutboundTag(fallbackOutboundTag); err != nil {
		return nil, err
	}
	maxInventoryUsers := config.MaxInventoryUsers
	if maxInventoryUsers == 0 {
		maxInventoryUsers = defaultMaxInventoryUsers
	}
	if maxInventoryUsers < 0 {
		return nil, errors.New("maximum inventory users must not be negative")
	}
	if dependencies.Status == nil {
		return nil, errors.New("status provider is required")
	}
	if dependencies.State == nil {
		return nil, errors.New("state store is required")
	}
	if dependencies.Xray == nil {
		return nil, errors.New("Xray user controller is required")
	}
	if dependencies.Usage == nil {
		return nil, errors.New("usage finalizer is required")
	}

	return &Service{
		nodeID:              nodeID,
		agentVersion:        agentVersion,
		localOutboundTag:    localOutboundTag,
		fallbackOutboundTag: fallbackOutboundTag,
		maxInventoryUsers:   maxInventoryUsers,
		status:              dependencies.Status,
		state:               dependencies.State,
		xray:                dependencies.Xray,
		usage:               dependencies.Usage,
		selfHealInterval:    defaultSelfHealInterval,
		xrayProbeInterval:   defaultXrayProbeInterval,
		now:                 time.Now,
	}, nil
}

// Health возвращает текущее состояние готовности без обращения к backend.
func (s *Service) Health(ctx context.Context, _ *nodeagentv1.HealthRequest) (*nodeagentv1.HealthResponse, error) {
	snapshot, err := s.status.Status(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, healthUnavailableMessage)
	}

	return &nodeagentv1.HealthResponse{
		NodeId:         s.nodeID,
		AgentVersion:   s.agentVersion,
		Ready:          snapshot.Ready(),
		NeedsBootstrap: snapshot.NeedsBootstrap,
		Xray: &nodeagentv1.XrayState{
			Reachable:     snapshot.Xray.Reachable,
			UptimeSeconds: durationSeconds(snapshot.Xray.Uptime),
			LastError:     snapshot.Xray.LastError,
		},
		Activity: activityState(snapshot.Activity),
	}, nil
}

func durationSeconds(value time.Duration) uint64 {
	if value <= 0 {
		return 0
	}
	return uint64(value / time.Second)
}

func activityState(value ActivityStatus) *nodeagentv1.ActivityState {
	var lastClosedBucketEnd int64
	if !value.LastClosedBucketEnd.IsZero() {
		lastClosedBucketEnd = value.LastClosedBucketEnd.UnixMilli()
	}

	return &nodeagentv1.ActivityState{
		Enabled:                   value.Enabled,
		Healthy:                   value.Healthy,
		LastClosedBucketEndUnixMs: lastClosedBucketEnd,
		OutboxBatches:             value.OutboxBatches,
		LastError:                 value.LastError,
	}
}
