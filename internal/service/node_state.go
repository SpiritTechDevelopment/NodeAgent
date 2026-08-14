package service

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodeagentv1 "github.com/SpiritTechDevelopment/NodeAgent/internal/gen/spiritvpn/nodeagent/v1"
	usagecollector "github.com/SpiritTechDevelopment/NodeAgent/internal/usage"
)

const (
	defaultUsageBatchLimit      = 16
	maximumUsageBatchLimit      = 1024
	nodeStateUnavailableMessage = "node state is unavailable"
	invalidBatchLimitMessage    = "usage batch limit is too large"
	invalidUsagePayloadMessage  = "stored usage batch is invalid"
)

// GetNodeState подтверждает ранее выданные usage batch, возвращает текущий outbox
// и по запросу добавляет полный снимок пользователей Xray.
func (s *Service) GetNodeState(
	ctx context.Context,
	request *nodeagentv1.GetNodeStateRequest,
) (*nodeagentv1.GetNodeStateResponse, error) {
	s.lastBackendPoll.Store(s.now().UTC().UnixNano())
	limit, err := usageBatchLimit(request)
	if err != nil {
		return nil, err
	}

	snapshot, err := s.status.Status(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, nodeStateUnavailableMessage)
	}
	metadata, err := s.state.Metadata(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, nodeStateUnavailableMessage)
	}

	var inventory observedInventory
	if request != nil && request.GetIncludeUsers() {
		inventory, err = s.observeInventory(ctx)
		if err != nil {
			return nil, err
		}
	}

	if request != nil && request.GetAcknowledgedUsageThrough() != nil {
		cursor := request.GetAcknowledgedUsageThrough()
		if _, err := s.state.AcknowledgeUsageBatches(
			ctx,
			cursor.GetSpoolId(),
			cursor.GetSequence(),
		); err != nil {
			return nil, status.Error(codes.Unavailable, nodeStateUnavailableMessage)
		}
	}

	storedBatches, err := s.state.PendingUsageBatches(ctx, limit)
	if err != nil {
		return nil, status.Error(codes.Unavailable, nodeStateUnavailableMessage)
	}
	wireBatches := make([]*nodeagentv1.UsageBatch, 0, len(storedBatches))
	for _, batch := range storedBatches {
		message, err := usagecollector.DecodeBatch(batch)
		if err != nil {
			return nil, status.Error(codes.Internal, invalidUsagePayloadMessage)
		}
		wireBatches = append(wireBatches, message)
	}

	response := &nodeagentv1.GetNodeStateResponse{
		NodeId:         s.nodeID,
		AgentVersion:   s.agentVersion,
		NeedsBootstrap: metadata.NeedsBootstrap(),
		Xray: &nodeagentv1.XrayState{
			Reachable:     snapshot.Xray.Reachable,
			UptimeSeconds: durationSeconds(snapshot.Xray.Uptime),
			LastError:     snapshot.Xray.LastError,
		},
		UsageBatches: wireBatches,
		Activity:     activityState(snapshot.Activity),
	}
	if request != nil && request.GetIncludeUsers() {
		response.UsersObservedAtUnixMs = inventory.observedAt.UnixMilli()
		response.Users = inventory.users
		response.UsersComplete = true
	}
	return response, nil
}

func usageBatchLimit(request *nodeagentv1.GetNodeStateRequest) (int, error) {
	if request == nil || request.GetMaxUsageBatches() == 0 {
		return defaultUsageBatchLimit, nil
	}
	if request.GetMaxUsageBatches() > maximumUsageBatchLimit {
		return 0, status.Error(codes.InvalidArgument, invalidBatchLimitMessage)
	}
	return int(request.GetMaxUsageBatches()), nil
}
