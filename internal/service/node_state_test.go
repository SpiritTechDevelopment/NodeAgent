package service

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	nodeagentv1 "github.com/SpiritTechDevelopment/NodeAgent/internal/gen/spiritvpn/nodeagent/v1"
)

func TestGetNodeStateReplaysAndAcknowledgesUsage(t *testing.T) {
	dependencies := newTestDependencies(t, stubStatusProvider{status: Status{
		Xray: XrayStatus{Reachable: true, Uptime: 75 * time.Second},
	}})
	service, err := New(
		Config{NodeID: "node-test", AgentVersion: "test", LocalOutboundTag: "direct"},
		dependencies,
	)
	if err != nil {
		t.Fatalf("New() вернул ошибку: %v", err)
	}
	collectedAt := time.Date(2026, time.August, 14, 16, 0, 0, 123_000_000, time.UTC)
	created, err := dependencies.State.AppendUsageBatches(context.Background(), collectedAt, [][]byte{
		usagePayload(t, testAccountingID, 10, 20),
		usagePayload(t, "u.bcdefghijklmnopqrstu", 30, 40),
	})
	if err != nil {
		t.Fatalf("AppendUsageBatches() вернул ошибку: %v", err)
	}
	spoolID := created[0].SpoolID

	first := getNodeState(t, service, &nodeagentv1.GetNodeStateRequest{MaxUsageBatches: 1})
	assertNodeStateBatch(t, first, spoolID, 1, testAccountingID, collectedAt)
	if first.GetNodeId() != "node-test" || first.GetAgentVersion() != "test" {
		t.Fatalf("идентичность ответа = %q/%q", first.GetNodeId(), first.GetAgentVersion())
	}
	if !first.GetNeedsBootstrap() {
		t.Error("новая SQLite не выставила needs_bootstrap")
	}
	if !first.GetXray().GetReachable() || first.GetXray().GetUptimeSeconds() != 75 {
		t.Fatalf("Xray state = %+v", first.GetXray())
	}

	emptyAck := getNodeState(t, service, &nodeagentv1.GetNodeStateRequest{
		AcknowledgedUsageThrough: &nodeagentv1.UsageCursor{},
		MaxUsageBatches:          1,
	})
	assertNodeStateBatch(t, emptyAck, spoolID, 1, testAccountingID, collectedAt)

	foreignAck := getNodeState(t, service, &nodeagentv1.GetNodeStateRequest{
		AcknowledgedUsageThrough: &nodeagentv1.UsageCursor{SpoolId: "foreign", Sequence: 1},
		MaxUsageBatches:          1,
	})
	assertNodeStateBatch(t, foreignAck, spoolID, 1, testAccountingID, collectedAt)

	_, err = service.GetNodeState(context.Background(), &nodeagentv1.GetNodeStateRequest{
		AcknowledgedUsageThrough: &nodeagentv1.UsageCursor{SpoolId: spoolID, Sequence: 1},
		MaxUsageBatches:          maximumUsageBatchLimit + 1,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("невалидный запрос code = %s, ожидался InvalidArgument", status.Code(err))
	}
	notAcknowledged := getNodeState(t, service, &nodeagentv1.GetNodeStateRequest{MaxUsageBatches: 1})
	assertNodeStateBatch(t, notAcknowledged, spoolID, 1, testAccountingID, collectedAt)

	second := getNodeState(t, service, &nodeagentv1.GetNodeStateRequest{
		AcknowledgedUsageThrough: &nodeagentv1.UsageCursor{SpoolId: spoolID, Sequence: 1},
		MaxUsageBatches:          1,
	})
	assertNodeStateBatch(t, second, spoolID, 2, "u.bcdefghijklmnopqrstu", collectedAt)

	beyondEmitted := getNodeState(t, service, &nodeagentv1.GetNodeStateRequest{
		AcknowledgedUsageThrough: &nodeagentv1.UsageCursor{SpoolId: spoolID, Sequence: 3},
	})
	assertNodeStateBatch(t, beyondEmitted, spoolID, 2, "u.bcdefghijklmnopqrstu", collectedAt)

	empty := getNodeState(t, service, &nodeagentv1.GetNodeStateRequest{
		AcknowledgedUsageThrough: &nodeagentv1.UsageCursor{SpoolId: spoolID, Sequence: 2},
	})
	if len(empty.GetUsageBatches()) != 0 {
		t.Fatalf("подтверждённые batch выданы повторно: %+v", empty.GetUsageBatches())
	}
}

func TestGetNodeStateReturnsEmptyCompleteInventoryAndRejectsLargeLimit(t *testing.T) {
	service := newTestService(t, stubStatusProvider{})

	response, err := service.GetNodeState(context.Background(), &nodeagentv1.GetNodeStateRequest{
		IncludeUsers: true,
	})
	if err != nil {
		t.Fatalf("GetNodeState(include_users) вернул ошибку: %v", err)
	}
	if !response.GetUsersComplete() || response.GetUsersObservedAtUnixMs() == 0 || len(response.GetUsers()) != 0 {
		t.Fatalf("пустой inventory = %+v", response)
	}

	_, err = service.GetNodeState(context.Background(), &nodeagentv1.GetNodeStateRequest{
		MaxUsageBatches: maximumUsageBatchLimit + 1,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("большой limit code = %s, ожидался InvalidArgument", status.Code(err))
	}
}

func TestGetNodeStateHidesInvalidStoredPayload(t *testing.T) {
	dependencies := newTestDependencies(t, stubStatusProvider{})
	service, err := New(
		Config{NodeID: "node-test", AgentVersion: "test", LocalOutboundTag: "direct"},
		dependencies,
	)
	if err != nil {
		t.Fatalf("New() вернул ошибку: %v", err)
	}
	if _, err := dependencies.State.AppendUsageBatches(
		context.Background(),
		time.Now(),
		[][]byte{{0xff}},
	); err != nil {
		t.Fatalf("AppendUsageBatches() вернул ошибку: %v", err)
	}

	_, err = service.GetNodeState(context.Background(), &nodeagentv1.GetNodeStateRequest{})
	if status.Code(err) != codes.Internal {
		t.Fatalf("повреждённый payload code = %s, ожидался Internal", status.Code(err))
	}
	if err.Error() != "rpc error: code = Internal desc = "+invalidUsagePayloadMessage {
		t.Fatalf("наружу вышла внутренняя ошибка: %v", err)
	}
}

func TestGetNodeStateHidesStatusProviderError(t *testing.T) {
	service := newTestService(t, stubStatusProvider{err: context.DeadlineExceeded})
	_, err := service.GetNodeState(context.Background(), nil)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("provider error code = %s, ожидался Unavailable", status.Code(err))
	}
}

func TestGetNodeStateRecordsBackendPollBeforeValidation(t *testing.T) {
	service := newTestService(t, stubStatusProvider{})
	want := time.Date(2026, time.August, 14, 18, 30, 0, 125_000_000, time.UTC)
	service.now = func() time.Time { return want }
	_, err := service.GetNodeState(context.Background(), &nodeagentv1.GetNodeStateRequest{
		MaxUsageBatches: maximumUsageBatchLimit + 1,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetNodeState() code = %s, ожидался InvalidArgument", status.Code(err))
	}
	if got := service.LastBackendPollAt(); !got.Equal(want) {
		t.Fatalf("LastBackendPollAt() = %v, ожидалось %v", got, want)
	}
}

func getNodeState(
	t *testing.T,
	service *Service,
	request *nodeagentv1.GetNodeStateRequest,
) *nodeagentv1.GetNodeStateResponse {
	t.Helper()
	response, err := service.GetNodeState(context.Background(), request)
	if err != nil {
		t.Fatalf("GetNodeState() вернул ошибку: %v", err)
	}
	return response
}

func assertNodeStateBatch(
	t *testing.T,
	response *nodeagentv1.GetNodeStateResponse,
	spoolID string,
	sequence uint64,
	accountingID string,
	collectedAt time.Time,
) {
	t.Helper()
	if len(response.GetUsageBatches()) != 1 {
		t.Fatalf("usage batches = %+v", response.GetUsageBatches())
	}
	batch := response.GetUsageBatches()[0]
	if batch.GetCursor().GetSpoolId() != spoolID || batch.GetCursor().GetSequence() != sequence {
		t.Fatalf("cursor = %+v", batch.GetCursor())
	}
	if batch.GetCollectedAtUnixMs() != collectedAt.UnixMilli() {
		t.Fatalf("collected_at = %d, ожидалось %d", batch.GetCollectedAtUnixMs(), collectedAt.UnixMilli())
	}
	if len(batch.GetItems()) != 1 || batch.GetItems()[0].GetAccountingId() != accountingID {
		t.Fatalf("items = %+v", batch.GetItems())
	}
}

func usagePayload(t *testing.T, accountingID string, uplink, downlink uint64) []byte {
	t.Helper()
	payload, err := proto.Marshal(&nodeagentv1.UsageBatch{Items: []*nodeagentv1.UserUsage{
		{AccountingId: accountingID, UplinkBytes: uplink, DownlinkBytes: downlink},
	}})
	if err != nil {
		t.Fatalf("кодировать usage payload: %v", err)
	}
	return payload
}
