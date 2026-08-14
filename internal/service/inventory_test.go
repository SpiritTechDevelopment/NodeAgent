package service

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodeagentv1 "github.com/SpiritTechDevelopment/NodeAgent/internal/gen/spiritvpn/nodeagent/v1"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/xray"
)

func TestGetNodeStateReturnsCompleteActualInventory(t *testing.T) {
	dependencies := newTestDependencies(t, stubStatusProvider{})
	runtime := dependencies.Xray.(*fakeXrayController)
	secondID := "u.bcdefghijklmnopqrstu"
	runtime.users[testAccountingID] = xray.User{
		AccountingID:   testAccountingID,
		CredentialUUID: testCredentialUUID,
		Flow:           "xtls-rprx-vision",
	}
	runtime.users[secondID] = xray.User{
		AccountingID:   secondID,
		CredentialUUID: "22222222-2222-4222-8222-222222222222",
	}
	runtime.users["svc-monitoring"] = xray.User{
		AccountingID:   "svc-monitoring",
		CredentialUUID: "33333333-3333-4333-8333-333333333333",
	}
	runtime.rules[testAccountingID] = xray.UserRule{
		AccountingID: testAccountingID,
		OutboundTag:  "direct",
	}
	runtime.rules[secondID] = xray.UserRule{
		AccountingID: secondID,
		OutboundTag:  "bridge-configured",
	}
	runtime.routes[secondID] = "bridge-actual"
	service, err := New(
		Config{NodeID: "node-test", AgentVersion: "test", LocalOutboundTag: "direct"},
		dependencies,
	)
	if err != nil {
		t.Fatalf("New() вернул ошибку: %v", err)
	}
	observedAt := time.Date(2026, time.August, 14, 19, 0, 0, 123_000_000, time.UTC)
	service.now = func() time.Time { return observedAt }
	created, err := dependencies.State.AppendUsageBatches(
		context.Background(),
		observedAt.Add(-time.Minute),
		[][]byte{usagePayload(t, testAccountingID, 1, 2)},
	)
	if err != nil {
		t.Fatalf("AppendUsageBatches() вернул ошибку: %v", err)
	}

	response, err := service.GetNodeState(context.Background(), &nodeagentv1.GetNodeStateRequest{
		IncludeUsers: true,
	})
	if err != nil {
		t.Fatalf("GetNodeState() вернул ошибку: %v", err)
	}
	if !response.GetUsersComplete() || response.GetUsersObservedAtUnixMs() != observedAt.UnixMilli() {
		t.Fatalf("inventory metadata: complete=%t observed_at=%d", response.GetUsersComplete(), response.GetUsersObservedAtUnixMs())
	}
	if len(response.GetUsers()) != 3 {
		t.Fatalf("users = %+v", response.GetUsers())
	}
	gotOrder := []string{
		response.GetUsers()[0].GetUser().GetAccountingId(),
		response.GetUsers()[1].GetUser().GetAccountingId(),
		response.GetUsers()[2].GetUser().GetAccountingId(),
	}
	wantOrder := []string{"svc-monitoring", testAccountingID, secondID}
	if !slices.Equal(gotOrder, wantOrder) {
		t.Fatalf("порядок inventory = %v, ожидался %v", gotOrder, wantOrder)
	}
	byID := actualUsersByID(response.GetUsers())
	if byID[testAccountingID].GetUser().GetEgressKey() != "" ||
		!byID[testAccountingID].GetBackendManaged() ||
		byID[testAccountingID].GetUser().GetCredentialUuid() != testCredentialUUID ||
		byID[testAccountingID].GetUser().GetFlow() != "xtls-rprx-vision" {
		t.Fatalf("локальный actual user = %+v", byID[testAccountingID])
	}
	if byID[secondID].GetUser().GetEgressKey() != "bridge-actual" ||
		!byID[secondID].GetBackendManaged() {
		t.Fatalf("bridge actual user = %+v", byID[secondID])
	}
	if byID["svc-monitoring"].GetUser().GetEgressKey() != "block" ||
		byID["svc-monitoring"].GetBackendManaged() {
		t.Fatalf("infrastructure actual user = %+v", byID["svc-monitoring"])
	}
	if len(response.GetUsageBatches()) != 1 ||
		response.GetUsageBatches()[0].GetCursor().GetSequence() != created[0].Sequence {
		t.Fatalf("inventory poll не вернул usage batch: %+v", response.GetUsageBatches())
	}

	replayed := getNodeState(t, service, &nodeagentv1.GetNodeStateRequest{})
	if len(replayed.GetUsageBatches()) != 1 ||
		replayed.GetUsageBatches()[0].GetCursor().GetSequence() != created[0].Sequence {
		t.Fatalf("inventory poll подтвердил usage: %+v", replayed.GetUsageBatches())
	}
}

func TestInventoryReportsFallbackWhenPersonalRuleIsMissing(t *testing.T) {
	dependencies := newTestDependencies(t, stubStatusProvider{})
	runtime := dependencies.Xray.(*fakeXrayController)
	runtime.users[testAccountingID] = xray.User{
		AccountingID:   testAccountingID,
		CredentialUUID: testCredentialUUID,
	}
	service, err := New(
		Config{
			NodeID:              "node-test",
			AgentVersion:        "test",
			LocalOutboundTag:    "direct",
			FallbackOutboundTag: "deny-all",
		},
		dependencies,
	)
	if err != nil {
		t.Fatalf("New() вернул ошибку: %v", err)
	}

	response := getNodeState(t, service, &nodeagentv1.GetNodeStateRequest{IncludeUsers: true})
	if len(response.GetUsers()) != 1 || response.GetUsers()[0].GetUser().GetEgressKey() != "deny-all" {
		t.Fatalf("actual user без правила = %+v", response.GetUsers())
	}
	for _, call := range runtime.calls {
		if call == "read:TestUserRoute" {
			t.Fatal("TestRoute вызван без детерминированного персонального правила")
		}
	}
}

func TestInventoryReportsFallbackWhenRuleDoesNotMatchUser(t *testing.T) {
	dependencies := newTestDependencies(t, stubStatusProvider{})
	runtime := dependencies.Xray.(*fakeXrayController)
	runtime.users[testAccountingID] = xray.User{
		AccountingID:   testAccountingID,
		CredentialUUID: testCredentialUUID,
	}
	runtime.rules[testAccountingID] = xray.UserRule{
		AccountingID: testAccountingID,
		OutboundTag:  "direct",
	}
	runtime.failMethod = "TestUserRoute"
	runtime.failErr = xray.ErrRouteNotFound
	service, err := New(
		Config{NodeID: "node-test", AgentVersion: "test", LocalOutboundTag: "direct"},
		dependencies,
	)
	if err != nil {
		t.Fatalf("New() вернул ошибку: %v", err)
	}

	response := getNodeState(t, service, &nodeagentv1.GetNodeStateRequest{IncludeUsers: true})
	if len(response.GetUsers()) != 1 || response.GetUsers()[0].GetUser().GetEgressKey() != "block" {
		t.Fatalf("actual user с несовпавшим правилом = %+v", response.GetUsers())
	}
}

func TestInventoryReturnsNoPartialSnapshotOnXrayFailure(t *testing.T) {
	dependencies := newTestDependencies(t, stubStatusProvider{})
	runtime := dependencies.Xray.(*fakeXrayController)
	runtime.users[testAccountingID] = xray.User{
		AccountingID:   testAccountingID,
		CredentialUUID: testCredentialUUID,
	}
	runtime.rules[testAccountingID] = xray.UserRule{
		AccountingID: testAccountingID,
		OutboundTag:  "direct",
	}
	runtime.failMethod = "TestUserRoute"
	runtime.failErr = errors.New("routing unavailable")
	service, err := New(
		Config{NodeID: "node-test", AgentVersion: "test", LocalOutboundTag: "direct"},
		dependencies,
	)
	if err != nil {
		t.Fatalf("New() вернул ошибку: %v", err)
	}

	response, err := service.GetNodeState(context.Background(), &nodeagentv1.GetNodeStateRequest{
		IncludeUsers: true,
	})
	if status.Code(err) != codes.Unavailable || response != nil {
		t.Fatalf("Xray failure: response=%+v error=%v", response, err)
	}
}

func TestInventoryRejectsOversizedSnapshotBeforeAcknowledgement(t *testing.T) {
	dependencies := newTestDependencies(t, stubStatusProvider{})
	runtime := dependencies.Xray.(*fakeXrayController)
	runtime.users[testAccountingID] = xray.User{
		AccountingID:   testAccountingID,
		CredentialUUID: testCredentialUUID,
	}
	runtime.users["u.bcdefghijklmnopqrstu"] = xray.User{
		AccountingID:   "u.bcdefghijklmnopqrstu",
		CredentialUUID: "22222222-2222-4222-8222-222222222222",
	}
	service, err := New(
		Config{
			NodeID:            "node-test",
			AgentVersion:      "test",
			LocalOutboundTag:  "direct",
			MaxInventoryUsers: 1,
		},
		dependencies,
	)
	if err != nil {
		t.Fatalf("New() вернул ошибку: %v", err)
	}
	created, err := dependencies.State.AppendUsageBatches(
		context.Background(),
		time.Now(),
		[][]byte{usagePayload(t, testAccountingID, 1, 0)},
	)
	if err != nil {
		t.Fatalf("AppendUsageBatches() вернул ошибку: %v", err)
	}
	if _, err := dependencies.State.PendingUsageBatches(context.Background(), 1); err != nil {
		t.Fatalf("выдать usage для подготовки ack: %v", err)
	}

	response, err := service.GetNodeState(context.Background(), &nodeagentv1.GetNodeStateRequest{
		IncludeUsers: true,
		AcknowledgedUsageThrough: &nodeagentv1.UsageCursor{
			SpoolId:  created[0].SpoolID,
			Sequence: created[0].Sequence,
		},
	})
	if status.Code(err) != codes.ResourceExhausted || response != nil {
		t.Fatalf("oversized inventory: response=%+v error=%v", response, err)
	}
	pending := getNodeState(t, service, &nodeagentv1.GetNodeStateRequest{})
	if len(pending.GetUsageBatches()) != 1 {
		t.Fatal("ошибка inventory применила acknowledgement")
	}
}

func actualUsersByID(users []*nodeagentv1.ActualUser) map[string]*nodeagentv1.ActualUser {
	result := make(map[string]*nodeagentv1.ActualUser, len(users))
	for _, user := range users {
		result[user.GetUser().GetAccountingId()] = user
	}
	return result
}
