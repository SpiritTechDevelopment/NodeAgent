package service

import (
	"context"
	"errors"
	"slices"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodeagentv1 "github.com/SpiritTechDevelopment/NodeAgent/internal/gen/spiritvpn/nodeagent/v1"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/state"
	usagecollector "github.com/SpiritTechDevelopment/NodeAgent/internal/usage"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/xray"
)

const (
	secondAccountingID = "u.bcdefghijklmnopqrstu"
	thirdAccountingID  = "u.cdefghijklmnopqrstuv"
	extraAccountingID  = "u.defghijklmnopqrstuvw"
	orphanAccountingID = "u.efghijklmnopqrstuvwx"
	secondCredential   = "22222222-2222-4222-8222-222222222222"
	thirdCredential    = "33333333-3333-4333-8333-333333333333"
	extraCredential    = "44444444-4444-4444-8444-444444444444"
)

func TestReconcileUsersAppliesCompleteSetPersistsAndReplays(t *testing.T) {
	events := make([]string, 0)
	service, store, runtime, usage := newUserTestService(t)
	runtime.events = &events
	usage.events = &events
	runtime.users[testAccountingID] = xray.User{
		AccountingID:   testAccountingID,
		CredentialUUID: testCredentialUUID,
	}
	runtime.rules[testAccountingID] = xray.UserRule{
		AccountingID: testAccountingID,
		OutboundTag:  "direct",
	}
	runtime.users[secondAccountingID] = xray.User{
		AccountingID:   secondAccountingID,
		CredentialUUID: extraCredential,
	}
	runtime.rules[secondAccountingID] = xray.UserRule{
		AccountingID: secondAccountingID,
		OutboundTag:  "old-egress",
	}
	runtime.users[extraAccountingID] = xray.User{
		AccountingID:   extraAccountingID,
		CredentialUUID: extraCredential,
	}
	runtime.rules[extraAccountingID] = xray.UserRule{
		AccountingID: extraAccountingID,
		OutboundTag:  "direct",
	}
	runtime.rules[orphanAccountingID] = xray.UserRule{
		AccountingID: orphanAccountingID,
		OutboundTag:  "direct",
	}
	runtime.users["svc-monitoring"] = xray.User{
		AccountingID:   "svc-monitoring",
		CredentialUUID: "55555555-5555-4555-8555-555555555555",
	}

	first := reconcileUser(testAccountingID, testCredentialUUID, "")
	second := reconcileUser(secondAccountingID, secondCredential, "bridge-test")
	third := reconcileUser(thirdAccountingID, thirdCredential, "")
	request := &nodeagentv1.ReconcileUsersRequest{
		OperationId: "reconcile-complete",
		Complete:    true,
		Users:       []*nodeagentv1.User{third, first, second},
	}

	response, err := service.ReconcileUsers(context.Background(), request)
	if err != nil {
		t.Fatalf("ReconcileUsers() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, response.GetOperation(), nodeagentv1.ApplyStatus_APPLY_STATUS_APPLIED)
	if response.GetAdded() != 1 || response.GetReplaced() != 1 ||
		response.GetRemoved() != 1 || response.GetUnchanged() != 1 {
		t.Fatalf("счётчики reconcile = %+v", response)
	}
	if usage.flushCalls != 1 || len(usage.calls) != 0 {
		t.Fatalf("usage flush=%d finalizations=%v", usage.flushCalls, usage.calls)
	}
	if len(events) == 0 || events[0] != "Flush" {
		t.Fatalf("первое событие массовых изменений = %v", events)
	}
	assertReconciledRuntime(t, runtime, map[string]string{
		testAccountingID:   "direct",
		secondAccountingID: "bridge-test",
		thirdAccountingID:  "direct",
	})
	if _, found := runtime.users["svc-monitoring"]; !found {
		t.Fatal("reconcile удалил инфраструктурного пользователя")
	}
	if _, found := runtime.rules[orphanAccountingID]; found {
		t.Fatal("reconcile оставил осиротевшее backend-owned правило")
	}
	for _, accountingID := range []string{testAccountingID, secondAccountingID, thirdAccountingID} {
		user, err := store.ManagedUser(context.Background(), accountingID)
		if err != nil || !user.DesiredPresent || !user.Applied {
			t.Fatalf("managed user %q = %+v, error=%v", accountingID, user, err)
		}
	}
	for _, accountingID := range []string{extraAccountingID, orphanAccountingID} {
		user, err := store.ManagedUser(context.Background(), accountingID)
		if err != nil || user.DesiredPresent || !user.Applied {
			t.Fatalf("tombstone %q = %+v, error=%v", accountingID, user, err)
		}
	}
	metadata, err := store.Metadata(context.Background())
	if err != nil || metadata.NeedsBootstrap() {
		t.Fatalf("metadata после reconcile = %+v, error=%v", metadata, err)
	}
	assertCompletedOperation(t, store, request.GetOperationId())

	mutations := runtime.mutationCount()
	replayed, err := service.ReconcileUsers(context.Background(), &nodeagentv1.ReconcileUsersRequest{
		OperationId: request.GetOperationId(),
		Complete:    true,
		Users:       []*nodeagentv1.User{second, third, first},
	})
	if err != nil {
		t.Fatalf("повторный ReconcileUsers() вернул gRPC-ошибку: %v", err)
	}
	if replayed.GetAdded() != response.GetAdded() || replayed.GetReplaced() != response.GetReplaced() ||
		replayed.GetRemoved() != response.GetRemoved() || replayed.GetUnchanged() != response.GetUnchanged() {
		t.Fatalf("replay потерял счётчики: %+v", replayed)
	}
	if runtime.mutationCount() != mutations || usage.flushCalls != 1 {
		t.Fatalf("replay повторил изменения: Xray=%v flush=%d", runtime.calls, usage.flushCalls)
	}
	conflicting := &nodeagentv1.ReconcileUsersRequest{
		OperationId: request.GetOperationId(),
		Complete:    true,
		Users: []*nodeagentv1.User{
			first,
			second,
			reconcileUser(thirdAccountingID, thirdCredential, "other-egress"),
		},
	}
	conflict, err := service.ReconcileUsers(context.Background(), conflicting)
	if err != nil {
		t.Fatalf("конфликтный ReconcileUsers() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, conflict.GetOperation(), nodeagentv1.ApplyStatus_APPLY_STATUS_PERMANENT_ERROR)
	if conflict.GetOperation().GetMessage() != operationConflictMessage || runtime.mutationCount() != mutations {
		t.Fatalf("конфликт operation_id = %+v, Xray=%v", conflict, runtime.calls)
	}
}

func TestReconcileUsersAcceptsEmptySetAndIgnoresFlushFailure(t *testing.T) {
	service, store, runtime, usage := newUserTestService(t)
	runtime.users[testAccountingID] = xray.User{
		AccountingID:   testAccountingID,
		CredentialUUID: testCredentialUUID,
	}
	runtime.rules[testAccountingID] = xray.UserRule{
		AccountingID: testAccountingID,
		OutboundTag:  "direct",
	}
	runtime.users["svc-monitoring"] = xray.User{AccountingID: "svc-monitoring"}
	usage.err = errors.New("bulk usage unavailable")

	response, err := service.ReconcileUsers(context.Background(), &nodeagentv1.ReconcileUsersRequest{
		OperationId: "reconcile-empty",
		Complete:    true,
	})
	if err != nil {
		t.Fatalf("ReconcileUsers() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, response.GetOperation(), nodeagentv1.ApplyStatus_APPLY_STATUS_APPLIED)
	if response.GetRemoved() != 1 || usage.flushCalls != 1 {
		t.Fatalf("empty reconcile = %+v, flush=%d", response, usage.flushCalls)
	}
	if _, found := runtime.users[testAccountingID]; found {
		t.Fatal("backend-owned пользователь не удалён пустым полным набором")
	}
	if _, found := runtime.users["svc-monitoring"]; !found {
		t.Fatal("инфраструктурный пользователь удалён пустым набором")
	}
	metadata, err := store.Metadata(context.Background())
	if err != nil || metadata.NeedsBootstrap() {
		t.Fatalf("bootstrap не завершён: metadata=%+v error=%v", metadata, err)
	}
}

func TestReconcileUsersFlushesUsageToSQLiteBeforeRemoval(t *testing.T) {
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
	source := &serviceUsageSource{items: []xray.Usage{{
		AccountingID:  testAccountingID,
		UplinkBytes:   17,
		DownlinkBytes: 19,
	}}}
	collector, err := usagecollector.New(dependencies.State, source)
	if err != nil {
		t.Fatalf("создать usage collector: %v", err)
	}
	dependencies.Usage = collector
	service, err := New(
		Config{NodeID: "node-test", AgentVersion: "test", LocalOutboundTag: "direct"},
		dependencies,
	)
	if err != nil {
		t.Fatalf("New() вернул ошибку: %v", err)
	}

	response, err := service.ReconcileUsers(context.Background(), &nodeagentv1.ReconcileUsersRequest{
		OperationId: "reconcile-real-flush",
		Complete:    true,
	})
	if err != nil {
		t.Fatalf("ReconcileUsers() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, response.GetOperation(), nodeagentv1.ApplyStatus_APPLY_STATUS_APPLIED)
	if source.bulkCalls != 1 {
		t.Fatalf("bulk reset calls = %d, ожидался один", source.bulkCalls)
	}
	pending, err := dependencies.State.PendingUsageBatches(context.Background(), 10)
	if err != nil {
		t.Fatalf("PendingUsageBatches() вернул ошибку: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending usage batches = %+v", pending)
	}
	batch, err := usagecollector.DecodeBatch(pending[0])
	if err != nil {
		t.Fatalf("DecodeBatch() вернул ошибку: %v", err)
	}
	if len(batch.GetItems()) != 1 || batch.GetItems()[0].GetAccountingId() != testAccountingID ||
		batch.GetItems()[0].GetUplinkBytes() != 17 || batch.GetItems()[0].GetDownlinkBytes() != 19 {
		t.Fatalf("usage batch после reconcile = %+v", batch)
	}
	if _, found := runtime.users[testAccountingID]; found {
		t.Fatal("пользователь остался после успешного flush и reconcile")
	}
}

func TestReconcileUsersValidatesWholeRequestBeforeXrayMutation(t *testing.T) {
	tests := []struct {
		name    string
		request *nodeagentv1.ReconcileUsersRequest
	}{
		{
			name: "incomplete",
			request: &nodeagentv1.ReconcileUsersRequest{
				OperationId: "reconcile-incomplete",
			},
		},
		{
			name: "duplicate",
			request: &nodeagentv1.ReconcileUsersRequest{
				OperationId: "reconcile-duplicate",
				Complete:    true,
				Users: []*nodeagentv1.User{
					reconcileUser(testAccountingID, testCredentialUUID, ""),
					reconcileUser(testAccountingID, secondCredential, ""),
				},
			},
		},
		{
			name: "invalid second user",
			request: &nodeagentv1.ReconcileUsersRequest{
				OperationId: "reconcile-invalid-user",
				Complete:    true,
				Users: []*nodeagentv1.User{
					reconcileUser(testAccountingID, testCredentialUUID, ""),
					reconcileUser(secondAccountingID, "secret-not-a-uuid", ""),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service, store, runtime, usage := newUserTestService(t)
			runtime.users[extraAccountingID] = xray.User{
				AccountingID:   extraAccountingID,
				CredentialUUID: extraCredential,
			}

			response, err := service.ReconcileUsers(context.Background(), test.request)
			if err != nil {
				t.Fatalf("ReconcileUsers() вернул gRPC-ошибку: %v", err)
			}
			assertApplyStatus(t, response.GetOperation(), nodeagentv1.ApplyStatus_APPLY_STATUS_PERMANENT_ERROR)
			if runtime.mutationCount() != 0 || usage.flushCalls != 0 {
				t.Fatalf("невалидный набор изменил зависимости: Xray=%v flush=%d", runtime.calls, usage.flushCalls)
			}
			if _, found := runtime.users[extraAccountingID]; !found {
				t.Fatal("невалидный набор удалил существующего пользователя")
			}
			assertCompletedOperation(t, store, test.request.GetOperationId())
			metadata, metadataErr := store.Metadata(context.Background())
			if metadataErr != nil || !metadata.NeedsBootstrap() {
				t.Fatalf("невалидный набор завершил bootstrap: metadata=%+v error=%v", metadata, metadataErr)
			}
		})
	}
}

func TestReconcileUsersRejectsSetAboveConfiguredLimit(t *testing.T) {
	dependencies := newTestDependencies(t, stubStatusProvider{})
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
	runtime := dependencies.Xray.(*fakeXrayController)
	response, err := service.ReconcileUsers(context.Background(), &nodeagentv1.ReconcileUsersRequest{
		OperationId: "reconcile-too-large",
		Complete:    true,
		Users: []*nodeagentv1.User{
			reconcileUser(testAccountingID, testCredentialUUID, ""),
			reconcileUser(secondAccountingID, secondCredential, ""),
		},
	})
	if err != nil {
		t.Fatalf("ReconcileUsers() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, response.GetOperation(), nodeagentv1.ApplyStatus_APPLY_STATUS_PERMANENT_ERROR)
	if runtime.mutationCount() != 0 {
		t.Fatalf("превышенный набор изменил Xray: %v", runtime.calls)
	}
}

func TestReconcileUsersContinuesPendingPartialApplication(t *testing.T) {
	service, store, runtime, _ := newUserTestService(t)
	runtime.failMethod = "AddUserRule"
	runtime.failErr = errors.New("routing unavailable")
	request := &nodeagentv1.ReconcileUsersRequest{
		OperationId: "reconcile-retry",
		Complete:    true,
		Users: []*nodeagentv1.User{
			reconcileUser(testAccountingID, testCredentialUUID, ""),
		},
	}

	first, err := service.ReconcileUsers(context.Background(), request)
	if err != nil {
		t.Fatalf("первый ReconcileUsers() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, first.GetOperation(), nodeagentv1.ApplyStatus_APPLY_STATUS_RETRYABLE_ERROR)
	assertPendingOperation(t, store, request.GetOperationId())
	user, err := store.ManagedUser(context.Background(), testAccountingID)
	if err != nil || !user.DesiredPresent || user.Applied {
		t.Fatalf("intent после частичной ошибки = %+v, error=%v", user, err)
	}

	second, err := service.ReconcileUsers(context.Background(), request)
	if err != nil {
		t.Fatalf("повторный ReconcileUsers() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, second.GetOperation(), nodeagentv1.ApplyStatus_APPLY_STATUS_APPLIED)
	if second.GetReplaced() != 1 {
		t.Fatalf("счётчики продолженной попытки = %+v", second)
	}
	assertRuntimeUser(t, runtime, testCredentialUUID, "direct")
	assertCompletedOperation(t, store, request.GetOperationId())
}

func TestReconcileUsersRepairsEffectiveRouteDrift(t *testing.T) {
	service, _, runtime, _ := newUserTestService(t)
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

	response, err := service.ReconcileUsers(context.Background(), &nodeagentv1.ReconcileUsersRequest{
		OperationId: "reconcile-route-drift",
		Complete:    true,
		Users: []*nodeagentv1.User{
			reconcileUser(testAccountingID, testCredentialUUID, ""),
		},
	})
	if err != nil {
		t.Fatalf("ReconcileUsers() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, response.GetOperation(), nodeagentv1.ApplyStatus_APPLY_STATUS_APPLIED)
	if response.GetReplaced() != 1 || response.GetUnchanged() != 0 {
		t.Fatalf("route drift counters = %+v", response)
	}
	if !slices.Contains(runtime.calls, "mutation:RemoveUserRule") ||
		!slices.Contains(runtime.calls, "mutation:AddUserRule") {
		t.Fatalf("route drift не переустановил правило: %v", runtime.calls)
	}
}

func TestEnsureMethodsRejectCallsWhileReconcileOwnsMutationGate(t *testing.T) {
	service, _, _, _ := newUserTestService(t)
	service.reconciling.Store(true)
	t.Cleanup(func() { service.reconciling.Store(false) })

	if _, err := service.EnsureUserPresent(
		context.Background(),
		presentRequest("ensure-during-reconcile", testCredentialUUID, ""),
	); status.Code(err) != codes.Unavailable {
		t.Fatalf("EnsureUserPresent code = %s, ожидался Unavailable", status.Code(err))
	}
	if _, err := service.EnsureUserAbsent(context.Background(), &nodeagentv1.EnsureUserAbsentRequest{
		OperationId:  "absent-during-reconcile",
		AccountingId: testAccountingID,
	}); status.Code(err) != codes.Unavailable {
		t.Fatalf("EnsureUserAbsent code = %s, ожидался Unavailable", status.Code(err))
	}
}

func reconcileUser(accountingID, credentialUUID, egressKey string) *nodeagentv1.User {
	return &nodeagentv1.User{
		AccountingId:   accountingID,
		CredentialUuid: credentialUUID,
		EgressKey:      egressKey,
	}
}

func assertReconciledRuntime(
	t *testing.T,
	runtime *fakeXrayController,
	want map[string]string,
) {
	t.Helper()
	for accountingID, outbound := range want {
		if _, found := runtime.users[accountingID]; !found {
			t.Fatalf("runtime не содержит %q: %+v", accountingID, runtime.users)
		}
		rule, found := runtime.rules[accountingID]
		if !found || rule.OutboundTag != outbound {
			t.Fatalf("runtime rule %q = %+v, found=%t", accountingID, rule, found)
		}
	}
	for accountingID := range runtime.users {
		if xray.ValidateAccountingID(accountingID) == nil {
			if _, found := want[accountingID]; !found {
				t.Fatalf("runtime содержит лишнего backend-owned пользователя %q", accountingID)
			}
		}
	}
}

func assertManagedUserState(
	t *testing.T,
	store *state.Store,
	accountingID string,
	desiredPresent bool,
	applied bool,
) {
	t.Helper()
	user, err := store.ManagedUser(context.Background(), accountingID)
	if err != nil || user.DesiredPresent != desiredPresent || user.Applied != applied {
		t.Fatalf("managed user %q = %+v, error=%v", accountingID, user, err)
	}
}
