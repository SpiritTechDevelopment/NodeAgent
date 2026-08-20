package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	nodeagentv1 "github.com/SpiritTechDevelopment/NodeAgent/internal/gen/spiritvpn/nodeagent/v1"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/state"
	usagecollector "github.com/SpiritTechDevelopment/NodeAgent/internal/usage"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/xray"
)

const (
	testAccountingID   = "u.abcdefghijklmnopqrst"
	testCredentialUUID = "11111111-1111-4111-8111-111111111111"
)

func TestEnsureUserPresentAppliesPersistsAndReplays(t *testing.T) {
	service, store, runtime, _ := newUserTestService(t)
	request := presentRequest("operation-present", testCredentialUUID, "")

	result, err := service.EnsureUserPresent(context.Background(), request)
	if err != nil {
		t.Fatalf("EnsureUserPresent() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, result, nodeagentv1.ApplyStatus_APPLY_STATUS_APPLIED)
	assertRuntimeUser(t, runtime, testCredentialUUID, "direct")
	persisted := service.xrayConfig.(*fakeXrayConfigWriter).users
	if len(persisted) != 1 || persisted[0].User.AccountingID != testAccountingID ||
		persisted[0].OutboundTag != "direct" {
		t.Fatalf("persistent Xray config = %+v", persisted)
	}
	assertManagedUser(t, store, true, true, testCredentialUUID, "")
	assertCompletedOperation(t, store, request.GetOperationId())

	mutations := runtime.mutationCount()
	replayed, err := service.EnsureUserPresent(context.Background(), request)
	if err != nil {
		t.Fatalf("повторный EnsureUserPresent() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, replayed, nodeagentv1.ApplyStatus_APPLY_STATUS_APPLIED)
	if runtime.mutationCount() != mutations {
		t.Fatalf("повтор operation_id выполнил мутацию Xray: вызовы %v", runtime.calls)
	}

	conflicting := presentRequest(
		request.GetOperationId(),
		"22222222-2222-4222-8222-222222222222",
		"",
	)
	conflict, err := service.EnsureUserPresent(context.Background(), conflicting)
	if err != nil {
		t.Fatalf("конфликтный EnsureUserPresent() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, conflict, nodeagentv1.ApplyStatus_APPLY_STATUS_PERMANENT_ERROR)
	if conflict.GetMessage() != operationConflictMessage {
		t.Errorf("message конфликта = %q", conflict.GetMessage())
	}
	assertRuntimeUser(t, runtime, testCredentialUUID, "direct")
}

func TestEnsureUserPresentReturnsAlreadyAppliedForExactRuntime(t *testing.T) {
	service, store, runtime, usage := newUserTestService(t)
	runtime.users[testAccountingID] = xray.User{
		AccountingID:   testAccountingID,
		CredentialUUID: testCredentialUUID,
		Flow:           "xtls-rprx-vision",
	}
	runtime.rules[testAccountingID] = xray.UserRule{
		AccountingID: testAccountingID,
		OutboundTag:  "bridge-test",
	}
	request := presentRequest("operation-existing", testCredentialUUID, "bridge-test")
	request.User.Flow = "xtls-rprx-vision"

	result, err := service.EnsureUserPresent(context.Background(), request)
	if err != nil {
		t.Fatalf("EnsureUserPresent() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, result, nodeagentv1.ApplyStatus_APPLY_STATUS_ALREADY_APPLIED)
	if runtime.mutationCount() != 0 {
		t.Errorf("для точного runtime выполнены мутации: %v", runtime.calls)
	}
	if len(usage.calls) != 0 {
		t.Errorf("для точного runtime выполнен финальный сбор: %v", usage.calls)
	}
	assertManagedUser(t, store, true, true, testCredentialUUID, "bridge-test")
}

func TestEnsureUserPresentContinuesAfterPartialRoutingFailure(t *testing.T) {
	service, store, runtime, usage := newUserTestService(t)
	oldCredential := "22222222-2222-4222-8222-222222222222"
	runtime.users[testAccountingID] = xray.User{
		AccountingID:   testAccountingID,
		CredentialUUID: oldCredential,
	}
	runtime.rules[testAccountingID] = xray.UserRule{
		AccountingID: testAccountingID,
		OutboundTag:  "old-egress",
	}
	runtime.failMethod = "AddUserRule"
	runtime.failErr = errors.New("Xray rejected secret 11111111-1111-4111-8111-111111111111")
	request := presentRequest("operation-replace", testCredentialUUID, "bridge-test")

	first, err := service.EnsureUserPresent(context.Background(), request)
	if err != nil {
		t.Fatalf("первый EnsureUserPresent() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, first, nodeagentv1.ApplyStatus_APPLY_STATUS_RETRYABLE_ERROR)
	if strings.Contains(first.GetMessage(), testCredentialUUID) {
		t.Fatalf("retryable message раскрыл credential UUID: %q", first.GetMessage())
	}
	assertManagedUser(t, store, true, false, testCredentialUUID, "bridge-test")
	assertPendingOperation(t, store, request.GetOperationId())
	if !slices.Equal(usage.calls, []string{testAccountingID}) {
		t.Fatalf("финальные сборы = %v, ожидался один", usage.calls)
	}

	second, err := service.EnsureUserPresent(context.Background(), request)
	if err != nil {
		t.Fatalf("повторный EnsureUserPresent() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, second, nodeagentv1.ApplyStatus_APPLY_STATUS_APPLIED)
	assertRuntimeUser(t, runtime, testCredentialUUID, "bridge-test")
	assertManagedUser(t, store, true, true, testCredentialUUID, "bridge-test")
	assertCompletedOperation(t, store, request.GetOperationId())
	if !slices.Equal(usage.calls, []string{testAccountingID}) {
		t.Fatalf("повтор лишний раз сбросил трафик: %v", usage.calls)
	}
}

func TestEnsureUserPresentDoesNotMutateRuntimeWhenConfigCannotPersist(t *testing.T) {
	service, store, runtime, _ := newUserTestService(t)
	service.xrayConfig.(*fakeXrayConfigWriter).err = errors.New("config is read-only")
	result, err := service.EnsureUserPresent(
		context.Background(),
		presentRequest("operation-config-failure", testCredentialUUID, ""),
	)
	if err != nil {
		t.Fatalf("EnsureUserPresent() gRPC error = %v", err)
	}
	assertApplyStatus(t, result, nodeagentv1.ApplyStatus_APPLY_STATUS_RETRYABLE_ERROR)
	if runtime.mutationCount() != 0 {
		t.Fatalf("runtime changed before durable config: %v", runtime.calls)
	}
	assertManagedUser(t, store, true, false, testCredentialUUID, "")
}

func TestEnsureUserAbsentFinalizesBeforeRemovalAndReplays(t *testing.T) {
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
	if err := store.PutManagedUser(context.Background(), state.ManagedUser{
		AccountingID:   testAccountingID,
		CredentialUUID: testCredentialUUID,
		DesiredPresent: true,
		Applied:        true,
		UpdatedAt:      service.now().UTC(),
	}); err != nil {
		t.Fatalf("подготовить managed user: %v", err)
	}
	request := &nodeagentv1.EnsureUserAbsentRequest{
		OperationId:  "operation-absent",
		AccountingId: testAccountingID,
	}

	result, err := service.EnsureUserAbsent(context.Background(), request)
	if err != nil {
		t.Fatalf("EnsureUserAbsent() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, result, nodeagentv1.ApplyStatus_APPLY_STATUS_APPLIED)
	wantEvents := []string{"FinalizeUser", "RemoveUser", "RemoveUserRule"}
	if !slices.Equal(events, wantEvents) {
		t.Fatalf("порядок операций = %v, ожидался %v", events, wantEvents)
	}
	assertManagedUser(t, store, false, true, testCredentialUUID, "")
	if persisted := service.xrayConfig.(*fakeXrayConfigWriter).users; len(persisted) != 0 {
		t.Fatalf("removed user remains in persistent config: %+v", persisted)
	}
	assertCompletedOperation(t, store, request.GetOperationId())

	mutations := runtime.mutationCount()
	if _, err := service.EnsureUserAbsent(context.Background(), request); err != nil {
		t.Fatalf("повторный EnsureUserAbsent() вернул gRPC-ошибку: %v", err)
	}
	if runtime.mutationCount() != mutations || len(usage.calls) != 1 {
		t.Fatalf("повтор операции вызвал зависимости: Xray=%v usage=%v", runtime.calls, usage.calls)
	}
}

func TestEnsureUserAbsentPersistsFinalUsageWithCollector(t *testing.T) {
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
		UplinkBytes:   41,
		DownlinkBytes: 43,
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

	result, err := service.EnsureUserAbsent(context.Background(), &nodeagentv1.EnsureUserAbsentRequest{
		OperationId:  "operation-real-finalizer",
		AccountingId: testAccountingID,
	})
	if err != nil {
		t.Fatalf("EnsureUserAbsent() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, result, nodeagentv1.ApplyStatus_APPLY_STATUS_APPLIED)
	if source.accountingID != testAccountingID || source.calls != 1 {
		t.Fatalf("точечный сброс: accounting_id=%q calls=%d", source.accountingID, source.calls)
	}
	pending, err := dependencies.State.PendingUsageBatches(context.Background(), 10)
	if err != nil {
		t.Fatalf("PendingUsageBatches() вернул ошибку: %v", err)
	}
	if len(pending) != 1 || pending[0].Sequence != 1 {
		t.Fatalf("pending usage = %+v", pending)
	}
	payload := new(nodeagentv1.UsageBatch)
	if err := proto.Unmarshal(pending[0].Payload, payload); err != nil {
		t.Fatalf("декодировать usage payload: %v", err)
	}
	items := payload.GetItems()
	if len(items) != 1 || items[0].GetUplinkBytes() != 41 || items[0].GetDownlinkBytes() != 43 {
		t.Fatalf("usage items = %+v", items)
	}
}

func TestEnsureUserAbsentReturnsAlreadyApplied(t *testing.T) {
	service, store, runtime, usage := newUserTestService(t)
	request := &nodeagentv1.EnsureUserAbsentRequest{
		OperationId:  "operation-already-absent",
		AccountingId: testAccountingID,
	}

	result, err := service.EnsureUserAbsent(context.Background(), request)
	if err != nil {
		t.Fatalf("EnsureUserAbsent() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, result, nodeagentv1.ApplyStatus_APPLY_STATUS_ALREADY_APPLIED)
	if runtime.mutationCount() != 0 || len(usage.calls) != 0 {
		t.Fatalf("отсутствующий пользователь вызвал зависимости: Xray=%v usage=%v", runtime.calls, usage.calls)
	}
	assertManagedUser(t, store, false, true, "", "")
}

func TestEnsureMethodsPersistPermanentValidationResult(t *testing.T) {
	service, store, runtime, _ := newUserTestService(t)
	request := presentRequest("operation-invalid", "not-a-uuid", "")

	first, err := service.EnsureUserPresent(context.Background(), request)
	if err != nil {
		t.Fatalf("EnsureUserPresent() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, first, nodeagentv1.ApplyStatus_APPLY_STATUS_PERMANENT_ERROR)
	assertCompletedOperation(t, store, request.GetOperationId())
	if runtime.mutationCount() != 0 {
		t.Fatalf("невалидный запрос изменил Xray: %v", runtime.calls)
	}

	replayed, err := service.EnsureUserPresent(context.Background(), request)
	if err != nil {
		t.Fatalf("повторный EnsureUserPresent() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, replayed, nodeagentv1.ApplyStatus_APPLY_STATUS_PERMANENT_ERROR)

	validWithSameID := presentRequest(request.GetOperationId(), testCredentialUUID, "")
	conflict, err := service.EnsureUserPresent(context.Background(), validWithSameID)
	if err != nil {
		t.Fatalf("конфликтный EnsureUserPresent() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, conflict, nodeagentv1.ApplyStatus_APPLY_STATUS_PERMANENT_ERROR)
	if conflict.GetMessage() != operationConflictMessage {
		t.Errorf("message конфликта = %q", conflict.GetMessage())
	}
}

func TestEnsureMethodsRejectOperationIDReusedByAnotherMethod(t *testing.T) {
	service, _, runtime, _ := newUserTestService(t)
	operationID := "operation-cross-method"
	absent, err := service.EnsureUserAbsent(context.Background(), &nodeagentv1.EnsureUserAbsentRequest{
		OperationId:  operationID,
		AccountingId: testAccountingID,
	})
	if err != nil {
		t.Fatalf("EnsureUserAbsent() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, absent, nodeagentv1.ApplyStatus_APPLY_STATUS_ALREADY_APPLIED)

	present, err := service.EnsureUserPresent(
		context.Background(),
		presentRequest(operationID, testCredentialUUID, ""),
	)
	if err != nil {
		t.Fatalf("EnsureUserPresent() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, present, nodeagentv1.ApplyStatus_APPLY_STATUS_PERMANENT_ERROR)
	if present.GetMessage() != operationConflictMessage {
		t.Errorf("message конфликта = %q", present.GetMessage())
	}
	if runtime.mutationCount() != 0 {
		t.Fatalf("конфликт методов изменил Xray: %v", runtime.calls)
	}
}

func TestEnsureUserAbsentStopsWhenUsageFinalizationFails(t *testing.T) {
	service, store, runtime, usage := newUserTestService(t)
	runtime.users[testAccountingID] = xray.User{
		AccountingID:   testAccountingID,
		CredentialUUID: testCredentialUUID,
	}
	usage.err = errors.New("usage unavailable")
	request := &nodeagentv1.EnsureUserAbsentRequest{
		OperationId:  "operation-usage-error",
		AccountingId: testAccountingID,
	}

	result, err := service.EnsureUserAbsent(context.Background(), request)
	if err != nil {
		t.Fatalf("EnsureUserAbsent() вернул gRPC-ошибку: %v", err)
	}
	assertApplyStatus(t, result, nodeagentv1.ApplyStatus_APPLY_STATUS_RETRYABLE_ERROR)
	if runtime.mutationCount() != 0 {
		t.Fatalf("после ошибки сбора Xray изменён: %v", runtime.calls)
	}
	if _, err := store.ManagedUser(context.Background(), testAccountingID); !errors.Is(err, state.ErrNotFound) {
		t.Fatalf("tombstone записан до успешного сбора: %v", err)
	}
	assertPendingOperation(t, store, request.GetOperationId())
}

func presentRequest(operationID, credentialUUID, egressKey string) *nodeagentv1.EnsureUserPresentRequest {
	return &nodeagentv1.EnsureUserPresentRequest{
		OperationId: operationID,
		User: &nodeagentv1.User{
			AccountingId:   testAccountingID,
			CredentialUuid: credentialUUID,
			EgressKey:      egressKey,
		},
	}
}

func newUserTestService(t *testing.T) (*Service, *state.Store, *fakeXrayController, *fakeUsageFinalizer) {
	t.Helper()
	dependencies := newTestDependencies(t, stubStatusProvider{})
	service, err := New(
		Config{NodeID: "node-test", AgentVersion: "test", LocalOutboundTag: "direct"},
		dependencies,
	)
	if err != nil {
		t.Fatalf("New() вернул ошибку: %v", err)
	}
	return service,
		dependencies.State,
		dependencies.Xray.(*fakeXrayController),
		dependencies.Usage.(*fakeUsageFinalizer)
}

func newTestDependencies(t *testing.T, provider StatusProvider) Dependencies {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatalf("задать права тестового каталога: %v", err)
	}
	store, err := state.Open(context.Background(), filepath.Join(directory, "state.db"))
	if err != nil {
		t.Fatalf("открыть тестовую SQLite: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return Dependencies{
		Status:     provider,
		State:      store,
		XrayConfig: &fakeXrayConfigWriter{},
		Xray: &fakeXrayController{
			users:  make(map[string]xray.User),
			rules:  make(map[string]xray.UserRule),
			routes: make(map[string]string),
		},
		Usage: &fakeUsageFinalizer{},
	}
}

type fakeXrayConfigWriter struct {
	users []xray.PersistentUser
	err   error
}

func (writer *fakeXrayConfigWriter) Reconcile(_ context.Context, users []xray.PersistentUser) error {
	writer.users = slices.Clone(users)
	return writer.err
}

func assertApplyStatus(t *testing.T, result *nodeagentv1.OperationResult, want nodeagentv1.ApplyStatus) {
	t.Helper()
	if result.GetStatus() != want {
		t.Fatalf("apply status = %s, ожидался %s; message=%q", result.GetStatus(), want, result.GetMessage())
	}
}

func assertRuntimeUser(t *testing.T, runtime *fakeXrayController, credentialUUID, outbound string) {
	t.Helper()
	user, found := runtime.users[testAccountingID]
	if !found || user.CredentialUUID != credentialUUID {
		t.Fatalf("runtime user = %+v, found=%t", user, found)
	}
	rule, found := runtime.rules[testAccountingID]
	if !found || rule.OutboundTag != outbound {
		t.Fatalf("runtime rule = %+v, found=%t", rule, found)
	}
}

func assertManagedUser(
	t *testing.T,
	store *state.Store,
	desiredPresent bool,
	applied bool,
	credentialUUID string,
	egressKey string,
) {
	t.Helper()
	user, err := store.ManagedUser(context.Background(), testAccountingID)
	if err != nil {
		t.Fatalf("ManagedUser() вернул ошибку: %v", err)
	}
	if user.DesiredPresent != desiredPresent || user.Applied != applied ||
		user.CredentialUUID != credentialUUID || user.EgressKey != egressKey {
		t.Fatalf("managed user = %+v", user)
	}
}

func assertCompletedOperation(t *testing.T, store *state.Store, operationID string) {
	t.Helper()
	operation, err := store.Operation(context.Background(), operationID)
	if err != nil {
		t.Fatalf("Operation() вернул ошибку: %v", err)
	}
	if operation.Status != state.OperationStatusCompleted || len(operation.Result) == 0 {
		t.Fatalf("operation = %+v", operation)
	}
}

func assertPendingOperation(t *testing.T, store *state.Store, operationID string) {
	t.Helper()
	operation, err := store.Operation(context.Background(), operationID)
	if err != nil {
		t.Fatalf("Operation() вернул ошибку: %v", err)
	}
	if operation.Status != state.OperationStatusPending || len(operation.Result) != 0 {
		t.Fatalf("operation = %+v", operation)
	}
}

type fakeXrayController struct {
	users      map[string]xray.User
	rules      map[string]xray.UserRule
	routes     map[string]string
	calls      []string
	events     *[]string
	failMethod string
	failErr    error
}

func (controller *fakeXrayController) AddUser(_ context.Context, user xray.User) error {
	controller.record("AddUser", true)
	if err := controller.fail("AddUser"); err != nil {
		return err
	}
	controller.users[user.AccountingID] = user
	return nil
}

func (controller *fakeXrayController) RemoveUser(_ context.Context, accountingID string) error {
	controller.record("RemoveUser", true)
	if err := controller.fail("RemoveUser"); err != nil {
		return err
	}
	delete(controller.users, accountingID)
	return nil
}

func (controller *fakeXrayController) User(_ context.Context, accountingID string) (xray.User, error) {
	controller.record("User", false)
	if err := controller.fail("User"); err != nil {
		return xray.User{}, err
	}
	user, found := controller.users[accountingID]
	if !found {
		return xray.User{}, xray.ErrUserNotFound
	}
	return user, nil
}

func (controller *fakeXrayController) Users(context.Context) ([]xray.User, error) {
	controller.record("Users", false)
	if err := controller.fail("Users"); err != nil {
		return nil, err
	}
	users := make([]xray.User, 0, len(controller.users))
	for _, user := range controller.users {
		users = append(users, user)
	}
	slices.SortFunc(users, func(left, right xray.User) int {
		return strings.Compare(left.AccountingID, right.AccountingID)
	})
	return users, nil
}

func (controller *fakeXrayController) AddUserRule(
	_ context.Context,
	accountingID string,
	outbound string,
) error {
	controller.record("AddUserRule", true)
	if err := controller.fail("AddUserRule"); err != nil {
		return err
	}
	ruleTag, _ := xray.UserRuleTag(accountingID)
	controller.rules[accountingID] = xray.UserRule{
		AccountingID: accountingID,
		OutboundTag:  outbound,
		RuleTag:      ruleTag,
	}
	return nil
}

func (controller *fakeXrayController) RemoveUserRule(_ context.Context, accountingID string) error {
	controller.record("RemoveUserRule", true)
	if err := controller.fail("RemoveUserRule"); err != nil {
		return err
	}
	delete(controller.rules, accountingID)
	return nil
}

func (controller *fakeXrayController) UserRules(context.Context) ([]xray.UserRule, error) {
	controller.record("UserRules", false)
	if err := controller.fail("UserRules"); err != nil {
		return nil, err
	}
	rules := make([]xray.UserRule, 0, len(controller.rules))
	for _, rule := range controller.rules {
		rules = append(rules, rule)
	}
	return rules, nil
}

func (controller *fakeXrayController) TestUserRoute(
	_ context.Context,
	accountingID string,
) (string, error) {
	controller.record("TestUserRoute", false)
	if err := controller.fail("TestUserRoute"); err != nil {
		return "", err
	}
	if outbound, overridden := controller.routes[accountingID]; overridden {
		return outbound, nil
	}
	rule, found := controller.rules[accountingID]
	if !found {
		return "", xray.ErrRouteNotFound
	}
	return rule.OutboundTag, nil
}

func (controller *fakeXrayController) record(method string, mutation bool) {
	kind := "read:"
	if mutation {
		kind = "mutation:"
	}
	controller.calls = append(controller.calls, kind+method)
	if mutation && controller.events != nil {
		*controller.events = append(*controller.events, method)
	}
}

func (controller *fakeXrayController) fail(method string) error {
	if controller.failMethod != method {
		return nil
	}
	controller.failMethod = ""
	return controller.failErr
}

func (controller *fakeXrayController) mutationCount() int {
	count := 0
	for _, call := range controller.calls {
		if strings.HasPrefix(call, "mutation:") {
			count++
		}
	}
	return count
}

type fakeUsageFinalizer struct {
	calls      []string
	flushCalls int
	events     *[]string
	err        error
}

func (finalizer *fakeUsageFinalizer) FinalizeUser(_ context.Context, accountingID string) error {
	finalizer.calls = append(finalizer.calls, accountingID)
	if finalizer.events != nil {
		*finalizer.events = append(*finalizer.events, "FinalizeUser")
	}
	return finalizer.err
}

func (finalizer *fakeUsageFinalizer) Flush(context.Context) error {
	finalizer.flushCalls++
	if finalizer.events != nil {
		*finalizer.events = append(*finalizer.events, "Flush")
	}
	return finalizer.err
}

type serviceUsageSource struct {
	items        []xray.Usage
	accountingID string
	calls        int
	bulkCalls    int
}

func (source *serviceUsageSource) ResetUsage(context.Context) ([]xray.Usage, error) {
	source.bulkCalls++
	return append([]xray.Usage(nil), source.items...), nil
}

func (source *serviceUsageSource) ResetUserUsage(
	_ context.Context,
	accountingID string,
) ([]xray.Usage, error) {
	source.accountingID = accountingID
	source.calls++
	return append([]xray.Usage(nil), source.items...), nil
}
