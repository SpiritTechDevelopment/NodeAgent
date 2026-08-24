package service

import (
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	nodeagentv1 "github.com/SpiritTechDevelopment/NodeAgent/internal/gen/spiritvpn/nodeagent/v1"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/state"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/xray"
)

const reconcileInProgressMessage = "authoritative reconciliation is in progress"

type reconcileDesiredUser struct {
	user             state.ManagedUser
	resolvedOutbound string
}

type reconcileRuntime struct {
	users map[string]xray.User
	rules map[string]xray.UserRule
}

type reconcileAction uint8

const (
	reconcileUnchanged reconcileAction = iota
	reconcileAdded
	reconcileReplaced
)

type reconcilePlan struct {
	desired  reconcileDesiredUser
	observed observedUser
	action   reconcileAction
}

type reconcileSummary struct {
	added     uint32
	replaced  uint32
	removed   uint32
	unchanged uint32
}

// ReconcileUsers приводит весь backend-owned набор пользователей к авторитетному запросу.
func (s *Service) ReconcileUsers(
	ctx context.Context,
	request *nodeagentv1.ReconcileUsersRequest,
) (*nodeagentv1.ReconcileUsersResponse, error) {
	if !s.reconciling.CompareAndSwap(false, true) {
		return nil, status.Error(codes.Unavailable, reconcileInProgressMessage)
	}
	defer s.reconciling.Store(false)

	s.mutations.Lock()
	defer s.mutations.Unlock()

	operationID := ""
	if request != nil {
		operationID = request.GetOperationId()
	}
	digest := digestReconcileRequest(request)
	if response, found, err := s.replayReconcile(ctx, operationID, digest); err != nil {
		return retryableReconcile(operationID, stateFailureMessage), nil
	} else if found {
		return response, nil
	}

	desired, err := s.validateReconcileRequest(request)
	if err != nil {
		return s.completeInvalidReconcile(ctx, operationID, digest), nil
	}
	if err := s.ensurePendingOperation(
		ctx,
		operationID,
		state.OperationTypeReconcile,
		digest,
	); err != nil {
		return retryableReconcile(operationID, stateFailureMessage), nil
	}

	runtime, err := s.observeReconcileRuntime(ctx)
	if err != nil {
		return retryableReconcile(operationID, xrayFailureMessage), nil
	}
	managedUsers, err := s.state.ManagedUsers(ctx)
	if err != nil {
		return retryableReconcile(operationID, stateFailureMessage), nil
	}

	plans, removals, intents, summary := s.planReconcile(desired, runtime, managedUsers)
	if summary.replaced > 0 || summary.removed > 0 {
		// Reconcile авторитетен, поэтому ошибка best-effort сброса не блокирует исправление доступа.
		_ = s.usage.Flush(ctx)
	}
	if err := s.storeReconcileIntents(ctx, intents); err != nil {
		return retryableReconcile(operationID, stateFailureMessage), nil
	}
	if err := s.applyManagedConfig(ctx); err != nil {
		return retryableReconcile(operationID, xrayFailureMessage), nil
	}

	for index := range plans {
		if plans[index].action == reconcileUnchanged {
			continue
		}
		if err := s.applyReconcilePresent(ctx, plans[index]); err != nil {
			return retryableReconcile(operationID, xrayFailureMessage), nil
		}
	}
	for index := range plans {
		outbound, err := s.xray.TestUserRoute(ctx, plans[index].desired.user.AccountingID)
		if err == nil && outbound == plans[index].desired.resolvedOutbound {
			continue
		}
		if err != nil && !errors.Is(err, xray.ErrRouteNotFound) {
			return retryableReconcile(operationID, xrayFailureMessage), nil
		}
		if plans[index].action != reconcileUnchanged {
			return retryableReconcile(operationID, xrayFailureMessage), nil
		}
		// Точечно исправить одно правило нельзя: переустанавливаем таблицу целиком.
		if err := s.reinstallRouting(ctx); err != nil {
			return retryableReconcile(operationID, xrayFailureMessage), nil
		}
		outbound, err = s.xray.TestUserRoute(ctx, plans[index].desired.user.AccountingID)
		if err != nil || outbound != plans[index].desired.resolvedOutbound {
			return retryableReconcile(operationID, xrayFailureMessage), nil
		}
		plans[index].action = reconcileReplaced
		summary.unchanged--
		summary.replaced++
	}

	for _, accountingID := range sortedKeys(removals) {
		observed := removals[accountingID]
		// Правила снятых пользователей уже убраны applyManagedConfig.
		if observed.user != nil {
			if err := s.xray.RemoveUser(ctx, accountingID); err != nil {
				return retryableReconcile(operationID, xrayFailureMessage), nil
			}
		}
	}

	verified, err := s.observeReconcileRuntime(ctx)
	if err != nil || !reconcileRuntimeMatches(verified, desired) {
		return retryableReconcile(operationID, xrayFailureMessage), nil
	}
	response := successfulReconcile(operationID, summary)
	if err := s.completeReconcile(ctx, operationID, intents, response); err != nil {
		return retryableReconcile(operationID, stateFailureMessage), nil
	}
	return response, nil
}

func (s *Service) validateReconcileRequest(
	request *nodeagentv1.ReconcileUsersRequest,
) ([]reconcileDesiredUser, error) {
	if request == nil || strings.TrimSpace(request.GetOperationId()) == "" ||
		request.GetOperationId() != strings.TrimSpace(request.GetOperationId()) ||
		!request.GetComplete() || len(request.GetUsers()) > s.maxInventoryUsers {
		return nil, errors.New("invalid reconcile request")
	}
	desired := make([]reconcileDesiredUser, 0, len(request.GetUsers()))
	seen := make(map[string]struct{}, len(request.GetUsers()))
	for _, item := range request.GetUsers() {
		if item == nil {
			return nil, errors.New("reconcile user is required")
		}
		xrayUser := xray.User{
			AccountingID:   item.GetAccountingId(),
			CredentialUUID: item.GetCredentialUuid(),
			Flow:           item.GetFlow(),
		}
		if err := xray.ValidateUser(xrayUser); err != nil {
			return nil, err
		}
		if _, duplicate := seen[xrayUser.AccountingID]; duplicate {
			return nil, errors.New("duplicate reconcile accounting ID")
		}
		seen[xrayUser.AccountingID] = struct{}{}
		resolvedOutbound := item.GetEgressKey()
		if resolvedOutbound == "" {
			resolvedOutbound = s.localOutboundTag
		}
		if err := xray.ValidateOutboundTag(resolvedOutbound); err != nil {
			return nil, err
		}
		desired = append(desired, reconcileDesiredUser{
			user: state.ManagedUser{
				AccountingID:   xrayUser.AccountingID,
				CredentialUUID: xrayUser.CredentialUUID,
				Flow:           xrayUser.Flow,
				EgressKey:      item.GetEgressKey(),
				DesiredPresent: true,
			},
			resolvedOutbound: resolvedOutbound,
		})
	}
	slices.SortFunc(desired, func(left, right reconcileDesiredUser) int {
		return strings.Compare(left.user.AccountingID, right.user.AccountingID)
	})
	return desired, nil
}

func (s *Service) observeReconcileRuntime(ctx context.Context) (reconcileRuntime, error) {
	users, err := s.xray.Users(ctx)
	if err != nil {
		return reconcileRuntime{}, err
	}
	rules, err := s.xray.UserRules(ctx)
	if err != nil {
		return reconcileRuntime{}, err
	}
	runtime := reconcileRuntime{
		users: make(map[string]xray.User, len(users)),
		rules: make(map[string]xray.UserRule, len(rules)),
	}
	for _, user := range users {
		if _, duplicate := runtime.users[user.AccountingID]; duplicate {
			return reconcileRuntime{}, errors.New("duplicate Xray user")
		}
		runtime.users[user.AccountingID] = user
	}
	for _, rule := range rules {
		if _, duplicate := runtime.rules[rule.AccountingID]; duplicate {
			return reconcileRuntime{}, errors.New("duplicate Xray user rule")
		}
		runtime.rules[rule.AccountingID] = rule
	}
	return runtime, nil
}

func (s *Service) planReconcile(
	desired []reconcileDesiredUser,
	runtime reconcileRuntime,
	managedUsers []state.ManagedUser,
) ([]reconcilePlan, map[string]observedUser, map[string]state.ManagedUser, reconcileSummary) {
	desiredIDs := make(map[string]struct{}, len(desired))
	plans := make([]reconcilePlan, 0, len(desired))
	intents := make(map[string]state.ManagedUser, len(desired)+len(managedUsers))
	var summary reconcileSummary
	updatedAt := s.now().UTC()
	for _, item := range desired {
		desiredIDs[item.user.AccountingID] = struct{}{}
		item.user.Applied = false
		item.user.UpdatedAt = updatedAt
		intents[item.user.AccountingID] = item.user
		plan := reconcilePlan{desired: item}
		if user, found := runtime.users[item.user.AccountingID]; found {
			plan.observed.user = &user
		}
		if rule, found := runtime.rules[item.user.AccountingID]; found {
			plan.observed.rule = &rule
		}
		switch {
		case plan.observed.user == nil:
			plan.action = reconcileAdded
			summary.added++
		case plan.observed.user.CredentialUUID != item.user.CredentialUUID,
			plan.observed.user.Flow != item.user.Flow,
			plan.observed.rule == nil,
			plan.observed.rule.OutboundTag != item.resolvedOutbound:
			plan.action = reconcileReplaced
			summary.replaced++
		default:
			plan.action = reconcileUnchanged
			summary.unchanged++
		}
		plans = append(plans, plan)
	}

	removals := make(map[string]observedUser)
	for accountingID, user := range runtime.users {
		if xray.ValidateAccountingID(accountingID) != nil {
			continue
		}
		if _, wanted := desiredIDs[accountingID]; wanted {
			continue
		}
		copy := user
		observed := removals[accountingID]
		observed.user = &copy
		removals[accountingID] = observed
		summary.removed++
	}
	for accountingID, rule := range runtime.rules {
		if _, wanted := desiredIDs[accountingID]; wanted {
			continue
		}
		copy := rule
		observed := removals[accountingID]
		observed.rule = &copy
		removals[accountingID] = observed
	}
	for _, user := range managedUsers {
		if xray.ValidateAccountingID(user.AccountingID) != nil {
			continue
		}
		if _, wanted := desiredIDs[user.AccountingID]; wanted {
			continue
		}
		user.DesiredPresent = false
		user.Applied = false
		user.UpdatedAt = updatedAt
		intents[user.AccountingID] = user
	}
	for accountingID, observed := range removals {
		if _, exists := intents[accountingID]; exists {
			continue
		}
		user := state.ManagedUser{AccountingID: accountingID, UpdatedAt: updatedAt}
		if observed.user != nil {
			user.CredentialUUID = observed.user.CredentialUUID
			user.Flow = observed.user.Flow
		}
		intents[accountingID] = user
	}
	return plans, removals, intents, summary
}

func (s *Service) applyReconcilePresent(ctx context.Context, plan reconcilePlan) error {
	desired := plan.desired
	if plan.observed.user == nil ||
		plan.observed.user.CredentialUUID != desired.user.CredentialUUID ||
		plan.observed.user.Flow != desired.user.Flow {
		if plan.observed.user != nil {
			if err := s.xray.RemoveUser(ctx, desired.user.AccountingID); err != nil {
				return err
			}
		}
		if err := s.xray.AddUser(ctx, xray.User{
			AccountingID:   desired.user.AccountingID,
			CredentialUUID: desired.user.CredentialUUID,
			Flow:           desired.user.Flow,
		}); err != nil {
			return err
		}
	}
	// Правила маршрутизации уже установлены applyManagedConfig целой таблицей.
	return nil
}

func reconcileRuntimeMatches(runtime reconcileRuntime, desired []reconcileDesiredUser) bool {
	desiredByID := make(map[string]reconcileDesiredUser, len(desired))
	for _, item := range desired {
		desiredByID[item.user.AccountingID] = item
		user, found := runtime.users[item.user.AccountingID]
		if !found || user.CredentialUUID != item.user.CredentialUUID || user.Flow != item.user.Flow {
			return false
		}
		rule, found := runtime.rules[item.user.AccountingID]
		if !found || rule.OutboundTag != item.resolvedOutbound {
			return false
		}
	}
	for accountingID := range runtime.users {
		if xray.ValidateAccountingID(accountingID) == nil {
			if _, wanted := desiredByID[accountingID]; !wanted {
				return false
			}
		}
	}
	for accountingID := range runtime.rules {
		if _, wanted := desiredByID[accountingID]; !wanted {
			return false
		}
	}
	return true
}

func (s *Service) storeReconcileIntents(
	ctx context.Context,
	intents map[string]state.ManagedUser,
) error {
	return s.state.Transaction(ctx, func(transaction *state.Transaction) error {
		for _, accountingID := range sortedKeys(intents) {
			if err := transaction.PutManagedUser(ctx, intents[accountingID]); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Service) completeReconcile(
	ctx context.Context,
	operationID string,
	intents map[string]state.ManagedUser,
	response *nodeagentv1.ReconcileUsersResponse,
) error {
	payload, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	completedAt := s.now().UTC()
	return s.state.Transaction(ctx, func(transaction *state.Transaction) error {
		for _, accountingID := range sortedKeys(intents) {
			user := intents[accountingID]
			user.Applied = true
			user.UpdatedAt = completedAt
			if err := transaction.PutManagedUser(ctx, user); err != nil {
				return err
			}
		}
		if err := transaction.SetInitialized(ctx, true); err != nil {
			return err
		}
		return transaction.CompleteOperation(ctx, operationID, payload, completedAt)
	})
}

func (s *Service) replayReconcile(
	ctx context.Context,
	operationID string,
	digest [sha256.Size]byte,
) (*nodeagentv1.ReconcileUsersResponse, bool, error) {
	if strings.TrimSpace(operationID) == "" || operationID != strings.TrimSpace(operationID) {
		return nil, false, nil
	}
	operation, err := s.state.Operation(ctx, operationID)
	if errors.Is(err, state.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if operation.Type != state.OperationTypeReconcile || operation.RequestDigest != digest {
		return permanentReconcile(operationID, operationConflictMessage), true, nil
	}
	if operation.Status == state.OperationStatusPending {
		return nil, false, nil
	}
	response := new(nodeagentv1.ReconcileUsersResponse)
	if err := proto.Unmarshal(operation.Result, response); err != nil {
		return nil, false, err
	}
	if response.GetOperation().GetOperationId() != operationID {
		return nil, false, errors.New("stored reconcile result has an unexpected operation ID")
	}
	return response, true, nil
}

func (s *Service) completeInvalidReconcile(
	ctx context.Context,
	operationID string,
	digest [sha256.Size]byte,
) *nodeagentv1.ReconcileUsersResponse {
	response := permanentReconcile(operationID, invalidRequestMessage)
	if strings.TrimSpace(operationID) == "" || operationID != strings.TrimSpace(operationID) {
		return response
	}
	if err := s.ensurePendingOperation(
		ctx,
		operationID,
		state.OperationTypeReconcile,
		digest,
	); err != nil {
		return retryableReconcile(operationID, stateFailureMessage)
	}
	payload, err := proto.Marshal(response)
	if err != nil {
		return retryableReconcile(operationID, stateFailureMessage)
	}
	if err := s.state.Transaction(ctx, func(transaction *state.Transaction) error {
		return transaction.CompleteOperation(ctx, operationID, payload, s.now().UTC())
	}); err != nil {
		return retryableReconcile(operationID, stateFailureMessage)
	}
	return response
}

func successfulReconcile(
	operationID string,
	summary reconcileSummary,
) *nodeagentv1.ReconcileUsersResponse {
	applyStatus := nodeagentv1.ApplyStatus_APPLY_STATUS_ALREADY_APPLIED
	if summary.added > 0 || summary.replaced > 0 || summary.removed > 0 {
		applyStatus = nodeagentv1.ApplyStatus_APPLY_STATUS_APPLIED
	}
	return &nodeagentv1.ReconcileUsersResponse{
		Operation: operationResult(operationID, applyStatus, ""),
		Added:     summary.added,
		Replaced:  summary.replaced,
		Removed:   summary.removed,
		Unchanged: summary.unchanged,
	}
}

func retryableReconcile(operationID, message string) *nodeagentv1.ReconcileUsersResponse {
	return &nodeagentv1.ReconcileUsersResponse{Operation: retryableResult(operationID, message)}
}

func permanentReconcile(operationID, message string) *nodeagentv1.ReconcileUsersResponse {
	return &nodeagentv1.ReconcileUsersResponse{Operation: operationResult(
		operationID,
		nodeagentv1.ApplyStatus_APPLY_STATUS_PERMANENT_ERROR,
		message,
	)}
}

func digestReconcileRequest(request *nodeagentv1.ReconcileUsersRequest) [sha256.Size]byte {
	digest := newDigest(state.OperationTypeReconcile)
	if request == nil {
		digest.field("nil")
		return digest.sum()
	}
	digest.field("request")
	if request.GetComplete() {
		digest.field("complete")
	} else {
		digest.field("incomplete")
	}
	type digestUser struct {
		nilUser        bool
		accountingID   string
		credentialUUID string
		flow           string
		egressKey      string
	}
	users := make([]digestUser, 0, len(request.GetUsers()))
	for _, item := range request.GetUsers() {
		if item == nil {
			users = append(users, digestUser{nilUser: true})
			continue
		}
		users = append(users, digestUser{
			accountingID:   item.GetAccountingId(),
			credentialUUID: item.GetCredentialUuid(),
			flow:           item.GetFlow(),
			egressKey:      item.GetEgressKey(),
		})
	}
	slices.SortFunc(users, func(left, right digestUser) int {
		leftKey := []string{left.accountingID, left.credentialUUID, left.flow, left.egressKey}
		rightKey := []string{right.accountingID, right.credentialUUID, right.flow, right.egressKey}
		if left.nilUser != right.nilUser {
			if left.nilUser {
				return -1
			}
			return 1
		}
		for index := range leftKey {
			if comparison := strings.Compare(leftKey[index], rightKey[index]); comparison != 0 {
				return comparison
			}
		}
		return 0
	})
	for _, user := range users {
		if user.nilUser {
			digest.field("nil-user")
			continue
		}
		digest.field("user")
		digest.field(user.accountingID)
		digest.field(user.credentialUUID)
		digest.field(user.flow)
		digest.field(user.egressKey)
	}
	return digest.sum()
}

func sortedKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
