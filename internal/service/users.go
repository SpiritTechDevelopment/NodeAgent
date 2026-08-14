package service

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	nodeagentv1 "github.com/SpiritTechDevelopment/NodeAgent/internal/gen/spiritvpn/nodeagent/v1"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/state"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/xray"
)

const (
	invalidRequestMessage    = "request is invalid"
	operationConflictMessage = "operation ID was reused with a different request"
	stateFailureMessage      = "local state is unavailable"
	xrayFailureMessage       = "Xray state could not be applied"
	usageFailureMessage      = "usage could not be finalized"
)

// XrayUserController управляет runtime-пользователями Xray и их правилами.
type XrayUserController interface {
	AddUser(context.Context, xray.User) error
	RemoveUser(context.Context, string) error
	User(context.Context, string) (xray.User, error)
	Users(context.Context) ([]xray.User, error)
	AddUserRule(context.Context, string, string) error
	RemoveUserRule(context.Context, string) error
	UserRules(context.Context) ([]xray.UserRule, error)
	TestUserRoute(context.Context, string) (string, error)
}

// UsageFinalizer сохраняет и сбрасывает остаток трафика пользователей.
// Сервис вызывает её под блокировкой пользовательских мутаций, а реализация
// обязана сериализовать точечные и полные сбросы со своим периодическим сбором.
type UsageFinalizer interface {
	FinalizeUser(context.Context, string) error
	Flush(context.Context) error
}

// EnsureUserPresent приводит пользователя и его routing rule к точному состоянию запроса.
func (s *Service) EnsureUserPresent(
	ctx context.Context,
	request *nodeagentv1.EnsureUserPresentRequest,
) (*nodeagentv1.OperationResult, error) {
	operationID := ""
	if request != nil {
		operationID = request.GetOperationId()
	}
	digest := digestPresentRequest(request)

	if !s.lockEnsureMutation() {
		return nil, status.Error(codes.Unavailable, reconcileInProgressMessage)
	}
	defer s.mutations.Unlock()

	if result, found, err := s.replayOperation(
		ctx,
		operationID,
		state.OperationTypeEnsurePresent,
		digest,
	); err != nil {
		return retryableResult(operationID, stateFailureMessage), nil
	} else if found {
		return result, nil
	}

	desired, resolvedOutbound, err := s.presentUser(request)
	if err != nil {
		return s.completeInvalidOperation(
			ctx,
			operationID,
			state.OperationTypeEnsurePresent,
			digest,
		), nil
	}
	if err := s.ensurePendingOperation(
		ctx,
		operationID,
		state.OperationTypeEnsurePresent,
		digest,
	); err != nil {
		return retryableResult(operationID, stateFailureMessage), nil
	}

	observed, err := s.observeUser(ctx, desired.AccountingID)
	if err != nil {
		return retryableResult(operationID, xrayFailureMessage), nil
	}
	matches, err := s.presentMatches(ctx, observed, desired, resolvedOutbound)
	if err != nil {
		return retryableResult(operationID, xrayFailureMessage), nil
	}
	if matches {
		return s.completePresent(
			ctx,
			operationID,
			digest,
			desired,
			nodeagentv1.ApplyStatus_APPLY_STATUS_ALREADY_APPLIED,
		), nil
	}

	if observed.user != nil && observed.user.CredentialUUID != desired.CredentialUUID {
		if err := s.usage.FinalizeUser(ctx, desired.AccountingID); err != nil {
			return retryableResult(operationID, usageFailureMessage), nil
		}
	}
	if err := s.storePresentIntent(ctx, desired, false); err != nil {
		return retryableResult(operationID, stateFailureMessage), nil
	}
	if err := s.applyPresent(ctx, observed, desired, resolvedOutbound); err != nil {
		return retryableResult(operationID, xrayFailureMessage), nil
	}
	verified, err := s.observeUser(ctx, desired.AccountingID)
	if err != nil {
		return retryableResult(operationID, xrayFailureMessage), nil
	}
	matches, err = s.presentMatches(ctx, verified, desired, resolvedOutbound)
	if err != nil || !matches {
		return retryableResult(operationID, xrayFailureMessage), nil
	}

	return s.completePresent(
		ctx,
		operationID,
		digest,
		desired,
		nodeagentv1.ApplyStatus_APPLY_STATUS_APPLIED,
	), nil
}

// EnsureUserAbsent удаляет пользователя и его routing rule после финального сбора трафика.
func (s *Service) EnsureUserAbsent(
	ctx context.Context,
	request *nodeagentv1.EnsureUserAbsentRequest,
) (*nodeagentv1.OperationResult, error) {
	operationID := ""
	if request != nil {
		operationID = request.GetOperationId()
	}
	digest := digestAbsentRequest(request)

	if !s.lockEnsureMutation() {
		return nil, status.Error(codes.Unavailable, reconcileInProgressMessage)
	}
	defer s.mutations.Unlock()

	if result, found, err := s.replayOperation(
		ctx,
		operationID,
		state.OperationTypeEnsureAbsent,
		digest,
	); err != nil {
		return retryableResult(operationID, stateFailureMessage), nil
	} else if found {
		return result, nil
	}

	accountingID, err := absentAccountingID(request)
	if err != nil {
		return s.completeInvalidOperation(
			ctx,
			operationID,
			state.OperationTypeEnsureAbsent,
			digest,
		), nil
	}
	if err := s.ensurePendingOperation(
		ctx,
		operationID,
		state.OperationTypeEnsureAbsent,
		digest,
	); err != nil {
		return retryableResult(operationID, stateFailureMessage), nil
	}

	observed, err := s.observeUser(ctx, accountingID)
	if err != nil {
		return retryableResult(operationID, xrayFailureMessage), nil
	}
	tombstone, err := s.tombstone(ctx, accountingID)
	if err != nil {
		return retryableResult(operationID, stateFailureMessage), nil
	}
	if observed.user == nil && observed.rule == nil {
		return s.completeAbsent(
			ctx,
			operationID,
			digest,
			tombstone,
			nodeagentv1.ApplyStatus_APPLY_STATUS_ALREADY_APPLIED,
		), nil
	}

	if observed.user != nil {
		if err := s.usage.FinalizeUser(ctx, accountingID); err != nil {
			return retryableResult(operationID, usageFailureMessage), nil
		}
	}
	if err := s.storeAbsentIntent(ctx, tombstone, false); err != nil {
		return retryableResult(operationID, stateFailureMessage), nil
	}
	if observed.user != nil {
		if err := s.xray.RemoveUser(ctx, accountingID); err != nil {
			return retryableResult(operationID, xrayFailureMessage), nil
		}
	}
	if observed.rule != nil {
		if err := s.xray.RemoveUserRule(ctx, accountingID); err != nil {
			return retryableResult(operationID, xrayFailureMessage), nil
		}
	}
	verified, err := s.observeUser(ctx, accountingID)
	if err != nil || verified.user != nil || verified.rule != nil {
		return retryableResult(operationID, xrayFailureMessage), nil
	}

	return s.completeAbsent(
		ctx,
		operationID,
		digest,
		tombstone,
		nodeagentv1.ApplyStatus_APPLY_STATUS_APPLIED,
	), nil
}

type observedUser struct {
	user *xray.User
	rule *xray.UserRule
}

func (s *Service) presentUser(
	request *nodeagentv1.EnsureUserPresentRequest,
) (state.ManagedUser, string, error) {
	if request == nil || strings.TrimSpace(request.GetOperationId()) == "" ||
		request.GetOperationId() != strings.TrimSpace(request.GetOperationId()) ||
		request.GetUser() == nil {
		return state.ManagedUser{}, "", errors.New("invalid present request")
	}
	user := request.GetUser()
	xrayUser := xray.User{
		AccountingID:   user.GetAccountingId(),
		CredentialUUID: user.GetCredentialUuid(),
		Flow:           user.GetFlow(),
	}
	if err := xray.ValidateUser(xrayUser); err != nil {
		return state.ManagedUser{}, "", err
	}
	resolvedOutbound := user.GetEgressKey()
	if resolvedOutbound == "" {
		resolvedOutbound = s.localOutboundTag
	}
	if err := xray.ValidateOutboundTag(resolvedOutbound); err != nil {
		return state.ManagedUser{}, "", err
	}
	return state.ManagedUser{
		AccountingID:   xrayUser.AccountingID,
		CredentialUUID: xrayUser.CredentialUUID,
		Flow:           xrayUser.Flow,
		EgressKey:      user.GetEgressKey(),
		DesiredPresent: true,
		UpdatedAt:      s.now().UTC(),
	}, resolvedOutbound, nil
}

func absentAccountingID(request *nodeagentv1.EnsureUserAbsentRequest) (string, error) {
	if request == nil || strings.TrimSpace(request.GetOperationId()) == "" ||
		request.GetOperationId() != strings.TrimSpace(request.GetOperationId()) {
		return "", errors.New("invalid absent request")
	}
	if err := xray.ValidateAccountingID(request.GetAccountingId()); err != nil {
		return "", err
	}
	return request.GetAccountingId(), nil
}

func (s *Service) observeUser(ctx context.Context, accountingID string) (observedUser, error) {
	var observed observedUser
	user, err := s.xray.User(ctx, accountingID)
	switch {
	case err == nil:
		observed.user = &user
	case errors.Is(err, xray.ErrUserNotFound):
	default:
		return observedUser{}, err
	}

	rules, err := s.xray.UserRules(ctx)
	if err != nil {
		return observedUser{}, err
	}
	for index := range rules {
		if rules[index].AccountingID == accountingID {
			observed.rule = &rules[index]
			break
		}
	}
	return observed, nil
}

func (s *Service) presentMatches(
	ctx context.Context,
	observed observedUser,
	desired state.ManagedUser,
	resolvedOutbound string,
) (bool, error) {
	if observed.user == nil ||
		observed.user.CredentialUUID != desired.CredentialUUID ||
		observed.user.Flow != desired.Flow ||
		observed.rule == nil || observed.rule.OutboundTag != resolvedOutbound {
		return false, nil
	}
	outbound, err := s.xray.TestUserRoute(ctx, desired.AccountingID)
	if errors.Is(err, xray.ErrRouteNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return outbound == resolvedOutbound, nil
}

func (s *Service) applyPresent(
	ctx context.Context,
	observed observedUser,
	desired state.ManagedUser,
	resolvedOutbound string,
) error {
	if observed.user == nil ||
		observed.user.CredentialUUID != desired.CredentialUUID ||
		observed.user.Flow != desired.Flow {
		if observed.user != nil {
			if err := s.xray.RemoveUser(ctx, desired.AccountingID); err != nil {
				return err
			}
		}
		if err := s.xray.AddUser(ctx, xray.User{
			AccountingID:   desired.AccountingID,
			CredentialUUID: desired.CredentialUUID,
			Flow:           desired.Flow,
		}); err != nil {
			return err
		}
	}

	ruleMatches := observed.rule != nil && observed.rule.OutboundTag == resolvedOutbound
	if ruleMatches {
		outbound, err := s.xray.TestUserRoute(ctx, desired.AccountingID)
		ruleMatches = err == nil && outbound == resolvedOutbound
		if err != nil && !errors.Is(err, xray.ErrRouteNotFound) {
			return err
		}
	}
	if !ruleMatches {
		if observed.rule != nil {
			if err := s.xray.RemoveUserRule(ctx, desired.AccountingID); err != nil {
				return err
			}
		}
		if err := s.xray.AddUserRule(ctx, desired.AccountingID, resolvedOutbound); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) tombstone(ctx context.Context, accountingID string) (state.ManagedUser, error) {
	user, err := s.state.ManagedUser(ctx, accountingID)
	if errors.Is(err, state.ErrNotFound) {
		user = state.ManagedUser{AccountingID: accountingID}
	} else if err != nil {
		return state.ManagedUser{}, err
	}
	user.DesiredPresent = false
	user.Applied = false
	user.UpdatedAt = s.now().UTC()
	return user, nil
}

func (s *Service) storePresentIntent(
	ctx context.Context,
	user state.ManagedUser,
	applied bool,
) error {
	user.Applied = applied
	user.UpdatedAt = s.now().UTC()
	return s.state.PutManagedUser(ctx, user)
}

func (s *Service) storeAbsentIntent(
	ctx context.Context,
	user state.ManagedUser,
	applied bool,
) error {
	user.DesiredPresent = false
	user.Applied = applied
	user.UpdatedAt = s.now().UTC()
	return s.state.PutManagedUser(ctx, user)
}

func (s *Service) completePresent(
	ctx context.Context,
	operationID string,
	digest [sha256.Size]byte,
	user state.ManagedUser,
	applyStatus nodeagentv1.ApplyStatus,
) *nodeagentv1.OperationResult {
	result := operationResult(operationID, applyStatus, "")
	user.Applied = true
	user.UpdatedAt = s.now().UTC()
	if err := s.completeOperationWithUser(
		ctx,
		operationID,
		state.OperationTypeEnsurePresent,
		digest,
		user,
		result,
	); err != nil {
		return retryableResult(operationID, stateFailureMessage)
	}
	return result
}

func (s *Service) completeAbsent(
	ctx context.Context,
	operationID string,
	digest [sha256.Size]byte,
	user state.ManagedUser,
	applyStatus nodeagentv1.ApplyStatus,
) *nodeagentv1.OperationResult {
	result := operationResult(operationID, applyStatus, "")
	user.DesiredPresent = false
	user.Applied = true
	user.UpdatedAt = s.now().UTC()
	if err := s.completeOperationWithUser(
		ctx,
		operationID,
		state.OperationTypeEnsureAbsent,
		digest,
		user,
		result,
	); err != nil {
		return retryableResult(operationID, stateFailureMessage)
	}
	return result
}

func (s *Service) replayOperation(
	ctx context.Context,
	operationID string,
	operationType state.OperationType,
	digest [sha256.Size]byte,
) (*nodeagentv1.OperationResult, bool, error) {
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
	if operation.Type != operationType || operation.RequestDigest != digest {
		return operationResult(
			operationID,
			nodeagentv1.ApplyStatus_APPLY_STATUS_PERMANENT_ERROR,
			operationConflictMessage,
		), true, nil
	}
	if operation.Status == state.OperationStatusPending {
		return nil, false, nil
	}
	result := new(nodeagentv1.OperationResult)
	if err := proto.Unmarshal(operation.Result, result); err != nil {
		return nil, false, err
	}
	if result.GetOperationId() != operationID {
		return nil, false, errors.New("stored operation result has an unexpected operation ID")
	}
	return result, true, nil
}

func (s *Service) ensurePendingOperation(
	ctx context.Context,
	operationID string,
	operationType state.OperationType,
	digest [sha256.Size]byte,
) error {
	return s.state.Transaction(ctx, func(transaction *state.Transaction) error {
		err := transaction.CreateOperation(ctx, state.Operation{
			ID:            operationID,
			Type:          operationType,
			RequestDigest: digest,
			CreatedAt:     s.now().UTC(),
		})
		if errors.Is(err, state.ErrOperationExists) {
			return nil
		}
		return err
	})
}

func (s *Service) completeInvalidOperation(
	ctx context.Context,
	operationID string,
	operationType state.OperationType,
	digest [sha256.Size]byte,
) *nodeagentv1.OperationResult {
	result := operationResult(
		operationID,
		nodeagentv1.ApplyStatus_APPLY_STATUS_PERMANENT_ERROR,
		invalidRequestMessage,
	)
	if strings.TrimSpace(operationID) == "" || operationID != strings.TrimSpace(operationID) {
		return result
	}
	if err := s.ensurePendingOperation(ctx, operationID, operationType, digest); err != nil {
		return retryableResult(operationID, stateFailureMessage)
	}
	if err := s.completeOperation(ctx, operationID, result); err != nil {
		return retryableResult(operationID, stateFailureMessage)
	}
	return result
}

func (s *Service) completeOperationWithUser(
	ctx context.Context,
	operationID string,
	operationType state.OperationType,
	digest [sha256.Size]byte,
	user state.ManagedUser,
	result *nodeagentv1.OperationResult,
) error {
	payload, err := proto.Marshal(result)
	if err != nil {
		return err
	}
	return s.state.Transaction(ctx, func(transaction *state.Transaction) error {
		if err := transaction.PutManagedUser(ctx, user); err != nil {
			return err
		}
		operation, err := transaction.Operation(ctx, operationID)
		if errors.Is(err, state.ErrNotFound) {
			err = transaction.CreateOperation(ctx, state.Operation{
				ID:            operationID,
				Type:          operationType,
				RequestDigest: digest,
				CreatedAt:     s.now().UTC(),
			})
		}
		if err != nil {
			return err
		}
		if operation.Status == state.OperationStatusCompleted {
			return nil
		}
		return transaction.CompleteOperation(ctx, operationID, payload, s.now().UTC())
	})
}

func (s *Service) completeOperation(
	ctx context.Context,
	operationID string,
	result *nodeagentv1.OperationResult,
) error {
	payload, err := proto.Marshal(result)
	if err != nil {
		return err
	}
	return s.state.Transaction(ctx, func(transaction *state.Transaction) error {
		return transaction.CompleteOperation(ctx, operationID, payload, s.now().UTC())
	})
}

func operationResult(
	operationID string,
	applyStatus nodeagentv1.ApplyStatus,
	message string,
) *nodeagentv1.OperationResult {
	return &nodeagentv1.OperationResult{
		OperationId: operationID,
		Status:      applyStatus,
		Message:     message,
	}
}

func retryableResult(operationID, message string) *nodeagentv1.OperationResult {
	return operationResult(
		operationID,
		nodeagentv1.ApplyStatus_APPLY_STATUS_RETRYABLE_ERROR,
		message,
	)
}

func (s *Service) lockEnsureMutation() bool {
	if s.reconciling.Load() {
		return false
	}
	s.mutations.Lock()
	if s.reconciling.Load() {
		s.mutations.Unlock()
		return false
	}
	return true
}

func digestPresentRequest(request *nodeagentv1.EnsureUserPresentRequest) [sha256.Size]byte {
	digest := newDigest(state.OperationTypeEnsurePresent)
	if request == nil || request.GetUser() == nil {
		digest.field("nil")
		return digest.sum()
	}
	user := request.GetUser()
	digest.field("user")
	digest.field(user.GetAccountingId())
	digest.field(user.GetCredentialUuid())
	digest.field(user.GetFlow())
	digest.field(user.GetEgressKey())
	return digest.sum()
}

func digestAbsentRequest(request *nodeagentv1.EnsureUserAbsentRequest) [sha256.Size]byte {
	digest := newDigest(state.OperationTypeEnsureAbsent)
	if request == nil {
		digest.field("nil")
	} else {
		digest.field("request")
		digest.field(request.GetAccountingId())
	}
	return digest.sum()
}

type digestBuilder struct {
	hash hash.Hash
}

func newDigest(operationType state.OperationType) *digestBuilder {
	builder := &digestBuilder{hash: sha256.New()}
	builder.field(string(operationType))
	return builder
}

func (builder *digestBuilder) field(value string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = builder.hash.Write(size[:])
	_, _ = builder.hash.Write([]byte(value))
}

func (builder *digestBuilder) sum() [sha256.Size]byte {
	var result [sha256.Size]byte
	copy(result[:], builder.hash.Sum(nil))
	return result
}
