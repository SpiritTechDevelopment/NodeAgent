package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/SpiritTechDevelopment/NodeAgent/internal/state"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/xray"
)

const (
	defaultSelfHealInterval  = 30 * time.Second
	defaultXrayProbeInterval = time.Second
)

var (
	// ErrSelfHealAlreadyRunning означает, что цикл локального восстановления уже запущен.
	ErrSelfHealAlreadyRunning = errors.New("local self-heal is already running")
)

// RunSelfHeal starts immediately, retries readiness failures with bounded
// backoff, audits periodically, and audits again after an observed Xray restart.
func (s *Service) RunSelfHeal(ctx context.Context, report func(error)) error {
	if !s.selfHealRunning.CompareAndSwap(false, true) {
		return ErrSelfHealAlreadyRunning
	}
	defer s.selfHealRunning.Store(false)
	if s.selfHealInterval <= 0 {
		return errors.New("self-heal interval must be positive")
	}
	if s.xrayProbeInterval <= 0 {
		return errors.New("Xray probe interval must be positive")
	}

	ticker := time.NewTicker(s.xrayProbeInterval)
	defer ticker.Stop()
	var (
		previousStatus XrayStatus
		statusObserved bool
		nextAttempt    time.Time
		retryDelay     = s.xrayProbeInterval
	)
	for {
		now := s.now().UTC()
		snapshot, statusErr := s.status.Status(ctx)
		restarted := statusErr == nil && statusObserved && xrayRestarted(previousStatus, snapshot.Xray)
		due := nextAttempt.IsZero() || !now.Before(nextAttempt)
		var err error
		if due || restarted {
			switch {
			case statusErr != nil:
				err = errors.Join(
					errors.New("node status is unavailable for local self-heal"),
					s.persistManagedConfigSafely(ctx),
				)
			case !snapshot.Xray.Reachable:
				err = errors.Join(
					errors.New("Xray is unavailable for local self-heal"),
					s.persistManagedConfigSafely(ctx),
				)
			default:
				err = s.recoverAndAudit(ctx)
			}
			if err == nil {
				retryDelay = s.xrayProbeInterval
				nextAttempt = now.Add(s.selfHealInterval)
			} else {
				nextAttempt = now.Add(retryDelay)
				retryDelay = min(retryDelay*2, s.selfHealInterval)
				s.recordUnavailableReconciliation(ctx)
			}
		}
		if statusErr != nil {
			previousStatus = XrayStatus{}
		} else {
			previousStatus = snapshot.Xray
		}
		statusObserved = true
		if err != nil && ctx.Err() == nil {
			s.localReconcileErrs.Add(1)
			if report != nil {
				report(err)
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func xrayRestarted(previous, current XrayStatus) bool {
	if !current.Reachable {
		return false
	}
	if !previous.Reachable {
		return true
	}
	return current.Uptime < previous.Uptime
}

func (s *Service) recoverAndAudit(ctx context.Context) error {
	recoveryErr := s.recoverIntents(ctx)
	auditErr := s.auditManagedUsers(ctx)
	if recoveryErr != nil || auditErr != nil {
		return errors.New("local self-heal did not converge")
	}
	return nil
}

func (s *Service) recoverIntents(ctx context.Context) error {
	s.mutations.Lock()
	defer s.mutations.Unlock()

	metadata, err := s.state.Metadata(ctx)
	if err != nil {
		return err
	}
	users, err := s.state.ManagedUsers(ctx)
	if err != nil {
		return err
	}
	pending := make([]state.ManagedUser, 0)
	for _, user := range users {
		if !user.Applied && (user.DesiredPresent || metadata.Initialized) {
			pending = append(pending, user)
		}
	}
	if len(pending) == 0 {
		return nil
	}

	runtime, err := s.observeReconcileRuntime(ctx)
	if err != nil {
		return err
	}
	eligible := make(map[string]state.ManagedUser, len(pending))
	hadFailure := false
	for _, user := range pending {
		if user.DesiredPresent {
			desired, err := s.desiredFromManagedUser(user)
			if err != nil {
				hadFailure = true
				continue
			}
			observed := runtimeUser(runtime, user.AccountingID)
			if err := s.applyPresent(
				ctx,
				observed,
				desired.user,
				desired.resolvedOutbound,
			); err != nil {
				hadFailure = true
				continue
			}
			eligible[user.AccountingID] = user
			continue
		}
		observed := runtimeUser(runtime, user.AccountingID)
		if observed.user != nil {
			if err := s.xray.RemoveUser(ctx, user.AccountingID); err != nil {
				hadFailure = true
				continue
			}
		}
		if observed.rule != nil {
			if err := s.xray.RemoveUserRule(ctx, user.AccountingID); err != nil {
				hadFailure = true
				continue
			}
		}
		eligible[user.AccountingID] = user
	}

	verified, err := s.observeReconcileRuntime(ctx)
	if err != nil {
		return err
	}
	completed := make(map[string]state.ManagedUser, len(eligible))
	for accountingID, user := range eligible {
		if user.DesiredPresent {
			desired, err := s.desiredFromManagedUser(user)
			if err != nil || !s.runtimeUserMatches(ctx, verified, desired) {
				hadFailure = true
				continue
			}
		} else {
			observed := runtimeUser(verified, accountingID)
			if observed.user != nil || observed.rule != nil {
				hadFailure = true
				continue
			}
		}
		user.Applied = true
		user.UpdatedAt = s.now().UTC()
		completed[accountingID] = user
	}
	if len(completed) > 0 {
		if err := s.storeRecoveredUsers(ctx, completed); err != nil {
			return err
		}
	}
	if hadFailure {
		return errors.New("one or more durable intents could not be recovered")
	}
	return nil
}

func (s *Service) auditManagedUsers(ctx context.Context) error {
	s.mutations.Lock()
	defer s.mutations.Unlock()
	if err := s.persistManagedConfig(ctx); err != nil {
		return err
	}

	users, err := s.state.ManagedUsers(ctx)
	if err != nil {
		return err
	}
	runtime, err := s.observeReconcileRuntime(ctx)
	if err != nil {
		return err
	}
	hadFailure := false
	desiredUsers := make([]reconcileDesiredUser, 0, len(users))
	for _, user := range users {
		if !user.DesiredPresent {
			continue
		}
		desired, err := s.desiredFromManagedUser(user)
		if err != nil {
			hadFailure = true
			continue
		}
		desiredUsers = append(desiredUsers, desired)
		observed := runtimeUser(runtime, user.AccountingID)
		matches, err := s.presentMatches(ctx, observed, user, desired.resolvedOutbound)
		if err != nil {
			hadFailure = true
			continue
		}
		if !matches {
			if err := s.applyPresent(ctx, observed, user, desired.resolvedOutbound); err != nil {
				hadFailure = true
				continue
			}
		}
	}
	verified, err := s.observeReconcileRuntime(ctx)
	if err != nil {
		return err
	}
	for _, desired := range desiredUsers {
		if !s.runtimeUserMatches(ctx, verified, desired) {
			hadFailure = true
		}
	}
	s.recordReconciliation(ctx, desiredUsers, verified, !hadFailure)
	if hadFailure {
		return errors.New("Xray audit did not converge")
	}
	observedAt := s.now().UTC()
	if err := s.state.SetLastXrayAuditAt(ctx, observedAt); err != nil {
		return err
	}
	status := s.Reconciliation()
	status.LastSuccessAt = observedAt
	s.reconciliation.Store(&status)
	return nil
}

func (s *Service) recordReconciliation(
	ctx context.Context,
	desired []reconcileDesiredUser,
	runtime reconcileRuntime,
	success bool,
) {
	status := ReconciliationStatus{DesiredUsers: uint64(len(desired))}
	previous := s.Reconciliation()
	status.LastSuccessAt = previous.LastSuccessAt
	for _, item := range desired {
		observed := runtimeUser(runtime, item.user.AccountingID)
		if observed.user != nil && observed.user.CredentialUUID == item.user.CredentialUUID &&
			observed.user.Flow == item.user.Flow {
			status.AppliedUsers++
		}
		if observed.rule != nil && observed.rule.OutboundTag == item.resolvedOutbound {
			outbound, err := s.xray.TestUserRoute(ctx, item.user.AccountingID)
			if err == nil && outbound == item.resolvedOutbound {
				status.AppliedRules++
			}
		}
	}
	status.Drift = status.DesiredUsers*2 - status.AppliedUsers - status.AppliedRules
	if success {
		status.Drift = 0
	}
	s.reconciliation.Store(&status)
}

func (s *Service) recordUnavailableReconciliation(ctx context.Context) {
	users, err := s.state.ManagedUsers(ctx)
	if err != nil {
		return
	}
	var desired uint64
	for _, user := range users {
		if user.DesiredPresent {
			desired++
		}
	}
	previous := s.Reconciliation()
	s.reconciliation.Store(&ReconciliationStatus{
		DesiredUsers:  desired,
		Drift:         desired * 2,
		LastSuccessAt: previous.LastSuccessAt,
	})
}

func (s *Service) desiredFromManagedUser(user state.ManagedUser) (reconcileDesiredUser, error) {
	if err := xray.ValidateUser(xray.User{
		AccountingID:   user.AccountingID,
		CredentialUUID: user.CredentialUUID,
		Flow:           user.Flow,
	}); err != nil {
		return reconcileDesiredUser{}, err
	}
	resolvedOutbound := user.EgressKey
	if resolvedOutbound == "" {
		resolvedOutbound = s.localOutboundTag
	}
	if err := xray.ValidateOutboundTag(resolvedOutbound); err != nil {
		return reconcileDesiredUser{}, err
	}
	return reconcileDesiredUser{user: user, resolvedOutbound: resolvedOutbound}, nil
}

func runtimeUser(runtime reconcileRuntime, accountingID string) observedUser {
	var observed observedUser
	if user, found := runtime.users[accountingID]; found {
		copy := user
		observed.user = &copy
	}
	if rule, found := runtime.rules[accountingID]; found {
		copy := rule
		observed.rule = &copy
	}
	return observed
}

func (s *Service) runtimeUserMatches(
	ctx context.Context,
	runtime reconcileRuntime,
	desired reconcileDesiredUser,
) bool {
	observed := runtimeUser(runtime, desired.user.AccountingID)
	if observed.user == nil || observed.user.CredentialUUID != desired.user.CredentialUUID ||
		observed.user.Flow != desired.user.Flow || observed.rule == nil ||
		observed.rule.OutboundTag != desired.resolvedOutbound {
		return false
	}
	outbound, err := s.xray.TestUserRoute(ctx, desired.user.AccountingID)
	return err == nil && outbound == desired.resolvedOutbound
}

func (s *Service) storeRecoveredUsers(
	ctx context.Context,
	users map[string]state.ManagedUser,
) error {
	return s.state.Transaction(ctx, func(transaction *state.Transaction) error {
		for _, accountingID := range sortedKeys(users) {
			if err := transaction.PutManagedUser(ctx, users[accountingID]); err != nil {
				return fmt.Errorf("store recovered managed user: %w", err)
			}
		}
		return nil
	})
}
