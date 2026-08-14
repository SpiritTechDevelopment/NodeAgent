package service

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SpiritTechDevelopment/NodeAgent/internal/state"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/xray"
)

func TestRecoverIntentsCompletesPresentReplacement(t *testing.T) {
	service, store, runtime, usage := newUserTestService(t)
	if err := store.PutManagedUser(context.Background(), state.ManagedUser{
		AccountingID:   testAccountingID,
		CredentialUUID: testCredentialUUID,
		EgressKey:      "bridge-test",
		DesiredPresent: true,
		Applied:        false,
		UpdatedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("подготовить present intent: %v", err)
	}
	runtime.users[testAccountingID] = xray.User{
		AccountingID:   testAccountingID,
		CredentialUUID: secondCredential,
	}
	runtime.rules[testAccountingID] = xray.UserRule{
		AccountingID: testAccountingID,
		OutboundTag:  "old-egress",
	}

	if err := service.recoverIntents(context.Background()); err != nil {
		t.Fatalf("recoverIntents() вернул ошибку: %v", err)
	}
	assertRuntimeUser(t, runtime, testCredentialUUID, "bridge-test")
	assertManagedUserState(t, store, testAccountingID, true, true)
	if len(usage.calls) != 0 || usage.flushCalls != 0 {
		t.Fatalf("recovery повторно сбросил уже зафиксированный трафик: %+v", usage)
	}
}

func TestRecoverIntentsDeletesTombstoneOnlyAfterBootstrap(t *testing.T) {
	for _, test := range []struct {
		name        string
		initialized bool
		wantRemoved bool
	}{
		{name: "bootstrap required"},
		{name: "initialized", initialized: true, wantRemoved: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, store, runtime, usage := newUserTestService(t)
			if err := store.SetInitialized(context.Background(), test.initialized); err != nil {
				t.Fatalf("SetInitialized() вернул ошибку: %v", err)
			}
			if err := store.PutManagedUser(context.Background(), state.ManagedUser{
				AccountingID:   testAccountingID,
				CredentialUUID: testCredentialUUID,
				DesiredPresent: false,
				Applied:        false,
				UpdatedAt:      time.Now(),
			}); err != nil {
				t.Fatalf("подготовить absent intent: %v", err)
			}
			runtime.users[testAccountingID] = xray.User{
				AccountingID:   testAccountingID,
				CredentialUUID: testCredentialUUID,
			}
			runtime.rules[testAccountingID] = xray.UserRule{
				AccountingID: testAccountingID,
				OutboundTag:  "direct",
			}

			if err := service.recoverIntents(context.Background()); err != nil {
				t.Fatalf("recoverIntents() вернул ошибку: %v", err)
			}
			_, userPresent := runtime.users[testAccountingID]
			_, rulePresent := runtime.rules[testAccountingID]
			if userPresent == test.wantRemoved || rulePresent == test.wantRemoved {
				t.Fatalf("runtime после recovery: user_present=%t rule_present=%t", userPresent, rulePresent)
			}
			assertManagedUserState(t, store, testAccountingID, false, test.wantRemoved)
			if len(usage.calls) != 0 || usage.flushCalls != 0 {
				t.Fatalf("recovery tombstone повторно сбросил usage: %+v", usage)
			}
		})
	}
}

func TestAuditManagedUsersIsStrictlyAddOnly(t *testing.T) {
	service, store, runtime, _ := newUserTestService(t)
	if err := store.PutManagedUser(context.Background(), state.ManagedUser{
		AccountingID:   testAccountingID,
		CredentialUUID: testCredentialUUID,
		DesiredPresent: true,
		Applied:        true,
		UpdatedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("подготовить desired user: %v", err)
	}
	if err := store.PutManagedUser(context.Background(), state.ManagedUser{
		AccountingID:   extraAccountingID,
		CredentialUUID: extraCredential,
		DesiredPresent: false,
		Applied:        true,
		UpdatedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("подготовить tombstone: %v", err)
	}
	runtime.users[extraAccountingID] = xray.User{
		AccountingID:   extraAccountingID,
		CredentialUUID: extraCredential,
	}
	runtime.rules[extraAccountingID] = xray.UserRule{
		AccountingID: extraAccountingID,
		OutboundTag:  "direct",
	}
	runtime.users["svc-monitoring"] = xray.User{AccountingID: "svc-monitoring"}

	if err := service.auditManagedUsers(context.Background()); err != nil {
		t.Fatalf("auditManagedUsers() вернул ошибку: %v", err)
	}
	assertRuntimeUser(t, runtime, testCredentialUUID, "direct")
	if _, found := runtime.users[extraAccountingID]; !found {
		t.Fatal("add-only аудит удалил пользователя с tombstone")
	}
	if _, found := runtime.users["svc-monitoring"]; !found {
		t.Fatal("add-only аудит удалил инфраструктурного пользователя")
	}
	metadata, err := store.Metadata(context.Background())
	if err != nil || metadata.LastXrayAuditAt.IsZero() {
		t.Fatalf("last Xray audit = %v, error=%v", metadata.LastXrayAuditAt, err)
	}
}

func TestAuditManagedUsersDoesNotReplaceConflictingRuntime(t *testing.T) {
	service, store, runtime, _ := newUserTestService(t)
	if err := store.PutManagedUser(context.Background(), state.ManagedUser{
		AccountingID:   testAccountingID,
		CredentialUUID: testCredentialUUID,
		DesiredPresent: true,
		Applied:        true,
		UpdatedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("подготовить desired user: %v", err)
	}
	runtime.users[testAccountingID] = xray.User{
		AccountingID:   testAccountingID,
		CredentialUUID: secondCredential,
	}
	runtime.rules[testAccountingID] = xray.UserRule{
		AccountingID: testAccountingID,
		OutboundTag:  "unexpected-egress",
	}

	if err := service.auditManagedUsers(context.Background()); err == nil {
		t.Fatal("auditManagedUsers() не сообщил о конфликтующем runtime")
	}
	if runtime.mutationCount() != 0 {
		t.Fatalf("add-only аудит заменил конфликтующий runtime: %v", runtime.calls)
	}
	if runtime.users[testAccountingID].CredentialUUID != secondCredential {
		t.Fatal("конфликтующий credential был изменён")
	}
	metadata, err := store.Metadata(context.Background())
	if err != nil || !metadata.LastXrayAuditAt.IsZero() {
		t.Fatalf("неуспешный аудит записал время: %v, error=%v", metadata.LastXrayAuditAt, err)
	}
}

func TestXrayRestarted(t *testing.T) {
	tests := []struct {
		name     string
		previous XrayStatus
		current  XrayStatus
		want     bool
	}{
		{
			name:     "uptime reset",
			previous: XrayStatus{Reachable: true, Uptime: 10 * time.Minute},
			current:  XrayStatus{Reachable: true, Uptime: time.Second},
			want:     true,
		},
		{
			name:     "reachable transition",
			previous: XrayStatus{},
			current:  XrayStatus{Reachable: true, Uptime: time.Minute},
			want:     true,
		},
		{
			name:     "normal uptime growth",
			previous: XrayStatus{Reachable: true, Uptime: time.Minute},
			current:  XrayStatus{Reachable: true, Uptime: 2 * time.Minute},
		},
		{
			name:     "became unreachable",
			previous: XrayStatus{Reachable: true, Uptime: time.Minute},
			current:  XrayStatus{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := xrayRestarted(test.previous, test.current); got != test.want {
				t.Fatalf("xrayRestarted() = %t, ожидалось %t", got, test.want)
			}
		})
	}
}

func TestRunSelfHealAuditsImmediatelyAfterUptimeReset(t *testing.T) {
	provider := &atomicStatusProvider{}
	provider.reachable.Store(true)
	provider.uptimeNanos.Store(int64(10 * time.Minute))
	dependencies := newTestDependencies(t, provider)
	service, err := New(
		Config{NodeID: "node-test", AgentVersion: "test", LocalOutboundTag: "direct"},
		dependencies,
	)
	if err != nil {
		t.Fatalf("New() вернул ошибку: %v", err)
	}
	service.selfHealInterval = time.Hour
	service.xrayProbeInterval = 2 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	reports := make(chan error, 1)
	go func() {
		done <- service.RunSelfHeal(ctx, func(err error) { reports <- err })
	}()

	firstAudit := waitForAuditAfter(t, dependencies.State, time.Time{})
	if !service.selfHealRunning.Load() {
		t.Fatal("self-heal runner не перешёл в состояние running")
	}
	if err := service.RunSelfHeal(ctx, nil); !errors.Is(err, ErrSelfHealAlreadyRunning) {
		t.Fatalf("повторный RunSelfHeal() error = %v", err)
	}
	time.Sleep(3 * time.Millisecond)
	provider.uptimeNanos.Store(int64(time.Second))
	waitForAuditAfter(t, dependencies.State, firstAudit)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunSelfHeal() завершился с ошибкой: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunSelfHeal() не завершился после отмены контекста")
	}
	select {
	case err := <-reports:
		t.Fatalf("RunSelfHeal() сообщил неожиданную ошибку: %v", err)
	default:
	}
}

func TestRunSelfHealCountsReportedErrors(t *testing.T) {
	service := newTestService(t, stubStatusProvider{err: errors.New("status failed")})
	service.xrayProbeInterval = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- service.RunSelfHeal(ctx, func(error) { cancel() })
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunSelfHeal() вернул ошибку: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunSelfHeal() не завершился после ошибки и отмены")
	}
	if got := service.LocalReconcileErrors(); got != 1 {
		t.Fatalf("LocalReconcileErrors() = %d, ожидалось 1", got)
	}
}

type atomicStatusProvider struct {
	reachable   atomic.Bool
	uptimeNanos atomic.Int64
}

func (provider *atomicStatusProvider) Status(context.Context) (Status, error) {
	return Status{Xray: XrayStatus{
		Reachable: provider.reachable.Load(),
		Uptime:    time.Duration(provider.uptimeNanos.Load()),
	}}, nil
}

func waitForAuditAfter(t *testing.T, store *state.Store, after time.Time) time.Time {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		metadata, err := store.Metadata(context.Background())
		if err != nil {
			t.Fatalf("Metadata() вернул ошибку: %v", err)
		}
		if metadata.LastXrayAuditAt.After(after) {
			return metadata.LastXrayAuditAt
		}
		select {
		case <-deadline.C:
			t.Fatalf("аудит Xray не произошёл после %v", after)
		case <-ticker.C:
		}
	}
}
