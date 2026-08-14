//go:build xray_smoke

package xray

import (
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const smokeImage = "ghcr.io/xtls/xray-core:26.3.27"

const smokeSecondAccountingID = "u.bcdefghijklmnopqrstu"

const smokeSecondCredentialUUID = "22222222-2222-4222-8222-222222222222"

func TestXrayAPISmoke(t *testing.T) {
	configPath, err := filepath.Abs("testdata/smoke-config.json")
	if err != nil {
		t.Fatalf("получить путь конфигурации: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	containerID := dockerOutput(
		t,
		ctx,
		"run",
		"--rm",
		"--detach",
		"--publish",
		"127.0.0.1::10085",
		"--volume",
		configPath+":/usr/local/etc/xray/config.json:ro",
		smokeImage,
		"-config",
		"/usr/local/etc/xray/config.json",
	)
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		_ = exec.CommandContext(stopCtx, "docker", "rm", "--force", containerID).Run()
	})

	address := dockerOutput(t, ctx, "port", containerID, "10085/tcp")
	client, err := New(Config{Address: address, InboundTag: "vless-in"})
	if err != nil {
		t.Fatalf("создать Xray client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	waitForXray(t, ctx, client)

	firstUser := User{
		AccountingID:   testAccountingID,
		CredentialUUID: testCredentialUUID,
		Flow:           flowVision,
	}
	secondUser := User{
		AccountingID:   smokeSecondAccountingID,
		CredentialUUID: smokeSecondCredentialUUID,
	}
	if err := client.AddUser(ctx, firstUser); err != nil {
		t.Fatalf("добавить первого VLESS-пользователя: %v", err)
	}
	if got := smokeUser(t, ctx, client, firstUser.AccountingID); got != firstUser {
		t.Fatalf("первый пользователь = %+v, ожидался %+v", got, firstUser)
	}
	if err := client.AddUser(ctx, secondUser); err != nil {
		t.Fatalf("добавить второго VLESS-пользователя: %v", err)
	}
	if got := smokeUser(t, ctx, client, secondUser.AccountingID); got != secondUser {
		t.Fatalf("второй пользователь = %+v, ожидался %+v", got, secondUser)
	}
	users, err := client.Users(ctx)
	if err != nil {
		t.Fatalf("прочитать пользователей: %v", err)
	}
	if len(users) != 2 || users[0] != firstUser || users[1] != secondUser {
		t.Fatalf("пользователи inbound = %+v, ожидались два добавленных пользователя", users)
	}
	if usage, err := client.ResetUsage(ctx); err != nil {
		t.Fatalf("выполнить bulk-сброс StatsService: %v", err)
	} else if len(usage) != 0 {
		t.Fatalf("неиспользованные пользователи получили трафик: %+v", usage)
	}
	if usage, err := client.ResetUserUsage(ctx, testAccountingID); err != nil {
		t.Fatalf("выполнить точечный сброс StatsService: %v", err)
	} else if len(usage) != 0 {
		t.Fatalf("неиспользованный пользователь получил трафик: %+v", usage)
	}

	assertSmokeNoRoute(t, ctx, client, testAccountingID)
	if err := client.AddUserRule(ctx, testAccountingID, "direct"); err != nil {
		t.Fatalf("добавить правило FREEDOM: %v", err)
	}
	if outbound := testSmokeRoute(t, ctx, client, testAccountingID); outbound != "direct" {
		t.Fatalf("outbound после добавления = %q, ожидался direct", outbound)
	}
	if err := client.AddUserRule(ctx, smokeSecondAccountingID, "bridge-test"); err != nil {
		t.Fatalf("добавить правило BRIDGE: %v", err)
	}
	if outbound := testSmokeRoute(t, ctx, client, testAccountingID); outbound != "direct" {
		t.Fatalf("первое правило после второго выбрало %q, ожидался direct", outbound)
	}
	if outbound := testSmokeRoute(t, ctx, client, smokeSecondAccountingID); outbound != "bridge-test" {
		t.Fatalf("BRIDGE outbound = %q, ожидался bridge-test", outbound)
	}

	rules, err := client.UserRules(ctx)
	if err != nil {
		t.Fatalf("прочитать правила: %v", err)
	}
	if len(rules) != 2 ||
		rules[0].AccountingID != testAccountingID ||
		rules[0].OutboundTag != "direct" ||
		rules[1].AccountingID != smokeSecondAccountingID ||
		rules[1].OutboundTag != "bridge-test" {
		t.Fatalf("персональные правила = %+v, ожидались правила direct и bridge-test", rules)
	}

	if err := client.RemoveUserRule(ctx, testAccountingID); err != nil {
		t.Fatalf("удалить правило: %v", err)
	}
	assertSmokeNoRoute(t, ctx, client, testAccountingID)
	if err := client.RemoveUser(ctx, testAccountingID); err != nil {
		t.Fatalf("удалить первого VLESS-пользователя: %v", err)
	}
	assertSmokeUserAbsent(t, ctx, client, testAccountingID)
	if outbound := testSmokeRoute(t, ctx, client, smokeSecondAccountingID); outbound != "bridge-test" {
		t.Fatalf("второе правило после удаления первого выбрало %q", outbound)
	}
	if err := client.RemoveUserRule(ctx, smokeSecondAccountingID); err != nil {
		t.Fatalf("удалить второе правило: %v", err)
	}
	assertSmokeNoRoute(t, ctx, client, smokeSecondAccountingID)
	if err := client.RemoveUser(ctx, smokeSecondAccountingID); err != nil {
		t.Fatalf("удалить второго VLESS-пользователя: %v", err)
	}
	assertSmokeUserAbsent(t, ctx, client, smokeSecondAccountingID)
}

func waitForXray(t *testing.T, ctx context.Context, client *Client) {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		callCtx, cancel := context.WithTimeout(ctx, time.Second)
		_, lastErr = client.Health(callCtx)
		cancel()
		if lastErr == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("Xray не стал доступен: %v", errors.Join(ctx.Err(), lastErr))
		case <-ticker.C:
		}
	}
}

func testSmokeRoute(
	t *testing.T,
	ctx context.Context,
	client *Client,
	accountingID string,
) string {
	t.Helper()
	outbound, err := client.TestUserRoute(ctx, accountingID)
	if err != nil {
		t.Fatalf("проверить маршрут: %v", err)
	}
	return outbound
}

func assertSmokeNoRoute(
	t *testing.T,
	ctx context.Context,
	client *Client,
	accountingID string,
) {
	t.Helper()
	_, err := client.TestUserRoute(ctx, accountingID)
	if !errors.Is(err, ErrRouteNotFound) {
		t.Fatalf("TestRoute без правила вернул %v, ожидалась ErrRouteNotFound", err)
	}
}

func smokeUser(t *testing.T, ctx context.Context, client *Client, accountingID string) User {
	t.Helper()
	user, err := client.User(ctx, accountingID)
	if err != nil {
		t.Fatalf("прочитать VLESS-пользователя: %v", err)
	}
	return user
}

func assertSmokeUserAbsent(
	t *testing.T,
	ctx context.Context,
	client *Client,
	accountingID string,
) {
	t.Helper()
	if _, err := client.User(ctx, accountingID); !errors.Is(err, ErrUserNotFound) {
		t.Fatalf("User() после удаления вернул %v, ожидалась ErrUserNotFound", err)
	}
}

func dockerOutput(t *testing.T, ctx context.Context, arguments ...string) string {
	t.Helper()
	output, err := exec.CommandContext(ctx, "docker", arguments...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v: %s", strings.Join(arguments, " "), err, output)
	}
	return strings.TrimSpace(string(output))
}
