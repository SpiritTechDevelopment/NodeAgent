//go:build xray_smoke

package xray

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	routerCommand "github.com/xtls/xray-core/app/router/command"
)

const smokeImage = "ghcr.io/xtls/xray-core:26.3.27"

const smokeSecondAccountingID = "u.bcdefghijklmnopqrstu"

const smokeSecondCredentialUUID = "22222222-2222-4222-8222-222222222222"

func TestXrayAPISmoke(t *testing.T) {
	configPath := durableSmokeConfig(t, nil)

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

	file, err := NewConfigFile(configPath, "vless-in", "block")
	if err != nil {
		t.Fatalf("открыть конфигурацию: %v", err)
	}
	applySmokeRouting(t, ctx, client, file, []PersistentUser{
		{User: firstUser, OutboundTag: "direct"},
		{User: secondUser, OutboundTag: "bridge-test"},
	})

	// Регрессия: раньше правило слалось по одному с ShouldAppend=false, из-за чего
	// установка второго стирала первое, правило api и default-deny.
	if outbound := testSmokeRoute(t, ctx, client, testAccountingID); outbound != "direct" {
		t.Fatalf("первый пользователь выбрал %q, ожидался direct", outbound)
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
	assertSmokeForeignRulesSurvive(t, ctx, client, len(rules))

	// Снятие пользователя — тоже переустановка таблицы, уже без него.
	applySmokeRouting(t, ctx, client, file, []PersistentUser{
		{User: secondUser, OutboundTag: "bridge-test"},
	})
	assertSmokeNoRoute(t, ctx, client, testAccountingID)
	if err := client.RemoveUser(ctx, testAccountingID); err != nil {
		t.Fatalf("удалить первого VLESS-пользователя: %v", err)
	}
	assertSmokeUserAbsent(t, ctx, client, testAccountingID)
	if outbound := testSmokeRoute(t, ctx, client, smokeSecondAccountingID); outbound != "bridge-test" {
		t.Fatalf("второе правило после удаления первого выбрало %q", outbound)
	}

	applySmokeRouting(t, ctx, client, file, nil)
	assertSmokeNoRoute(t, ctx, client, smokeSecondAccountingID)
	if err := client.RemoveUser(ctx, smokeSecondAccountingID); err != nil {
		t.Fatalf("удалить второго VLESS-пользователя: %v", err)
	}
	assertSmokeUserAbsent(t, ctx, client, smokeSecondAccountingID)
}

// applySmokeRouting повторяет боевой путь агента: записать желаемое состояние в
// конфигурацию и установить собранную из неё таблицу в работающий Xray.
func applySmokeRouting(
	t *testing.T,
	ctx context.Context,
	client *Client,
	file *ConfigFile,
	desired []PersistentUser,
) {
	t.Helper()
	if err := file.Reconcile(ctx, desired); err != nil {
		t.Fatalf("записать желаемое состояние: %v", err)
	}
	table, err := file.DesiredRouting()
	if err != nil {
		t.Fatalf("собрать таблицу маршрутизации: %v", err)
	}
	if err := client.ApplyRouting(ctx, table); err != nil {
		t.Fatalf("установить таблицу маршрутизации: %v", err)
	}
}

// assertSmokeForeignRulesSurvive проверяет, что установка таблицы сохранила
// правила, которыми агент не владеет: правило api и завершающий default-deny.
// Без них Xray отправил бы и служебный трафик, и трафик неизвестных клиентов в
// первый outbound конфигурации — в smoke-фикстуре это blackhole.
func assertSmokeForeignRulesSurvive(
	t *testing.T,
	ctx context.Context,
	client *Client,
	userRules int,
) {
	t.Helper()
	response, err := client.routing.ListRule(ctx, &routerCommand.ListRuleRequest{})
	if err != nil {
		t.Fatalf("прочитать полную таблицу: %v", err)
	}
	tags := make([]string, 0, len(response.GetRules()))
	for _, rule := range response.GetRules() {
		tags = append(tags, rule.GetRuleTag())
	}
	// правило api + персональные правила + default-deny
	if len(tags) != userRules+2 {
		t.Fatalf("таблица = %v, ожидались api, %d персональных и default-deny", tags, userRules)
	}
	if !slices.Contains(tags, defaultDenyRuleTagPrefix+"vless-in") {
		t.Fatalf("default-deny исчез из таблицы: %v", tags)
	}
}

func TestXrayFileStateSurvivesRestart(t *testing.T) {
	desired := User{
		AccountingID: testAccountingID, CredentialUUID: testCredentialUUID, Flow: flowVision,
	}
	configPath := durableSmokeConfig(t, []PersistentUser{{
		User: desired, OutboundTag: "direct",
	}})

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	containerID := dockerOutput(
		t, ctx, "run", "--rm", "--detach", "--publish", "127.0.0.1::10085",
		"--volume", configPath+":/usr/local/etc/xray/config.json:ro",
		smokeImage, "-config", "/usr/local/etc/xray/config.json",
	)
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		_ = exec.CommandContext(stopCtx, "docker", "rm", "--force", containerID).Run()
	})
	address := dockerOutput(t, ctx, "port", containerID, "10085/tcp")
	client, err := New(Config{Address: address, InboundTag: "vless-in"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	assertPersistentSmokeState(t, ctx, client, desired)

	dockerOutput(t, ctx, "restart", containerID)
	waitForXray(t, ctx, client)
	assertPersistentSmokeState(t, ctx, client, desired)
}

func assertPersistentSmokeState(t *testing.T, ctx context.Context, client *Client, desired User) {
	t.Helper()
	waitForXray(t, ctx, client)
	if got := smokeUser(t, ctx, client, desired.AccountingID); got != desired {
		t.Fatalf("persistent user = %+v, want %+v", got, desired)
	}
	if outbound := testSmokeRoute(t, ctx, client, desired.AccountingID); outbound != "direct" {
		t.Fatalf("persistent route = %q, want direct", outbound)
	}
	rules, err := client.UserRules(ctx)
	if err != nil || len(rules) != 1 || rules[0].AccountingID != desired.AccountingID {
		t.Fatalf("persistent rules = %+v, error=%v", rules, err)
	}
}

func durableSmokeConfig(t *testing.T, desired []PersistentUser) string {
	t.Helper()
	source, err := filepath.Abs("testdata/smoke-config.json")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(configPath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	configuration, err := NewConfigFile(configPath, "vless-in", "block")
	if err != nil {
		t.Fatal(err)
	}
	if err := configuration.Reconcile(context.Background(), desired); err != nil {
		t.Fatal(err)
	}
	return configPath
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
	outbound, err := client.TestUserRoute(ctx, accountingID)
	if err != nil || outbound != "block" {
		t.Fatalf("TestRoute без персонального правила = %q, error=%v; ожидался block", outbound, err)
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
