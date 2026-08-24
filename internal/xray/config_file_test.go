package xray

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/xtls/xray-core/app/router"
)

func TestConfigFilePersistsUsersAndRulesBeforeLastDefaultDeny(t *testing.T) {
	path := copySmokeConfig(t)
	configuration := readTestConfig(t, path)
	routing := configuration["routing"].(map[string]any)
	rules := routing["rules"].([]any)
	routing["rules"] = append([]any{
		map[string]any{
			"type": "field", "inboundTag": []any{"vless-in"}, "outboundTag": "block",
		},
		map[string]any{
			"type": "field", "user": []any{testAccountingID}, "outboundTag": "wrong",
		},
	}, rules...)
	writeTestConfig(t, path, configuration)

	file, err := NewConfigFile(path, "vless-in", "block")
	if err != nil {
		t.Fatalf("NewConfigFile() error = %v", err)
	}
	secondID := "u.bcdefghijklmnopqrstu"
	if err := file.Reconcile(context.Background(), []PersistentUser{
		{User: User{AccountingID: secondID, CredentialUUID: "22222222-2222-4222-8222-222222222222"}, OutboundTag: "bridge-test"},
		{User: User{AccountingID: testAccountingID, CredentialUUID: "11111111-1111-4111-8111-111111111111", Flow: "xtls-rprx-vision"}, OutboundTag: "direct"},
	}); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	got := readTestConfig(t, path)
	inbounds := got["inbounds"].([]any)
	clients := inbounds[1].(map[string]any)["settings"].(map[string]any)["clients"].([]any)
	if len(clients) != 2 || stringValue(clients[0].(map[string]any)["email"]) != testAccountingID ||
		stringValue(clients[1].(map[string]any)["email"]) != secondID {
		t.Fatalf("persisted clients = %+v", clients)
	}

	rules = got["routing"].(map[string]any)["rules"].([]any)
	if len(rules) != 4 {
		t.Fatalf("routing rules = %+v", rules)
	}
	firstManaged := rules[1].(map[string]any)
	secondManaged := rules[2].(map[string]any)
	last := rules[3].(map[string]any)
	if stringValue(firstManaged["ruleTag"]) != userRuleTagPrefix+testAccountingID ||
		stringValue(secondManaged["ruleTag"]) != userRuleTagPrefix+secondID {
		t.Fatalf("managed rules are not stable and sorted: %+v", rules)
	}
	if stringValue(last["outboundTag"]) != "block" ||
		stringValue(last["ruleTag"]) != defaultDenyRuleTagPrefix+"vless-in" {
		t.Fatalf("last rule is not default-deny: %+v", last)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o640 {
		t.Fatalf("config mode = %v", info.Mode().Perm())
	}
}

// Регрессия: таблица, уезжающая в работающий Xray, обязана содержать правила
// всех пользователей сразу, правило инфраструктуры и default-deny в порядке
// приоритета. Прежний код слал по одному правилу за раз с ShouldAppend=false,
// поэтому в роутере выживало только последнее.
func TestDesiredRoutingCarriesEveryRuleToRuntime(t *testing.T) {
	path := copySmokeConfig(t)
	file, err := NewConfigFile(path, "vless-in", "block")
	if err != nil {
		t.Fatalf("NewConfigFile() error = %v", err)
	}
	secondID := "u.bcdefghijklmnopqrstu"
	if err := file.Reconcile(context.Background(), []PersistentUser{
		{
			User:        User{AccountingID: testAccountingID, CredentialUUID: testCredentialUUID, Flow: flowVision},
			OutboundTag: "direct",
		},
		{
			User:        User{AccountingID: secondID, CredentialUUID: "22222222-2222-4222-8222-222222222222"},
			OutboundTag: "bridge-test",
		},
	}); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	table, err := file.DesiredRouting()
	if err != nil {
		t.Fatalf("DesiredRouting() error = %v", err)
	}
	if table.Empty() {
		t.Fatal("DesiredRouting() вернула пустую таблицу")
	}

	routing := &fakeRoutingClient{}
	client := newClient(io.NopCloser(nilReader{}), &fakeStatsClient{}, routing)
	if err := client.ApplyRouting(context.Background(), table); err != nil {
		t.Fatalf("ApplyRouting() error = %v", err)
	}
	instance, err := routing.addRequest.GetConfig().GetInstance()
	if err != nil {
		t.Fatalf("декодировать routing config: %v", err)
	}
	rules := instance.(*router.Config).GetRule()
	gotTags := make([]string, 0, len(rules))
	for _, rule := range rules {
		gotTags = append(gotTags, rule.GetRuleTag())
	}
	// Первое правило — инфраструктурное (api) из fixture, у него нет ruleTag:
	// оно обязано пережить установку таблицы. Пользователи идут отсортированно.
	wantTags := []string{
		"",
		userRuleTagPrefix + testAccountingID,
		userRuleTagPrefix + secondID,
		defaultDenyRuleTagPrefix + "vless-in",
	}
	if !slices.Equal(gotTags, wantTags) {
		t.Fatalf("rule_tag в runtime = %v, ожидались %v", gotTags, wantTags)
	}
	if got := rules[len(rules)-1].GetTag(); got != "block" {
		t.Fatalf("последнее правило ведёт в %q, ожидался default-deny в block", got)
	}
}

func TestConfigFileRejectsEmptyRealityPublicKey(t *testing.T) {
	path := copySmokeConfig(t)
	configuration := readTestConfig(t, path)
	addRealityOutbound(configuration, "publicKey", "")
	writeTestConfig(t, path, configuration)
	_, err := NewConfigFile(path, "vless-in", "block")
	if err == nil || !strings.Contains(err.Error(), "empty publicKey/password") {
		t.Fatalf("NewConfigFile() error = %v", err)
	}
}

func TestConfigFileAcceptsRealityPublicKeyAndPasswordAliases(t *testing.T) {
	const key = "lMqgNaTTnkTY1Rwe-krqEFNNQKT8MzDOrxlRHVQd9Aw"
	for _, field := range []string{"publicKey", "password"} {
		t.Run(field, func(t *testing.T) {
			path := copySmokeConfig(t)
			configuration := readTestConfig(t, path)
			addRealityOutbound(configuration, field, key)
			writeTestConfig(t, path, configuration)
			if _, err := NewConfigFile(path, "vless-in", "block"); err != nil {
				t.Fatalf("NewConfigFile() error = %v", err)
			}
		})
	}
}

func addRealityOutbound(configuration map[string]any, keyField, key string) {
	configuration["outbounds"] = append(configuration["outbounds"].([]any), map[string]any{
		"tag": "broken-reality", "protocol": "vless",
		"settings": map[string]any{"vnext": []any{map[string]any{
			"address": "example.com", "port": json.Number("443"),
			"users": []any{map[string]any{"id": "11111111-1111-4111-8111-111111111111", "encryption": "none"}},
		}}},
		"streamSettings": map[string]any{
			"network": "tcp", "security": "reality",
			"realitySettings": map[string]any{
				"serverName": "example.com", "shortId": "0123456789abcdef", keyField: key,
			},
		},
	})
}

func copySmokeConfig(t *testing.T) string {
	t.Helper()
	payload, err := os.ReadFile("testdata/smoke-config.json")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, payload, 0o640); err != nil {
		t.Fatal(err)
	}
	return path
}

func readTestConfig(t *testing.T, path string) map[string]any {
	t.Helper()
	handle, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	decoder := json.NewDecoder(handle)
	decoder.UseNumber()
	var result map[string]any
	if err := decoder.Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

func writeTestConfig(t *testing.T, path string, configuration map[string]any) {
	t.Helper()
	payload, err := json.MarshalIndent(configuration, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(payload, '\n'), 0o640); err != nil {
		t.Fatal(err)
	}
}
