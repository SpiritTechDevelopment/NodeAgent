package xray

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
