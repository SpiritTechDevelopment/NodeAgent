package xray

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	jsonreader "github.com/xtls/xray-core/infra/conf/json"
)

const defaultDenyRuleTagPrefix = "spirit-agent:default-deny:"

// PersistentUser describes the part of Xray's file configuration owned by the
// agent. OutboundTag is already resolved: an empty backend egress key must not
// reach this layer.
type PersistentUser struct {
	User        User
	OutboundTag string
}

// ConfigFile atomically persists agent-owned clients and routing rules in an
// infrastructure-provided Xray JSON configuration.
type ConfigFile struct {
	path                string
	inboundTag          string
	fallbackOutboundTag string
	mu                  sync.Mutex
}

// NewConfigFile validates the existing Xray configuration before it can be
// used as a durable desired-state target.
func NewConfigFile(path, inboundTag, fallbackOutboundTag string) (*ConfigFile, error) {
	if strings.TrimSpace(path) == "" || strings.TrimSpace(path) != path {
		return nil, errors.New("Xray config path is required without surrounding whitespace")
	}
	if _, err := validateInboundTag(inboundTag); err != nil {
		return nil, err
	}
	if err := ValidateOutboundTag(fallbackOutboundTag); err != nil {
		return nil, err
	}
	result := &ConfigFile{
		path:                path,
		inboundTag:          inboundTag,
		fallbackOutboundTag: fallbackOutboundTag,
	}
	if _, _, err := result.read(); err != nil {
		return nil, err
	}
	return result, nil
}

// Reconcile writes the complete agent-owned desired set. The replacement is
// atomic and fsynced, so a successful return survives both process restarts.
func (file *ConfigFile) Reconcile(ctx context.Context, users []PersistentUser) error {
	if ctx == nil {
		return errors.New("Xray config reconcile context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	desired := slices.Clone(users)
	slices.SortFunc(desired, func(left, right PersistentUser) int {
		return strings.Compare(left.User.AccountingID, right.User.AccountingID)
	})
	for index, item := range desired {
		if err := ValidateUser(item.User); err != nil {
			return fmt.Errorf("validate persistent Xray user %d: %w", index, err)
		}
		if err := ValidateOutboundTag(item.OutboundTag); err != nil {
			return fmt.Errorf("validate persistent Xray route %d: %w", index, err)
		}
		if index > 0 && desired[index-1].User.AccountingID == item.User.AccountingID {
			return errors.New("duplicate persistent Xray accounting ID")
		}
	}

	file.mu.Lock()
	defer file.mu.Unlock()
	configuration, mode, err := file.read()
	if err != nil {
		return err
	}
	if err := file.replaceClients(configuration, desired); err != nil {
		return err
	}
	if err := file.replaceRules(configuration, desired); err != nil {
		return err
	}
	payload, err := json.MarshalIndent(configuration, "", "  ")
	if err != nil {
		return fmt.Errorf("encode Xray config: %w", err)
	}
	payload = append(payload, '\n')
	if err := validateRealityOutbounds(configuration); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return atomicWriteFile(file.path, payload, mode)
}

func (file *ConfigFile) read() (map[string]any, os.FileMode, error) {
	payload, err := os.ReadFile(file.path)
	if err != nil {
		return nil, 0, fmt.Errorf("read Xray config: %w", err)
	}
	info, err := os.Stat(file.path)
	if err != nil {
		return nil, 0, fmt.Errorf("inspect Xray config: %w", err)
	}
	decoder := json.NewDecoder(&jsonreader.Reader{Reader: bytes.NewReader(payload)})
	decoder.UseNumber()
	var configuration map[string]any
	if err := decoder.Decode(&configuration); err != nil {
		return nil, 0, fmt.Errorf("decode Xray config: %w", err)
	}
	if err := validateRealityOutbounds(configuration); err != nil {
		return nil, 0, err
	}
	return configuration, info.Mode().Perm(), nil
}

func (file *ConfigFile) replaceClients(configuration map[string]any, users []PersistentUser) error {
	inbounds, ok := objectList(configuration["inbounds"])
	if !ok {
		return errors.New("Xray config inbounds must be an array")
	}
	var target map[string]any
	for _, inbound := range inbounds {
		if stringValue(inbound["tag"]) != file.inboundTag {
			continue
		}
		if target != nil {
			return fmt.Errorf("Xray config contains duplicate inbound tag %q", file.inboundTag)
		}
		target = inbound
	}
	if target == nil {
		return fmt.Errorf("Xray config has no inbound %q", file.inboundTag)
	}
	if stringValue(target["protocol"]) != "vless" {
		return fmt.Errorf("Xray inbound %q must use VLESS", file.inboundTag)
	}
	settings, ok := target["settings"].(map[string]any)
	if !ok {
		return fmt.Errorf("Xray inbound %q settings must be an object", file.inboundTag)
	}
	clients, ok := objectList(settings["clients"])
	if !ok && settings["clients"] != nil {
		return fmt.Errorf("Xray inbound %q clients must be an array", file.inboundTag)
	}
	kept := make([]any, 0, len(clients)+len(users))
	for _, client := range clients {
		if ValidateAccountingID(stringValue(client["email"])) != nil {
			kept = append(kept, client)
		}
	}
	for _, item := range users {
		client := map[string]any{
			"id":    item.User.CredentialUUID,
			"email": item.User.AccountingID,
		}
		if item.User.Flow != "" {
			client["flow"] = item.User.Flow
		}
		kept = append(kept, client)
	}
	settings["clients"] = kept
	return nil
}

func (file *ConfigFile) replaceRules(configuration map[string]any, users []PersistentUser) error {
	routing, ok := configuration["routing"].(map[string]any)
	if !ok {
		return errors.New("Xray config routing must be an object")
	}
	rules, ok := objectList(routing["rules"])
	if !ok && routing["rules"] != nil {
		return errors.New("Xray config routing.rules must be an array")
	}
	outbounds, ok := objectList(configuration["outbounds"])
	if !ok {
		return errors.New("Xray config outbounds must be an array")
	}
	availableOutbounds := make(map[string]struct{}, len(outbounds))
	for _, outbound := range outbounds {
		availableOutbounds[stringValue(outbound["tag"])] = struct{}{}
	}
	for _, item := range users {
		if _, found := availableOutbounds[item.OutboundTag]; !found {
			return fmt.Errorf("Xray config has no outbound %q", item.OutboundTag)
		}
	}
	if _, found := availableOutbounds[file.fallbackOutboundTag]; !found {
		return fmt.Errorf("Xray config has no fallback outbound %q", file.fallbackOutboundTag)
	}
	kept := make([]any, 0, len(rules)+len(users)+1)
	var defaultDeny map[string]any
	for _, rule := range rules {
		switch {
		case file.isManagedRule(rule):
			continue
		case file.isDefaultDeny(rule):
			if defaultDeny == nil {
				defaultDeny = rule
			}
			continue
		default:
			kept = append(kept, rule)
		}
	}
	for _, item := range users {
		kept = append(kept, map[string]any{
			"type":        "field",
			"user":        []any{item.User.AccountingID},
			"outboundTag": item.OutboundTag,
			"ruleTag":     userRuleTagPrefix + item.User.AccountingID,
		})
	}
	if defaultDeny == nil {
		defaultDeny = map[string]any{
			"type":        "field",
			"inboundTag":  []any{file.inboundTag},
			"outboundTag": file.fallbackOutboundTag,
		}
	}
	defaultDeny["ruleTag"] = defaultDenyRuleTagPrefix + file.inboundTag
	kept = append(kept, defaultDeny)
	routing["rules"] = kept
	return nil
}

func (file *ConfigFile) isManagedRule(rule map[string]any) bool {
	if strings.HasPrefix(stringValue(rule["ruleTag"]), userRuleTagPrefix) {
		return true
	}
	for _, accountingID := range stringList(rule["user"]) {
		if ValidateAccountingID(accountingID) == nil {
			return true
		}
	}
	return false
}

func (file *ConfigFile) isDefaultDeny(rule map[string]any) bool {
	if stringValue(rule["outboundTag"]) != file.fallbackOutboundTag || len(stringList(rule["user"])) != 0 {
		return false
	}
	return slices.Contains(stringList(rule["inboundTag"]), file.inboundTag)
}

func objectList(value any) ([]map[string]any, bool) {
	if value == nil {
		return nil, true
	}
	items, ok := value.([]any)
	if !ok {
		return nil, false
	}
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		object, ok := item.(map[string]any)
		if !ok {
			return nil, false
		}
		result = append(result, object)
	}
	return result, true
}

func stringValue(value any) string {
	result, _ := value.(string)
	return result
}

func stringList(value any) []string {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		if text, ok := item.(string); ok {
			result = append(result, text)
		}
	}
	return result
}

// Xray 26.3.27 accepts both publicKey and its newer alias password for a
// Reality client, preferring password when both are present.
func validateRealityOutbounds(configuration map[string]any) error {
	outbounds, ok := objectList(configuration["outbounds"])
	if !ok {
		return errors.New("Xray config outbounds must be an array")
	}
	for _, outbound := range outbounds {
		stream, ok := outbound["streamSettings"].(map[string]any)
		if !ok || stringValue(stream["security"]) != "reality" {
			continue
		}
		reality, ok := stream["realitySettings"].(map[string]any)
		if !ok {
			return fmt.Errorf("Reality outbound %q has no realitySettings", stringValue(outbound["tag"]))
		}
		key := stringValue(reality["password"])
		if key == "" {
			key = stringValue(reality["publicKey"])
		}
		decoded, err := base64.RawURLEncoding.DecodeString(key)
		if key == "" {
			return fmt.Errorf("Reality outbound %q has empty publicKey/password", stringValue(outbound["tag"]))
		}
		if err != nil || len(decoded) != 32 {
			return fmt.Errorf("Reality outbound %q has invalid publicKey/password", stringValue(outbound["tag"]))
		}
	}
	return nil
}

func atomicWriteFile(path string, payload []byte, mode os.FileMode) error {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary Xray config: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(mode); err != nil {
		return fmt.Errorf("set temporary Xray config mode: %w", err)
	}
	if _, err := temporary.Write(payload); err != nil {
		return fmt.Errorf("write temporary Xray config: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync temporary Xray config: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary Xray config: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace Xray config: %w", err)
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open Xray config directory: %w", err)
	}
	defer directoryHandle.Close()
	if err := directoryHandle.Sync(); err != nil {
		return fmt.Errorf("sync Xray config directory: %w", err)
	}
	return nil
}
