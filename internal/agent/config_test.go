package agent

import (
	"errors"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigReturnsValidatedValuesAndDefaults(t *testing.T) {
	environment := validEnvironment()
	config, err := LoadConfig(mapEnvironment(environment), "v1.2.3")
	if err != nil {
		t.Fatalf("LoadConfig() вернул ошибку: %v", err)
	}
	if config.NodeID != "node-a" || config.AgentVersion != "v1.2.3" {
		t.Fatalf("идентичность процесса = node:%q version:%q", config.NodeID, config.AgentVersion)
	}
	if config.ListenAddress != "10.82.2.11:9443" {
		t.Fatalf("listen address = %q", config.ListenAddress)
	}
	if config.HTTPListenAddress != defaultHTTPListenAddress {
		t.Fatalf("HTTP listen address = %q", config.HTTPListenAddress)
	}
	if config.Xray.Address != defaultXrayAPIAddress || config.Xray.InboundTag != "public-vless" {
		t.Fatalf("Xray config = %+v", config.Xray)
	}
	if config.FallbackOutboundTag != "block" ||
		config.StateDatabasePath != defaultStateDatabasePath ||
		config.MaximumInventoryUsers != 2000 || config.ShutdownTimeout != 10*time.Second {
		t.Fatalf("значения по умолчанию = %+v", config)
	}
	if config.LogLevel != slog.LevelInfo {
		t.Fatalf("log level = %s", config.LogLevel)
	}
	wantIdentities := []string{"backend-a.internal", "spiffe://spiritvpn/backend"}
	if !slices.Equal(config.GRPC.TLS.AllowedClientIdentities, wantIdentities) {
		t.Fatalf("client identities = %v", config.GRPC.TLS.AllowedClientIdentities)
	}
}

func TestLoadConfigAppliesOptionalOverrides(t *testing.T) {
	environment := validEnvironment()
	environment[EnvLogLevel] = "warn"
	environment[EnvXrayAPIAddress] = "[::1]:10086"
	environment[EnvFallbackOutboundTag] = "deny-all"
	environment[EnvStateDatabasePath] = "/var/lib/custom-agent/state.db"
	environment[EnvMaximumInventoryUsers] = "1500"
	environment[EnvShutdownTimeout] = "25s"
	environment[EnvHTTPListen] = "[::1]:9091"
	config, err := LoadConfig(mapEnvironment(environment), "test")
	if err != nil {
		t.Fatalf("LoadConfig() вернул ошибку: %v", err)
	}
	if config.LogLevel != slog.LevelWarn || config.Xray.Address != "[::1]:10086" ||
		config.FallbackOutboundTag != "deny-all" || config.MaximumInventoryUsers != 1500 ||
		config.ShutdownTimeout != 25*time.Second || config.HTTPListenAddress != "[::1]:9091" {
		t.Fatalf("переопределённая конфигурация = %+v", config)
	}
}

func TestLoadConfigRejectsNonLoopbackHTTPListeners(t *testing.T) {
	for _, address := range []string{
		":9090",
		"0.0.0.0:9090",
		"10.82.2.11:9090",
		"localhost:9090",
		"127.0.0.1:0",
	} {
		t.Run(address, func(t *testing.T) {
			environment := validEnvironment()
			environment[EnvHTTPListen] = address
			if _, err := LoadConfig(mapEnvironment(environment), "test"); err == nil || !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("LoadConfig() error = %v", err)
			}
		})
	}
}

func TestLoadConfigRejectsMissingRequiredValuesTogether(t *testing.T) {
	config, err := LoadConfig(mapEnvironment(nil), "")
	if err == nil || !errors.Is(err, ErrMissingConfig) {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if !reflect.DeepEqual(config, Config{}) {
		t.Fatalf("при ошибке возвращена частичная конфигурация: %+v", config)
	}
	for _, name := range []string{
		EnvNodeID,
		EnvGRPCListen,
		EnvGRPCCertificateFile,
		EnvGRPCPrivateKeyFile,
		EnvGRPCClientCAFile,
		EnvGRPCClientIdentities,
		EnvXrayInboundTag,
		EnvLocalOutboundTag,
	} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("совокупная ошибка не содержит %s: %v", name, err)
		}
	}
}

func TestLoadConfigRejectsUnsafeManagementListeners(t *testing.T) {
	for _, address := range []string{
		":9443",
		"0.0.0.0:9443",
		"[::]:9443",
		"agent.internal:9443",
		"10.82.2.11:0",
	} {
		t.Run(address, func(t *testing.T) {
			environment := validEnvironment()
			environment[EnvGRPCListen] = address
			if _, err := LoadConfig(mapEnvironment(environment), "test"); err == nil || !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("LoadConfig() error = %v", err)
			}
		})
	}
}

func TestLoadConfigRejectsInvalidOptionalValues(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "log level", key: EnvLogLevel, value: "verbose"},
		{name: "inventory limit", key: EnvMaximumInventoryUsers, value: "0"},
		{name: "shutdown timeout", key: EnvShutdownTimeout, value: "forever"},
		{name: "duplicate identity", key: EnvGRPCClientIdentities, value: "backend, backend"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			environment := validEnvironment()
			environment[test.key] = test.value
			if _, err := LoadConfig(mapEnvironment(environment), "test"); err == nil || !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("LoadConfig() error = %v", err)
			}
		})
	}
}

func validEnvironment() map[string]string {
	return map[string]string{
		EnvNodeID:               "node-a",
		EnvGRPCListen:           "10.82.2.11:9443",
		EnvGRPCCertificateFile:  "/run/spirit-agent/server.crt",
		EnvGRPCPrivateKeyFile:   "/run/spirit-agent/server.key",
		EnvGRPCClientCAFile:     "/run/spirit-agent/backend-ca.crt",
		EnvGRPCClientIdentities: "spiffe://spiritvpn/backend, backend-a.internal",
		EnvXrayInboundTag:       "public-vless",
		EnvLocalOutboundTag:     "direct",
	}
}

func mapEnvironment(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}
