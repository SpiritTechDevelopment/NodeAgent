package agent

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/SpiritTechDevelopment/NodeAgent/internal/grpcserver"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/xray"
)

// Имена переменных окружения процесса агента ноды.
const (
	EnvNodeID                = "SPIRIT_NODE_ID"
	EnvLogLevel              = "SPIRIT_LOG_LEVEL"
	EnvGRPCListen            = "SPIRIT_GRPC_LISTEN"
	EnvHTTPListen            = "SPIRIT_HTTP_LISTEN"
	EnvGRPCCertificateFile   = "SPIRIT_GRPC_TLS_CERT_FILE"
	EnvGRPCPrivateKeyFile    = "SPIRIT_GRPC_TLS_KEY_FILE"
	EnvGRPCClientCAFile      = "SPIRIT_GRPC_TLS_CLIENT_CA_FILE"
	EnvGRPCClientIdentities  = "SPIRIT_GRPC_ALLOWED_CLIENT_IDENTITIES"
	EnvXrayAPIAddress        = "SPIRIT_XRAY_API_ADDRESS"
	EnvXrayInboundTag        = "SPIRIT_XRAY_INBOUND_TAG"
	EnvLocalOutboundTag      = "SPIRIT_XRAY_LOCAL_OUTBOUND_TAG"
	EnvFallbackOutboundTag   = "SPIRIT_XRAY_FALLBACK_OUTBOUND_TAG"
	EnvStateDatabasePath     = "SPIRIT_STATE_DB_PATH"
	EnvMaximumInventoryUsers = "SPIRIT_MAX_INVENTORY_USERS"
	EnvShutdownTimeout       = "SPIRIT_SHUTDOWN_TIMEOUT"
)

const (
	defaultLogLevel              = "info"
	defaultHTTPListenAddress     = "127.0.0.1:9090"
	defaultXrayAPIAddress        = "127.0.0.1:10085"
	defaultFallbackOutboundTag   = "block"
	defaultStateDatabasePath     = "/var/lib/spirit-agent/state.db"
	defaultMaximumInventoryUsers = 2000
	defaultShutdownTimeout       = 10 * time.Second
)

var (
	// ErrMissingConfig означает отсутствие обязательного параметра процесса.
	ErrMissingConfig = errors.New("required configuration is missing")
	// ErrInvalidConfig означает, что параметр процесса имеет недопустимое значение.
	ErrInvalidConfig = errors.New("configuration is invalid")
)

// Config содержит проверенную конфигурацию исполняемого агента ноды.
type Config struct {
	// NodeID содержит стабильную идентичность ноды.
	NodeID string
	// AgentVersion содержит версию текущей сборки.
	AgentVersion string
	// LogLevel задаёт минимальный уровень структурированных логов.
	LogLevel slog.Level
	// ListenAddress содержит конкретный management IP и порт gRPC.
	ListenAddress string
	// HTTPListenAddress содержит loopback IP и порт служебного HTTP-сервера.
	HTTPListenAddress string
	// GRPC задаёт параметры mTLS-сервера.
	GRPC grpcserver.Config
	// Xray задаёт адрес локального API и клиентский inbound.
	Xray xray.Config
	// LocalOutboundTag содержит outbound локального FREEDOM-выхода.
	LocalOutboundTag string
	// FallbackOutboundTag содержит безопасный outbound по умолчанию.
	FallbackOutboundTag string
	// StateDatabasePath содержит путь секретной SQLite-базы.
	StateDatabasePath string
	// MaximumInventoryUsers ограничивает полный набор пользователей.
	MaximumInventoryUsers int
	// ShutdownTimeout ограничивает graceful shutdown серверов агента.
	ShutdownTimeout time.Duration
}

// LoadConfig читает конфигурацию из окружения и возвращает все найденные ошибки.
func LoadConfig(getenv func(string) string, version string) (Config, error) {
	if getenv == nil {
		return Config{}, errors.New("environment reader is required")
	}
	var (
		config Config
		errs   []error
	)
	config.NodeID = required(getenv, EnvNodeID, &errs)
	config.AgentVersion = strings.TrimSpace(version)
	if config.AgentVersion == "" {
		errs = append(errs, fmt.Errorf("%w: build version", ErrMissingConfig))
	}

	level := new(slog.Level)
	if err := level.UnmarshalText([]byte(value(getenv, EnvLogLevel, defaultLogLevel))); err != nil {
		errs = append(errs, fmt.Errorf("%w: %s", ErrInvalidConfig, EnvLogLevel))
	} else {
		config.LogLevel = *level
	}

	listenAddress, err := managementListenAddress(required(getenv, EnvGRPCListen, &errs))
	if err != nil {
		errs = append(errs, fmt.Errorf("%w: %s", ErrInvalidConfig, EnvGRPCListen))
	}
	config.ListenAddress = listenAddress
	httpListenAddress, err := localListenAddress(value(getenv, EnvHTTPListen, defaultHTTPListenAddress))
	if err != nil {
		errs = append(errs, fmt.Errorf("%w: %s", ErrInvalidConfig, EnvHTTPListen))
	}
	config.HTTPListenAddress = httpListenAddress
	config.GRPC.TLS = grpcserver.TLSConfig{
		CertificateFile: required(getenv, EnvGRPCCertificateFile, &errs),
		PrivateKeyFile:  required(getenv, EnvGRPCPrivateKeyFile, &errs),
		ClientCAFile:    required(getenv, EnvGRPCClientCAFile, &errs),
	}
	identities, err := clientIdentities(getenv(EnvGRPCClientIdentities))
	if err != nil {
		errs = append(errs, err)
	}
	config.GRPC.TLS.AllowedClientIdentities = identities

	config.Xray = xray.Config{
		Address:    value(getenv, EnvXrayAPIAddress, defaultXrayAPIAddress),
		InboundTag: required(getenv, EnvXrayInboundTag, &errs),
	}
	config.LocalOutboundTag = required(getenv, EnvLocalOutboundTag, &errs)
	config.FallbackOutboundTag = value(getenv, EnvFallbackOutboundTag, defaultFallbackOutboundTag)
	config.StateDatabasePath = value(getenv, EnvStateDatabasePath, defaultStateDatabasePath)

	maximumUsers, err := positiveInteger(
		value(getenv, EnvMaximumInventoryUsers, strconv.Itoa(defaultMaximumInventoryUsers)),
	)
	if err != nil {
		errs = append(errs, fmt.Errorf("%w: %s", ErrInvalidConfig, EnvMaximumInventoryUsers))
	}
	config.MaximumInventoryUsers = maximumUsers
	shutdownTimeout, err := time.ParseDuration(
		value(getenv, EnvShutdownTimeout, defaultShutdownTimeout.String()),
	)
	if err != nil || shutdownTimeout <= 0 {
		errs = append(errs, fmt.Errorf("%w: %s", ErrInvalidConfig, EnvShutdownTimeout))
	}
	config.ShutdownTimeout = shutdownTimeout

	if err := errors.Join(errs...); err != nil {
		return Config{}, err
	}
	return config, nil
}

func required(getenv func(string) string, name string, errs *[]error) string {
	result := strings.TrimSpace(getenv(name))
	if result == "" {
		*errs = append(*errs, fmt.Errorf("%w: %s", ErrMissingConfig, name))
	}
	return result
}

func value(getenv func(string) string, name, fallback string) string {
	result := strings.TrimSpace(getenv(name))
	if result == "" {
		return fallback
	}
	return result
}

func managementListenAddress(raw string) (string, error) {
	host, portText, err := net.SplitHostPort(raw)
	if err != nil {
		return "", err
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.IsUnspecified() || ip.IsMulticast() {
		return "", errors.New("listener must use a concrete numeric management IP")
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return "", errors.New("listener must use a non-zero port")
	}
	return net.JoinHostPort(ip.String(), strconv.FormatUint(port, 10)), nil
}

func localListenAddress(raw string) (string, error) {
	address, err := managementListenAddress(raw)
	if err != nil {
		return "", err
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return "", err
	}
	if !net.ParseIP(host).IsLoopback() {
		return "", errors.New("listener must use a loopback IP")
	}
	return address, nil
}

func clientIdentities(raw string) ([]string, error) {
	var identities []string
	seen := make(map[string]struct{})
	for _, item := range strings.Split(raw, ",") {
		identity := strings.TrimSpace(item)
		if identity == "" {
			continue
		}
		if _, duplicate := seen[identity]; duplicate {
			return nil, fmt.Errorf("%w: duplicate %s", ErrInvalidConfig, EnvGRPCClientIdentities)
		}
		seen[identity] = struct{}{}
		identities = append(identities, identity)
	}
	if len(identities) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrMissingConfig, EnvGRPCClientIdentities)
	}
	slices.Sort(identities)
	return identities, nil
}

func positiveInteger(raw string) (int, error) {
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, errors.New("positive integer is required")
	}
	return value, nil
}
