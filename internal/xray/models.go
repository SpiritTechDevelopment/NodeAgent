package xray

import (
	"errors"
	"time"
)

var (
	// ErrUserNotFound означает, что пользователь отсутствует в настроенном inbound.
	ErrUserNotFound = errors.New("Xray user not found")
	// ErrRouteNotFound означает, что Xray не нашёл подходящего правила маршрутизации.
	ErrRouteNotFound = errors.New("Xray route not found")
)

// Health содержит результат проверки локального процесса Xray.
type Health struct {
	// Uptime содержит время работы процесса по данным StatsService.
	Uptime time.Duration
}

// User содержит фактические параметры VLESS-пользователя в Xray.
type User struct {
	// AccountingID содержит Xray email пользователя.
	AccountingID string
	// CredentialUUID содержит VLESS UUID пользователя.
	CredentialUUID string
	// Flow содержит режим VLESS flow.
	Flow string
}

// UserRule описывает персональное правило маршрутизации пользователя.
type UserRule struct {
	// AccountingID содержит стабильный Xray email пользователя.
	AccountingID string
	// OutboundTag содержит фактический тег целевого outbound.
	OutboundTag string
	// RuleTag содержит детерминированный идентификатор правила Xray.
	RuleTag string
}

// Usage содержит ненулевую дельту трафика backend-owned пользователя.
type Usage struct {
	// AccountingID содержит стабильный идентификатор пользователя.
	AccountingID string
	// UplinkBytes содержит число байтов от клиента.
	UplinkBytes uint64
	// DownlinkBytes содержит число байтов к клиенту.
	DownlinkBytes uint64
}
