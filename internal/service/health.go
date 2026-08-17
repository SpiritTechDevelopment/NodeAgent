package service

import (
	"context"
	"time"
)

// StatusProvider возвращает согласованный снимок состояния агента ноды и его
// локальных зависимостей. Реализации должны поддерживать конкурентные вызовы.
type StatusProvider interface {
	Status(context.Context) (Status, error)
}

// Status содержит данные, необходимые для определения готовности агента ноды.
type Status struct {
	// StateWritable показывает, принимает ли локальное долговременное хранилище записи.
	StateWritable bool
	// NeedsBootstrap показывает, требуется ли авторитетная сверка состояния.
	NeedsBootstrap bool
	// UsageCollectionSafe показывает, безопасен ли сейчас сброс счётчиков Xray.
	UsageCollectionSafe bool
	// Xray содержит результат последней проверки состояния Xray.
	Xray XrayStatus
	// Activity содержит состояние независимой подсистемы активности.
	Activity ActivityStatus
}

// Ready показывает, может ли агент ноды безопасно выполнять все обязанности v1.
func (s Status) Ready() bool {
	return s.StateWritable &&
		!s.NeedsBootstrap &&
		s.UsageCollectionSafe &&
		s.Xray.Reachable
}

// XrayStatus содержит результат последней проверки локального процесса Xray.
type XrayStatus struct {
	// Reachable показывает, ответил ли API Xray на проверку состояния.
	Reachable bool
	// Uptime содержит неотрицательное время работы, сообщённое Xray.
	Uptime time.Duration
	// LastError содержит очищенное диагностическое сообщение для оператора.
	LastError string
}

// ActivityStatus содержит данные о состоянии опциональной подсистемы активности.
type ActivityStatus struct {
	// Enabled показывает, настроен ли сбор активности.
	Enabled bool
	// Healthy показывает, работает ли включённый сбор активности штатно.
	Healthy bool
	// LastClosedBucketEnd содержит время окончания последнего сохранённого бакета активности.
	LastClosedBucketEnd time.Time
	// OutboxBatches содержит число неподтверждённых пакетов активности.
	OutboxBatches uint64
	// LastError содержит очищенное диагностическое сообщение для оператора.
	LastError string
}
