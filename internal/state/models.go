package state

import (
	"crypto/sha256"
	"errors"
	"time"
)

var (
	// ErrNotFound означает, что запрошенная запись отсутствует.
	ErrNotFound = errors.New("state record not found")
	// ErrOperationExists означает, что operation_id уже зарегистрирован.
	ErrOperationExists = errors.New("operation already exists")
	// ErrOperationNotPending означает, что завершить можно только ожидающую операцию.
	ErrOperationNotPending = errors.New("operation is not pending")
)

// Metadata содержит служебное состояние локальной базы.
type Metadata struct {
	// UsageSpoolID содержит UUID текущего спула трафика.
	UsageSpoolID string
	// UsageSequence содержит sequence последней сохранённой пачки.
	UsageSequence uint64
	// HighestEmittedUsageSequence содержит наибольший выданный backend sequence.
	HighestEmittedUsageSequence uint64
	// Initialized показывает, завершена ли авторитетная начальная сверка.
	Initialized bool
	// LastXrayAuditAt содержит время последней полной проверки Xray.
	LastXrayAuditAt time.Time
}

// NeedsBootstrap показывает, требуется ли авторитетная начальная сверка.
func (m Metadata) NeedsBootstrap() bool {
	return !m.Initialized
}

// ManagedUser содержит долговременное желаемое состояние пользователя Xray.
type ManagedUser struct {
	// AccountingID содержит стабильный идентификатор пользователя для учёта трафика.
	AccountingID string
	// CredentialUUID содержит секретный VLESS UUID.
	CredentialUUID string
	// Flow содержит режим VLESS flow.
	Flow string
	// EgressKey содержит логический ключ выхода; пустое значение означает FREEDOM.
	EgressKey string
	// DesiredPresent показывает, должен ли пользователь присутствовать в Xray.
	DesiredPresent bool
	// Applied показывает, подтверждено ли желаемое состояние фактической проверкой Xray.
	Applied bool
	// UpdatedAt содержит время последнего изменения intent.
	UpdatedAt time.Time
}

// OperationType определяет вид мутирующей операции агента.
type OperationType string

const (
	// OperationTypeEnsurePresent соответствует EnsureUserPresent.
	OperationTypeEnsurePresent OperationType = "ENSURE_PRESENT"
	// OperationTypeEnsureAbsent соответствует EnsureUserAbsent.
	OperationTypeEnsureAbsent OperationType = "ENSURE_ABSENT"
	// OperationTypeReconcile соответствует ReconcileUsers.
	OperationTypeReconcile OperationType = "RECONCILE"
)

// OperationStatus определяет состояние записи в журнале операций.
type OperationStatus string

const (
	// OperationStatusPending означает, что операция ещё не получила терминальный результат.
	OperationStatusPending OperationStatus = "PENDING"
	// OperationStatusCompleted означает, что терминальный результат сохранён.
	OperationStatusCompleted OperationStatus = "COMPLETED"
)

// Operation содержит запись долговременного журнала идемпотентности.
type Operation struct {
	// ID содержит уникальный operation_id из запроса backend.
	ID string
	// Type содержит вид мутирующей операции.
	Type OperationType
	// RequestDigest содержит канонический SHA-256 digest запроса.
	RequestDigest [sha256.Size]byte
	// Status содержит текущее состояние операции.
	Status OperationStatus
	// Result содержит сериализованный терминальный protobuf-ответ.
	Result []byte
	// CreatedAt содержит время регистрации операции.
	CreatedAt time.Time
	// UpdatedAt содержит время последнего изменения операции.
	UpdatedAt time.Time
}

// UsageBatch содержит сериализованную пачку трафика в локальном outbox.
type UsageBatch struct {
	// SpoolID содержит UUID спула, в котором создана пачка.
	SpoolID string
	// Sequence содержит монотонный номер пачки внутри спула.
	Sequence uint64
	// CollectedAt содержит время сбора счётчиков на ноде.
	CollectedAt time.Time
	// Payload содержит непрозрачное сериализованное содержимое пачки.
	Payload []byte
}

// UsageOutboxStats содержит размер неподтверждённого usage outbox.
type UsageOutboxStats struct {
	// Batches содержит число неподтверждённых пачек.
	Batches uint64
	// PayloadBytes содержит суммарный размер сериализованных payload.
	PayloadBytes uint64
}

// UsageAcknowledgement описывает результат применения курсора подтверждения.
type UsageAcknowledgement string

const (
	// UsageAcknowledgementApplied означает, что подтверждение принято.
	UsageAcknowledgementApplied UsageAcknowledgement = "APPLIED"
	// UsageAcknowledgementEmpty означает, что пустой курсор был проигнорирован.
	UsageAcknowledgementEmpty UsageAcknowledgement = "EMPTY"
	// UsageAcknowledgementForeignSpool означает, что курсор относится к другому спулу.
	UsageAcknowledgementForeignSpool UsageAcknowledgement = "FOREIGN_SPOOL"
	// UsageAcknowledgementBeyondEmitted означает, что backend подтвердил ещё не выданную пачку.
	UsageAcknowledgementBeyondEmitted UsageAcknowledgement = "BEYOND_EMITTED"
)
