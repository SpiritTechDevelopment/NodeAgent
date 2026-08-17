package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// CreateOperation создаёт ожидающую запись журнала операций.
func (tx *Transaction) CreateOperation(ctx context.Context, operation Operation) error {
	if err := validateNewOperation(operation); err != nil {
		return err
	}
	result, err := tx.tx.ExecContext(
		ctx,
		`INSERT INTO operations (
		     operation_id,
		     operation_type,
		     request_digest,
		     status,
		     result,
		     created_at_unix_ms,
		     updated_at_unix_ms
		 ) VALUES (?, ?, ?, ?, NULL, ?, ?)
		 ON CONFLICT (operation_id) DO NOTHING`,
		operation.ID,
		operation.Type,
		operation.RequestDigest[:],
		OperationStatusPending,
		unixMilli(operation.CreatedAt),
		unixMilli(operation.CreatedAt),
	)
	if err != nil {
		return fmt.Errorf("create operation %q: %w", operation.ID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("create operation %q: inspect affected rows: %w", operation.ID, err)
	}
	if rows == 0 {
		return ErrOperationExists
	}
	return nil
}

// CompleteOperation атомарно сохраняет терминальный результат ожидающей операции.
func (tx *Transaction) CompleteOperation(
	ctx context.Context,
	operationID string,
	resultPayload []byte,
	completedAt time.Time,
) error {
	operationID = strings.TrimSpace(operationID)
	if operationID == "" {
		return errors.New("operation ID is required")
	}
	if len(resultPayload) == 0 {
		return errors.New("operation result is required")
	}
	if completedAt.IsZero() {
		return errors.New("operation completion time is required")
	}

	result, err := tx.tx.ExecContext(
		ctx,
		`UPDATE operations
		    SET status = ?, result = ?, updated_at_unix_ms = ?
		  WHERE operation_id = ? AND status = ?`,
		OperationStatusCompleted,
		resultPayload,
		unixMilli(completedAt),
		operationID,
		OperationStatusPending,
	)
	if err != nil {
		return fmt.Errorf("complete operation %q: %w", operationID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("complete operation %q: inspect affected rows: %w", operationID, err)
	}
	if rows == 0 {
		return ErrOperationNotPending
	}
	return nil
}

// Operation возвращает запись журнала по operation_id.
func (s *Store) Operation(ctx context.Context, operationID string) (Operation, error) {
	return readOperation(ctx, s.db, operationID)
}

// Operation возвращает запись журнала по operation_id внутри текущей транзакции.
func (tx *Transaction) Operation(ctx context.Context, operationID string) (Operation, error) {
	return readOperation(ctx, tx.tx, operationID)
}

type operationQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readOperation(
	ctx context.Context,
	queryer operationQueryer,
	operationID string,
) (Operation, error) {
	operationID = strings.TrimSpace(operationID)
	if operationID == "" {
		return Operation{}, errors.New("operation ID is required")
	}

	var (
		operation Operation
		digest    []byte
		createdAt int64
		updatedAt int64
	)
	err := queryer.QueryRowContext(
		ctx,
		`SELECT operation_id,
		        operation_type,
		        request_digest,
		        status,
		        result,
		        created_at_unix_ms,
		        updated_at_unix_ms
		   FROM operations
		  WHERE operation_id = ?`,
		operationID,
	).Scan(
		&operation.ID,
		&operation.Type,
		&digest,
		&operation.Status,
		&operation.Result,
		&createdAt,
		&updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Operation{}, ErrNotFound
	}
	if err != nil {
		return Operation{}, fmt.Errorf("read operation %q: %w", operationID, err)
	}
	if len(digest) != sha256.Size {
		return Operation{}, fmt.Errorf("read operation %q: invalid digest length %d", operationID, len(digest))
	}
	copy(operation.RequestDigest[:], digest)
	operation.Result = append([]byte(nil), operation.Result...)
	operation.CreatedAt = time.UnixMilli(createdAt).UTC()
	operation.UpdatedAt = time.UnixMilli(updatedAt).UTC()
	return operation, nil
}

func validateNewOperation(operation Operation) error {
	if strings.TrimSpace(operation.ID) == "" {
		return errors.New("operation ID is required")
	}
	switch operation.Type {
	case OperationTypeEnsurePresent, OperationTypeEnsureAbsent, OperationTypeReconcile:
	default:
		return fmt.Errorf("unsupported operation type %q", operation.Type)
	}
	if operation.CreatedAt.IsZero() {
		return errors.New("operation creation time is required")
	}
	if operation.Status != "" && operation.Status != OperationStatusPending {
		return errors.New("new operation must be pending")
	}
	if len(operation.Result) != 0 {
		return errors.New("new operation must not contain a result")
	}
	return nil
}
