package state

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Metadata возвращает служебное состояние локальной базы.
func (s *Store) Metadata(ctx context.Context) (Metadata, error) {
	return readMetadata(ctx, s.db)
}

// SetInitialized отмечает, завершена ли авторитетная начальная сверка.
func (s *Store) SetInitialized(ctx context.Context, initialized bool) error {
	return s.Transaction(ctx, func(tx *Transaction) error {
		return tx.SetInitialized(ctx, initialized)
	})
}

// SetInitialized отмечает завершение авторитетной сверки внутри транзакции.
func (tx *Transaction) SetInitialized(ctx context.Context, initialized bool) error {
	result, err := tx.tx.ExecContext(
		ctx,
		`UPDATE agent_meta SET initialized = ? WHERE singleton = 1`,
		intFromBool(initialized),
	)
	if err != nil {
		return fmt.Errorf("update initialized state: %w", err)
	}
	return requireSingleRow(result, "update initialized state")
}

// SetLastXrayAuditAt сохраняет время последней полной проверки Xray.
func (s *Store) SetLastXrayAuditAt(ctx context.Context, observedAt time.Time) error {
	result, err := s.db.ExecContext(
		ctx,
		`UPDATE agent_meta SET last_xray_audit_at_unix_ms = ? WHERE singleton = 1`,
		nullableUnixMilli(observedAt),
	)
	if err != nil {
		return fmt.Errorf("update last Xray audit time: %w", err)
	}
	return requireSingleRow(result, "update last Xray audit time")
}

// Writable проверяет, что локальная база принимает транзакционные записи.
func (s *Store) Writable(ctx context.Context) error {
	return s.Transaction(ctx, func(tx *Transaction) error {
		result, err := tx.tx.ExecContext(
			ctx,
			`UPDATE agent_meta SET initialized = initialized WHERE singleton = 1`,
		)
		if err != nil {
			return fmt.Errorf("probe writable state: %w", err)
		}
		return requireSingleRow(result, "probe writable state")
	})
}

type metadataQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readMetadata(ctx context.Context, queryer metadataQueryer) (Metadata, error) {
	var (
		metadata       Metadata
		usageSequence  int64
		highestEmitted int64
		initialized    int
		lastAuditAt    sql.NullInt64
	)
	err := queryer.QueryRowContext(
		ctx,
		`SELECT usage_spool_id,
		        usage_sequence,
		        highest_emitted_usage_sequence,
		        initialized,
		        last_xray_audit_at_unix_ms
		   FROM agent_meta
		  WHERE singleton = 1`,
	).Scan(
		&metadata.UsageSpoolID,
		&usageSequence,
		&highestEmitted,
		&initialized,
		&lastAuditAt,
	)
	if err != nil {
		return Metadata{}, fmt.Errorf("read state metadata: %w", err)
	}
	metadata.UsageSequence = uint64(usageSequence)
	metadata.HighestEmittedUsageSequence = uint64(highestEmitted)
	metadata.Initialized = initialized == 1
	metadata.LastXrayAuditAt = timeFromNullableUnixMilli(lastAuditAt)
	return metadata, nil
}

func requireSingleRow(result sql.Result, action string) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: inspect affected rows: %w", action, err)
	}
	if rows != 1 {
		return fmt.Errorf("%s: affected %d rows instead of 1", action, rows)
	}
	return nil
}
