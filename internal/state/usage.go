package state

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// AppendUsageBatches атомарно присваивает sequence и добавляет пачки в outbox.
func (s *Store) AppendUsageBatches(
	ctx context.Context,
	collectedAt time.Time,
	payloads [][]byte,
) ([]UsageBatch, error) {
	if collectedAt.IsZero() {
		return nil, errors.New("usage collection time is required")
	}
	if len(payloads) == 0 {
		return nil, errors.New("at least one usage payload is required")
	}
	for index, payload := range payloads {
		if len(payload) == 0 {
			return nil, fmt.Errorf("usage payload %d is empty", index)
		}
	}

	var batches []UsageBatch
	err := s.Transaction(ctx, func(tx *Transaction) error {
		metadata, err := readMetadata(ctx, tx.tx)
		if err != nil {
			return err
		}
		if metadata.UsageSequence > math.MaxInt64-uint64(len(payloads)) {
			return errors.New("usage sequence is exhausted")
		}

		pending := make([]UsageBatch, 0, len(payloads))
		sequence := metadata.UsageSequence
		for _, payload := range payloads {
			sequence++
			if _, err := tx.tx.ExecContext(
				ctx,
				`INSERT INTO usage_batches (
				     spool_id,
				     sequence,
				     collected_at_unix_ms,
				     payload,
				     acknowledged
				 ) VALUES (?, ?, ?, ?, 0)`,
				metadata.UsageSpoolID,
				int64(sequence),
				unixMilli(collectedAt),
				payload,
			); err != nil {
				return fmt.Errorf("append usage batch %d: %w", sequence, err)
			}
			pending = append(pending, UsageBatch{
				SpoolID:     metadata.UsageSpoolID,
				Sequence:    sequence,
				CollectedAt: collectedAt.UTC(),
				Payload:     append([]byte(nil), payload...),
			})
		}

		result, err := tx.tx.ExecContext(
			ctx,
			`UPDATE agent_meta SET usage_sequence = ? WHERE singleton = 1`,
			int64(sequence),
		)
		if err != nil {
			return fmt.Errorf("advance usage sequence: %w", err)
		}
		if err := requireSingleRow(result, "advance usage sequence"); err != nil {
			return err
		}
		batches = pending
		return nil
	})
	if err != nil {
		return nil, err
	}
	return batches, nil
}

// PendingUsageBatches возвращает неподтверждённые пачки и запоминает наибольший
// sequence, который агент выдал backend.
func (s *Store) PendingUsageBatches(ctx context.Context, limit int) ([]UsageBatch, error) {
	if limit <= 0 {
		return nil, errors.New("usage batch limit must be positive")
	}

	var batches []UsageBatch
	err := s.Transaction(ctx, func(tx *Transaction) error {
		rows, err := tx.tx.QueryContext(
			ctx,
			`SELECT spool_id, sequence, collected_at_unix_ms, payload
			   FROM usage_batches
			  WHERE acknowledged = 0
			  ORDER BY sequence
			  LIMIT ?`,
			limit,
		)
		if err != nil {
			return fmt.Errorf("list pending usage batches: %w", err)
		}

		pending := make([]UsageBatch, 0, limit)
		for rows.Next() {
			var (
				batch       UsageBatch
				sequence    int64
				collectedAt int64
			)
			if err := rows.Scan(&batch.SpoolID, &sequence, &collectedAt, &batch.Payload); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan pending usage batch: %w", err)
			}
			batch.Sequence = uint64(sequence)
			batch.CollectedAt = time.UnixMilli(collectedAt).UTC()
			batch.Payload = append([]byte(nil), batch.Payload...)
			pending = append(pending, batch)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("iterate pending usage batches: %w", err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close pending usage batches: %w", err)
		}

		if len(pending) > 0 {
			highest := pending[len(pending)-1].Sequence
			result, err := tx.tx.ExecContext(
				ctx,
				`UPDATE agent_meta
				    SET highest_emitted_usage_sequence = max(highest_emitted_usage_sequence, ?)
				  WHERE singleton = 1`,
				int64(highest),
			)
			if err != nil {
				return fmt.Errorf("record emitted usage sequence: %w", err)
			}
			if err := requireSingleRow(result, "record emitted usage sequence"); err != nil {
				return err
			}
		}
		batches = pending
		return nil
	})
	if err != nil {
		return nil, err
	}
	return batches, nil
}

// AcknowledgeUsageBatches удаляет подтверждённые пачки только для текущего спула
// и только до наибольшего ранее выданного sequence.
func (s *Store) AcknowledgeUsageBatches(
	ctx context.Context,
	spoolID string,
	through uint64,
) (UsageAcknowledgement, error) {
	spoolID = strings.TrimSpace(spoolID)
	if spoolID == "" || through == 0 {
		return UsageAcknowledgementEmpty, nil
	}

	outcome := UsageAcknowledgementApplied
	err := s.Transaction(ctx, func(tx *Transaction) error {
		metadata, err := readMetadata(ctx, tx.tx)
		if err != nil {
			return err
		}
		switch {
		case spoolID != metadata.UsageSpoolID:
			outcome = UsageAcknowledgementForeignSpool
			return nil
		case through > metadata.HighestEmittedUsageSequence:
			outcome = UsageAcknowledgementBeyondEmitted
			return nil
		}

		if _, err := tx.tx.ExecContext(
			ctx,
			`UPDATE usage_batches
			    SET acknowledged = 1
			  WHERE spool_id = ? AND sequence <= ?`,
			spoolID,
			int64(through),
		); err != nil {
			return fmt.Errorf("acknowledge usage batches: %w", err)
		}
		if _, err := tx.tx.ExecContext(
			ctx,
			`DELETE FROM usage_batches
			  WHERE spool_id = ? AND sequence <= ? AND acknowledged = 1`,
			spoolID,
			int64(through),
		); err != nil {
			return fmt.Errorf("delete acknowledged usage batches: %w", err)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return outcome, nil
}

// PendingUsageBatchCount возвращает число неподтверждённых пачек в outbox.
func (s *Store) PendingUsageBatchCount(ctx context.Context) (uint64, error) {
	stats, err := s.UsageOutboxStats(ctx)
	return stats.Batches, err
}

// UsageOutboxStats возвращает число и суммарный размер payload
// неподтверждённых пачек usage.
func (s *Store) UsageOutboxStats(ctx context.Context) (UsageOutboxStats, error) {
	var (
		batches int64
		bytes   int64
	)
	if err := s.db.QueryRowContext(
		ctx,
		`SELECT count(*), coalesce(sum(length(payload)), 0)
		   FROM usage_batches
		  WHERE acknowledged = 0`,
	).Scan(&batches, &bytes); err != nil {
		return UsageOutboxStats{}, fmt.Errorf("read usage outbox stats: %w", err)
	}
	if batches < 0 || bytes < 0 {
		return UsageOutboxStats{}, errors.New("usage outbox stats are negative")
	}
	return UsageOutboxStats{Batches: uint64(batches), PayloadBytes: uint64(bytes)}, nil
}
