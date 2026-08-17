package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// PutManagedUser сохраняет желаемое и применённое состояние пользователя.
func (s *Store) PutManagedUser(ctx context.Context, user ManagedUser) error {
	return s.Transaction(ctx, func(tx *Transaction) error {
		return tx.PutManagedUser(ctx, user)
	})
}

// PutManagedUser сохраняет пользователя внутри текущей транзакции.
func (tx *Transaction) PutManagedUser(ctx context.Context, user ManagedUser) error {
	if err := validateManagedUser(user); err != nil {
		return err
	}
	_, err := tx.tx.ExecContext(
		ctx,
		`INSERT INTO managed_users (
		     accounting_id,
		     credential_uuid,
		     flow,
		     egress_key,
		     desired_present,
		     applied,
		     updated_at_unix_ms
		 ) VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (accounting_id) DO UPDATE SET
		     credential_uuid = excluded.credential_uuid,
		     flow = excluded.flow,
		     egress_key = excluded.egress_key,
		     desired_present = excluded.desired_present,
		     applied = excluded.applied,
		     updated_at_unix_ms = excluded.updated_at_unix_ms`,
		user.AccountingID,
		user.CredentialUUID,
		user.Flow,
		user.EgressKey,
		intFromBool(user.DesiredPresent),
		intFromBool(user.Applied),
		unixMilli(user.UpdatedAt),
	)
	if err != nil {
		return fmt.Errorf("store managed user %q: %w", user.AccountingID, err)
	}
	return nil
}

// ManagedUser возвращает пользователя по accounting_id.
func (s *Store) ManagedUser(ctx context.Context, accountingID string) (ManagedUser, error) {
	return readManagedUser(ctx, s.db, accountingID)
}

// ManagedUser возвращает пользователя по accounting_id внутри текущей транзакции.
func (tx *Transaction) ManagedUser(ctx context.Context, accountingID string) (ManagedUser, error) {
	return readManagedUser(ctx, tx.tx, accountingID)
}

// ManagedUsers возвращает полный снапшот пользователей по возрастанию accounting_id.
func (s *Store) ManagedUsers(ctx context.Context) ([]ManagedUser, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT accounting_id,
		        credential_uuid,
		        flow,
		        egress_key,
		        desired_present,
		        applied,
		        updated_at_unix_ms
		   FROM managed_users
		  ORDER BY accounting_id`,
	)
	if err != nil {
		return nil, fmt.Errorf("list managed users: %w", err)
	}
	defer rows.Close()

	var users []ManagedUser
	for rows.Next() {
		user, err := scanManagedUser(rows)
		if err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate managed users: %w", err)
	}
	return users, nil
}

type managedUserQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type rowScanner interface {
	Scan(...any) error
}

func readManagedUser(
	ctx context.Context,
	queryer managedUserQueryer,
	accountingID string,
) (ManagedUser, error) {
	accountingID = strings.TrimSpace(accountingID)
	if accountingID == "" {
		return ManagedUser{}, errors.New("accounting ID is required")
	}
	user, err := scanManagedUser(queryer.QueryRowContext(
		ctx,
		`SELECT accounting_id,
		        credential_uuid,
		        flow,
		        egress_key,
		        desired_present,
		        applied,
		        updated_at_unix_ms
		   FROM managed_users
		  WHERE accounting_id = ?`,
		accountingID,
	))
	if errors.Is(err, sql.ErrNoRows) {
		return ManagedUser{}, ErrNotFound
	}
	if err != nil {
		return ManagedUser{}, fmt.Errorf("read managed user %q: %w", accountingID, err)
	}
	return user, nil
}

func scanManagedUser(scanner rowScanner) (ManagedUser, error) {
	var (
		user           ManagedUser
		desiredPresent int
		applied        int
		updatedAt      int64
	)
	if err := scanner.Scan(
		&user.AccountingID,
		&user.CredentialUUID,
		&user.Flow,
		&user.EgressKey,
		&desiredPresent,
		&applied,
		&updatedAt,
	); err != nil {
		return ManagedUser{}, err
	}
	user.DesiredPresent = desiredPresent == 1
	user.Applied = applied == 1
	user.UpdatedAt = time.UnixMilli(updatedAt).UTC()
	return user, nil
}

func validateManagedUser(user ManagedUser) error {
	if strings.TrimSpace(user.AccountingID) == "" {
		return errors.New("accounting ID is required")
	}
	if user.UpdatedAt.IsZero() {
		return errors.New("managed user update time is required")
	}
	return nil
}
