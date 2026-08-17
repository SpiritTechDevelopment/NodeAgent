package state

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type migration struct {
	version int
	path    string
	after   func(context.Context, *sql.Tx) error
}

var migrations = []migration{
	{
		version: 1,
		path:    "migrations/0001_initial.sql",
		after:   initializeMetadata,
	},
}

func migrate(ctx context.Context, db *sql.DB) error {
	if err := validateMigrations(); err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin state migration: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var currentVersion int
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&currentVersion); err != nil {
		return fmt.Errorf("read state schema version: %w", err)
	}
	latestVersion := migrations[len(migrations)-1].version
	if currentVersion > latestVersion {
		return fmt.Errorf(
			"state schema version %d is newer than supported version %d",
			currentVersion,
			latestVersion,
		)
	}

	for _, migration := range migrations {
		if migration.version <= currentVersion {
			continue
		}
		if err := applyMigration(ctx, tx, migration); err != nil {
			return err
		}
		currentVersion = migration.version
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit state migration: %w", err)
	}
	return nil
}

func applyMigration(ctx context.Context, tx *sql.Tx, migration migration) error {
	script, err := migrationFiles.ReadFile(migration.path)
	if err != nil {
		return fmt.Errorf("read state migration %d: %w", migration.version, err)
	}
	if _, err := tx.ExecContext(ctx, string(script)); err != nil {
		return fmt.Errorf("apply state migration %d: %w", migration.version, err)
	}
	if migration.after != nil {
		if err := migration.after(ctx, tx); err != nil {
			return fmt.Errorf("finalize state migration %d: %w", migration.version, err)
		}
	}
	if _, err := tx.ExecContext(
		ctx,
		fmt.Sprintf("PRAGMA user_version = %d", migration.version),
	); err != nil {
		return fmt.Errorf("record state schema version %d: %w", migration.version, err)
	}
	return nil
}

func initializeMetadata(ctx context.Context, tx *sql.Tx) error {
	spoolID, err := newUUID()
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO agent_meta (singleton, usage_spool_id) VALUES (1, ?)`,
		spoolID,
	); err != nil {
		return fmt.Errorf("initialize state metadata: %w", err)
	}
	return nil
}

func validateMigrations() error {
	for index, migration := range migrations {
		wantVersion := index + 1
		if migration.version != wantVersion {
			return fmt.Errorf(
				"state migration %q has version %d instead of %d",
				migration.path,
				migration.version,
				wantVersion,
			)
		}
		if migration.path == "" {
			return fmt.Errorf("state migration %d has no file path", migration.version)
		}
		if _, err := migrationFiles.ReadFile(migration.path); err != nil {
			return fmt.Errorf("read state migration %d: %w", migration.version, err)
		}
	}
	return nil
}
