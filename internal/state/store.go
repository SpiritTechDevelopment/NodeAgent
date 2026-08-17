package state

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const (
	directoryMode os.FileMode = 0o700
	databaseMode  os.FileMode = 0o600
)

// Store предоставляет транзакционный доступ к локальной SQLite-базе.
type Store struct {
	db                      *sql.DB
	recoveredFromCorruption bool
}

// Transaction предоставляет операции, выполняемые в одной SQLite-транзакции.
type Transaction struct {
	tx *sql.Tx
}

// Open создаёт или открывает SQLite-базу, проверяет её и применяет миграции.
func Open(ctx context.Context, path string) (*Store, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("database path is required")
	}

	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve database path: %w", err)
	}
	if err := prepareDatabasePath(absolutePath); err != nil {
		return nil, err
	}

	store, err := openSQLite(ctx, absolutePath)
	if err == nil {
		return store, nil
	}
	if !errors.Is(err, errCorruptDatabase) {
		return nil, err
	}
	if quarantineErr := quarantineCorruptDatabase(absolutePath); quarantineErr != nil {
		return nil, fmt.Errorf("quarantine corrupt SQLite database: %w", quarantineErr)
	}
	if err := prepareDatabasePath(absolutePath); err != nil {
		return nil, err
	}

	store, err = openSQLite(ctx, absolutePath)
	if err != nil {
		return nil, fmt.Errorf("create replacement SQLite database: %w", err)
	}
	store.recoveredFromCorruption = true
	return store, nil
}

// Close закрывает SQLite-базу.
func (s *Store) Close() error {
	return s.db.Close()
}

// RecoveredFromCorruption показывает, была ли повреждённая база изолирована
// и заменена новой при открытии.
func (s *Store) RecoveredFromCorruption() bool {
	return s.recoveredFromCorruption
}

// Transaction выполняет fn атомарно и откатывает изменения при любой ошибке.
func (s *Store) Transaction(ctx context.Context, fn func(*Transaction) error) error {
	if fn == nil {
		return errors.New("transaction function is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin state transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if err := fn(&Transaction{tx: tx}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit state transaction: %w", err)
	}
	return nil
}

func (s *Store) initialize(ctx context.Context) error {
	if err := s.db.PingContext(ctx); err != nil {
		return classifyDatabaseError(fmt.Errorf("connect to SQLite database: %w", err))
	}

	var integrity string
	if err := s.db.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&integrity); err != nil {
		return classifyDatabaseError(fmt.Errorf("check SQLite database integrity: %w", err))
	}
	if integrity != "ok" {
		return fmt.Errorf("%w: %s", errCorruptDatabase, integrity)
	}

	if err := migrate(ctx, s.db); err != nil {
		return classifyDatabaseError(err)
	}
	return nil
}

func openSQLite(ctx context.Context, path string) (*Store, error) {
	db, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		return nil, fmt.Errorf("open SQLite database: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	store := &Store{db: db}
	if err := store.initialize(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func prepareDatabasePath(path string) error {
	directory := filepath.Dir(path)
	directoryInfo, err := os.Stat(directory)
	if err != nil {
		return fmt.Errorf("inspect state directory: %w", err)
	}
	if !directoryInfo.IsDir() {
		return errors.New("state directory path must point to a directory")
	}
	if mode := directoryInfo.Mode().Perm(); mode != directoryMode {
		return fmt.Errorf(
			"state directory permissions are %04o instead of %04o",
			mode,
			directoryMode,
		)
	}

	info, err := os.Lstat(path)
	switch {
	case err == nil && info.Mode()&os.ModeSymlink != 0:
		return errors.New("database path must not be a symbolic link")
	case err == nil && !info.Mode().IsRegular():
		return errors.New("database path must point to a regular file")
	case err == nil && info.Mode().Perm() != databaseMode:
		return fmt.Errorf(
			"database permissions are %04o instead of %04o",
			info.Mode().Perm(),
			databaseMode,
		)
	case err != nil && !errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("inspect database path: %w", err)
	}

	if errors.Is(err, os.ErrNotExist) {
		file, createErr := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, databaseMode)
		if createErr != nil {
			return fmt.Errorf("create database file: %w", createErr)
		}
		if closeErr := file.Close(); closeErr != nil {
			return fmt.Errorf("close new database file: %w", closeErr)
		}
	}
	return nil
}

func sqliteDSN(path string) string {
	uri := url.URL{Scheme: "file", Path: path}
	query := uri.Query()
	query.Set("_busy_timeout", "5000")
	query.Set("_foreign_keys", "on")
	query.Set("_journal_mode", "wal")
	query.Set("_synchronous", "full")
	query.Set("_txlock", "immediate")
	uri.RawQuery = query.Encode()
	return uri.String()
}

var errCorruptDatabase = errors.New("SQLite database is corrupt")

func classifyDatabaseError(err error) error {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return err
	}
	switch sqliteErr.Code() & 0xff {
	case sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB:
		return fmt.Errorf("%w: %v", errCorruptDatabase, err)
	default:
		return err
	}
}

func quarantineCorruptDatabase(path string) error {
	quarantineID, err := newUUID()
	if err != nil {
		return err
	}
	quarantinePath := path + ".corrupt-" + quarantineID
	if err := os.Rename(path, quarantinePath); err != nil {
		return fmt.Errorf("preserve database file: %w", err)
	}

	for _, suffix := range []string{"-wal", "-shm"} {
		if err := os.Rename(path+suffix, quarantinePath+suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("preserve SQLite %s file: %w", suffix, err)
		}
	}
	return nil
}

func newUUID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate usage spool ID: %w", err)
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	return fmt.Sprintf(
		"%08x-%04x-%04x-%04x-%012x",
		value[0:4],
		value[4:6],
		value[6:8],
		value[8:10],
		value[10:16],
	), nil
}

func unixMilli(value time.Time) int64 {
	return value.UTC().UnixMilli()
}

func nullableUnixMilli(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return unixMilli(value)
}

func timeFromNullableUnixMilli(value sql.NullInt64) time.Time {
	if !value.Valid {
		return time.Time{}
	}
	return time.UnixMilli(value.Int64).UTC()
}

func intFromBool(value bool) int {
	if value {
		return 1
	}
	return 0
}
