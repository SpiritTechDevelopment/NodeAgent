package state

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

var uuidPattern = regexp.MustCompile(
	`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`,
)

func TestOpenInitializesAndReopensState(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	directory := filepath.Join(root, "agent-state")
	path := filepath.Join(directory, "state.db")

	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatalf("создать каталог: %v", err)
	}

	store, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open() вернул ошибку: %v", err)
	}
	if store.RecoveredFromCorruption() {
		t.Error("новая база ошибочно помечена восстановленной после повреждения")
	}

	assertMode(t, directory, 0o700)
	assertMode(t, path, 0o600)
	assertSQLitePragmas(t, store)
	assertSidecarModes(t, path)

	metadata, err := store.Metadata(ctx)
	if err != nil {
		t.Fatalf("Metadata() вернул ошибку: %v", err)
	}
	if !uuidPattern.MatchString(metadata.UsageSpoolID) {
		t.Errorf("usage_spool_id = %q, ожидался UUID v4", metadata.UsageSpoolID)
	}
	if metadata.UsageSequence != 0 {
		t.Errorf("usage sequence = %d, ожидался 0", metadata.UsageSequence)
	}
	if metadata.HighestEmittedUsageSequence != 0 {
		t.Errorf("highest emitted sequence = %d, ожидался 0", metadata.HighestEmittedUsageSequence)
	}
	if metadata.Initialized {
		t.Error("новая база не должна быть initialized")
	}
	if !metadata.NeedsBootstrap() {
		t.Error("новая база должна требовать bootstrap")
	}
	if !metadata.LastXrayAuditAt.IsZero() {
		t.Errorf("last Xray audit = %v, ожидалось нулевое время", metadata.LastXrayAuditAt)
	}

	auditAt := time.Date(2026, time.August, 14, 11, 12, 13, 456_000_000, time.UTC)
	if err := store.SetInitialized(ctx, true); err != nil {
		t.Fatalf("SetInitialized() вернул ошибку: %v", err)
	}
	if err := store.SetLastXrayAuditAt(ctx, auditAt); err != nil {
		t.Fatalf("SetLastXrayAuditAt() вернул ошибку: %v", err)
	}
	if err := store.Writable(ctx); err != nil {
		t.Fatalf("Writable() вернул ошибку: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() вернул ошибку: %v", err)
	}

	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("повторный Open() вернул ошибку: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	reopenedMetadata, err := reopened.Metadata(ctx)
	if err != nil {
		t.Fatalf("Metadata() после открытия вернул ошибку: %v", err)
	}
	if reopenedMetadata.UsageSpoolID != metadata.UsageSpoolID {
		t.Errorf(
			"usage_spool_id после открытия = %q, ожидался %q",
			reopenedMetadata.UsageSpoolID,
			metadata.UsageSpoolID,
		)
	}
	if !reopenedMetadata.Initialized {
		t.Error("initialized не сохранился после открытия")
	}
	if reopenedMetadata.NeedsBootstrap() {
		t.Error("инициализированная база не должна требовать bootstrap")
	}
	if !reopenedMetadata.LastXrayAuditAt.Equal(auditAt) {
		t.Errorf(
			"last Xray audit после открытия = %v, ожидалось %v",
			reopenedMetadata.LastXrayAuditAt,
			auditAt,
		)
	}
}

func TestOpenRejectsUnsupportedAndInvalidDatabases(t *testing.T) {
	t.Run("newer schema", func(t *testing.T) {
		ctx := context.Background()
		path := filepath.Join(t.TempDir(), "state.db")
		store := openTestStore(t, path)
		if _, err := store.db.ExecContext(ctx, "PRAGMA user_version = 2"); err != nil {
			t.Fatalf("задать user_version: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("закрыть базу: %v", err)
		}

		_, err := Open(ctx, path)
		if err == nil || !strings.Contains(err.Error(), "newer than supported") {
			t.Fatalf("Open() error = %v, ожидалась ошибка новой версии схемы", err)
		}
	})

	t.Run("corrupt database", func(t *testing.T) {
		directory := t.TempDir()
		setTestDirectoryMode(t, directory)
		path := filepath.Join(directory, "state.db")
		corruptContents := []byte("not a SQLite database")
		if err := os.WriteFile(path, corruptContents, 0o600); err != nil {
			t.Fatalf("создать повреждённую базу: %v", err)
		}
		store, err := Open(context.Background(), path)
		if err != nil {
			t.Fatalf("Open() не восстановил повреждённую базу: %v", err)
		}
		defer store.Close()
		if !store.RecoveredFromCorruption() {
			t.Error("восстановленная база не помечена как recovered")
		}
		metadata, err := store.Metadata(context.Background())
		if err != nil {
			t.Fatalf("Metadata() вернул ошибку: %v", err)
		}
		if !metadata.NeedsBootstrap() {
			t.Error("база после восстановления должна требовать bootstrap")
		}

		matches, err := filepath.Glob(path + ".corrupt-*")
		if err != nil {
			t.Fatalf("найти изолированную базу: %v", err)
		}
		if len(matches) != 1 {
			t.Fatalf("изолированные файлы = %v, ожидался один файл базы", matches)
		}
		preserved, err := os.ReadFile(matches[0])
		if err != nil {
			t.Fatalf("прочитать изолированную базу: %v", err)
		}
		if !slices.Equal(preserved, corruptContents) {
			t.Errorf("изолированная база = %q, ожидалось %q", preserved, corruptContents)
		}
	})

	t.Run("symbolic link", func(t *testing.T) {
		root := t.TempDir()
		setTestDirectoryMode(t, root)
		target := filepath.Join(root, "target.db")
		if err := os.WriteFile(target, nil, 0o600); err != nil {
			t.Fatalf("создать целевой файл: %v", err)
		}
		link := filepath.Join(root, "state.db")
		if err := os.Symlink(target, link); err != nil {
			t.Fatalf("создать символическую ссылку: %v", err)
		}
		if _, err := Open(context.Background(), link); err == nil {
			t.Fatal("Open() не отклонил символическую ссылку")
		}
	})

	t.Run("missing directory", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing", "state.db")
		if _, err := Open(context.Background(), path); err == nil {
			t.Fatal("Open() не отклонил отсутствующий каталог")
		}
	})

	t.Run("unsafe directory permissions", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "state")
		if err := os.Mkdir(directory, 0o755); err != nil {
			t.Fatalf("создать каталог: %v", err)
		}
		if _, err := Open(context.Background(), filepath.Join(directory, "state.db")); err == nil {
			t.Fatal("Open() не отклонил небезопасные права каталога")
		}
	})

	t.Run("unsafe database permissions", func(t *testing.T) {
		directory := t.TempDir()
		setTestDirectoryMode(t, directory)
		path := filepath.Join(directory, "state.db")
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			t.Fatalf("создать файл базы: %v", err)
		}
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatalf("задать права базы: %v", err)
		}
		if _, err := Open(context.Background(), path); err == nil {
			t.Fatal("Open() не отклонил небезопасные права базы")
		}
	})
}

func TestTransactionRollsBackUserAndOperation(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "state.db"))
	t.Cleanup(func() { _ = store.Close() })

	now := time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)
	user := testManagedUser("u.aaaaaaaaaaaaaaaaaaaa", now)
	digest := sha256.Sum256([]byte("request"))
	operation := Operation{
		ID:            "operation-1",
		Type:          OperationTypeEnsurePresent,
		RequestDigest: digest,
		CreatedAt:     now,
	}
	wantErr := errors.New("отменить транзакцию")

	err := store.Transaction(ctx, func(tx *Transaction) error {
		if err := tx.CreateOperation(ctx, operation); err != nil {
			return err
		}
		if err := tx.PutManagedUser(ctx, user); err != nil {
			return err
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Transaction() error = %v, ожидалась %v", err, wantErr)
	}
	if _, err := store.ManagedUser(ctx, user.AccountingID); !errors.Is(err, ErrNotFound) {
		t.Errorf("ManagedUser() error = %v, ожидалась ErrNotFound", err)
	}
	if _, err := store.Operation(ctx, operation.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("Operation() error = %v, ожидалась ErrNotFound", err)
	}
}

func TestManagedUsersAndOperationJournal(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "state.db"))
	t.Cleanup(func() { _ = store.Close() })

	now := time.Date(2026, time.August, 14, 12, 30, 0, 0, time.UTC)
	first := testManagedUser("u.bbbbbbbbbbbbbbbbbbbb", now)
	second := testManagedUser("u.aaaaaaaaaaaaaaaaaaaa", now.Add(time.Second))
	second.DesiredPresent = false
	second.Applied = false
	digest := sha256.Sum256([]byte("ensure-present"))
	operation := Operation{
		ID:            "operation-2",
		Type:          OperationTypeEnsurePresent,
		RequestDigest: digest,
		CreatedAt:     now,
	}

	if err := store.Transaction(ctx, func(tx *Transaction) error {
		if err := tx.CreateOperation(ctx, operation); err != nil {
			return err
		}
		if err := tx.PutManagedUser(ctx, first); err != nil {
			return err
		}
		return tx.PutManagedUser(ctx, second)
	}); err != nil {
		t.Fatalf("Transaction() вернул ошибку: %v", err)
	}

	users, err := store.ManagedUsers(ctx)
	if err != nil {
		t.Fatalf("ManagedUsers() вернул ошибку: %v", err)
	}
	gotIDs := []string{users[0].AccountingID, users[1].AccountingID}
	wantIDs := []string{second.AccountingID, first.AccountingID}
	if !slices.Equal(gotIDs, wantIDs) {
		t.Errorf("порядок пользователей = %v, ожидался %v", gotIDs, wantIDs)
	}

	completedAt := now.Add(2 * time.Second)
	resultPayload := []byte{0x08, 0x01}
	if err := store.Transaction(ctx, func(tx *Transaction) error {
		return tx.CompleteOperation(ctx, operation.ID, resultPayload, completedAt)
	}); err != nil {
		t.Fatalf("CompleteOperation() вернул ошибку: %v", err)
	}

	gotOperation, err := store.Operation(ctx, operation.ID)
	if err != nil {
		t.Fatalf("Operation() вернул ошибку: %v", err)
	}
	if gotOperation.Status != OperationStatusCompleted {
		t.Errorf("operation status = %q, ожидался %q", gotOperation.Status, OperationStatusCompleted)
	}
	if !slices.Equal(gotOperation.Result, resultPayload) {
		t.Errorf("operation result = %x, ожидался %x", gotOperation.Result, resultPayload)
	}
	if !gotOperation.UpdatedAt.Equal(completedAt) {
		t.Errorf("operation updated_at = %v, ожидалось %v", gotOperation.UpdatedAt, completedAt)
	}

	err = store.Transaction(ctx, func(tx *Transaction) error {
		return tx.CreateOperation(ctx, operation)
	})
	if !errors.Is(err, ErrOperationExists) {
		t.Errorf("повторный CreateOperation() error = %v, ожидалась ErrOperationExists", err)
	}
	err = store.Transaction(ctx, func(tx *Transaction) error {
		return tx.CompleteOperation(ctx, operation.ID, resultPayload, completedAt)
	})
	if !errors.Is(err, ErrOperationNotPending) {
		t.Errorf("повторный CompleteOperation() error = %v, ожидалась ErrOperationNotPending", err)
	}
}

func TestUsageOutboxSequenceAndAcknowledgements(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "state.db")
	store := openTestStore(t, path)

	collectedAt := time.Date(2026, time.August, 14, 13, 0, 0, 123_000_000, time.UTC)
	created, err := store.AppendUsageBatches(ctx, collectedAt, [][]byte{{0x01}, {0x02}})
	if err != nil {
		t.Fatalf("AppendUsageBatches() вернул ошибку: %v", err)
	}
	if got := []uint64{created[0].Sequence, created[1].Sequence}; !slices.Equal(got, []uint64{1, 2}) {
		t.Fatalf("первые sequence = %v, ожидались [1 2]", got)
	}
	spoolID := created[0].SpoolID
	if spoolID == "" || created[1].SpoolID != spoolID {
		t.Fatalf("spool IDs = %q и %q, ожидалось одно непустое значение", spoolID, created[1].SpoolID)
	}
	stats, err := store.UsageOutboxStats(ctx)
	if err != nil {
		t.Fatalf("UsageOutboxStats() вернул ошибку: %v", err)
	}
	if stats.Batches != 2 || stats.PayloadBytes != 2 {
		t.Fatalf("UsageOutboxStats() = %+v, ожидалось 2 пачки и 2 байта", stats)
	}

	pending, err := store.PendingUsageBatches(ctx, 1)
	if err != nil {
		t.Fatalf("PendingUsageBatches() вернул ошибку: %v", err)
	}
	if len(pending) != 1 || pending[0].Sequence != 1 {
		t.Fatalf("первая выдача = %+v, ожидалась пачка sequence=1", pending)
	}

	assertAcknowledgement(t, store, "", 0, UsageAcknowledgementEmpty)
	assertAcknowledgement(t, store, "foreign-spool", 1, UsageAcknowledgementForeignSpool)
	assertAcknowledgement(t, store, "foreign-spool", ^uint64(0), UsageAcknowledgementForeignSpool)
	assertAcknowledgement(t, store, spoolID, 2, UsageAcknowledgementBeyondEmitted)
	assertPendingUsageCount(t, store, 2)

	assertAcknowledgement(t, store, spoolID, 1, UsageAcknowledgementApplied)
	assertPendingUsageCount(t, store, 1)
	stats, err = store.UsageOutboxStats(ctx)
	if err != nil {
		t.Fatalf("UsageOutboxStats() после подтверждения вернул ошибку: %v", err)
	}
	if stats.Batches != 1 || stats.PayloadBytes != 1 {
		t.Fatalf("UsageOutboxStats() после подтверждения = %+v", stats)
	}
	pending, err = store.PendingUsageBatches(ctx, 10)
	if err != nil {
		t.Fatalf("второй PendingUsageBatches() вернул ошибку: %v", err)
	}
	if len(pending) != 1 || pending[0].Sequence != 2 {
		t.Fatalf("вторая выдача = %+v, ожидалась пачка sequence=2", pending)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("Close() вернул ошибку: %v", err)
	}
	store = openTestStore(t, path)
	t.Cleanup(func() { _ = store.Close() })

	created, err = store.AppendUsageBatches(ctx, collectedAt.Add(time.Second), [][]byte{{0x03}})
	if err != nil {
		t.Fatalf("AppendUsageBatches() после открытия вернул ошибку: %v", err)
	}
	if len(created) != 1 || created[0].Sequence != 3 {
		t.Fatalf("sequence после открытия = %+v, ожидалась пачка sequence=3", created)
	}
	if created[0].SpoolID != spoolID {
		t.Errorf("spool ID после открытия = %q, ожидался %q", created[0].SpoolID, spoolID)
	}
}

func openTestStore(t *testing.T, path string) *Store {
	t.Helper()
	setTestDirectoryMode(t, filepath.Dir(path))
	store, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open(%q) вернул ошибку: %v", path, err)
	}
	return store
}

func setTestDirectoryMode(t *testing.T, path string) {
	t.Helper()
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatalf("задать права тестового каталога: %v", err)
	}
}

func testManagedUser(accountingID string, updatedAt time.Time) ManagedUser {
	return ManagedUser{
		AccountingID:   accountingID,
		CredentialUUID: "00000000-0000-4000-8000-000000000001",
		Flow:           "xtls-rprx-vision",
		EgressKey:      "bridge-de-1",
		DesiredPresent: true,
		Applied:        true,
		UpdatedAt:      updatedAt,
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(%q) вернул ошибку: %v", path, err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Errorf("права %q = %04o, ожидались %04o", path, got, want)
	}
}

func assertSidecarModes(t *testing.T, path string) {
	t.Helper()
	matches, err := filepath.Glob(path + "-*")
	if err != nil {
		t.Fatalf("найти служебные файлы SQLite: %v", err)
	}
	for _, match := range matches {
		assertMode(t, match, 0o600)
	}
}

func assertSQLitePragmas(t *testing.T, store *Store) {
	t.Helper()
	checks := []struct {
		query string
		want  string
	}{
		{"PRAGMA journal_mode", "wal"},
		{"PRAGMA foreign_keys", "1"},
		{"PRAGMA synchronous", "2"},
		{"PRAGMA busy_timeout", "5000"},
	}
	for _, check := range checks {
		var got string
		if err := store.db.QueryRow(check.query).Scan(&got); err != nil {
			t.Fatalf("%s: %v", check.query, err)
		}
		if got != check.want {
			t.Errorf("%s = %q, ожидалось %q", check.query, got, check.want)
		}
	}
}

func assertAcknowledgement(
	t *testing.T,
	store *Store,
	spoolID string,
	through uint64,
	want UsageAcknowledgement,
) {
	t.Helper()
	got, err := store.AcknowledgeUsageBatches(context.Background(), spoolID, through)
	if err != nil {
		t.Fatalf("AcknowledgeUsageBatches() вернул ошибку: %v", err)
	}
	if got != want {
		t.Errorf("AcknowledgeUsageBatches() = %q, ожидалось %q", got, want)
	}
}

func assertPendingUsageCount(t *testing.T, store *Store, want uint64) {
	t.Helper()
	got, err := store.PendingUsageBatchCount(context.Background())
	if err != nil {
		t.Fatalf("PendingUsageBatchCount() вернул ошибку: %v", err)
	}
	if got != want {
		t.Errorf("PendingUsageBatchCount() = %d, ожидалось %d", got, want)
	}
}
