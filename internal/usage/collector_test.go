package usage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	nodeagentv1 "github.com/SpiritTechDevelopment/NodeAgent/internal/gen/spiritvpn/nodeagent/v1"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/state"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/xray"
)

const testAccountingID = "u.abcdefghijklmnopqrst"

func TestCollectPersistsSortedPayloadStartingAtSequenceOne(t *testing.T) {
	store := openUsageStore(t)
	secondID := "u.bcdefghijklmnopqrstu"
	source := &fakeSource{usage: []xray.Usage{
		{AccountingID: secondID, DownlinkBytes: 20},
		{AccountingID: testAccountingID, UplinkBytes: 10},
	}}
	collector := newTestCollector(t, store, source)
	collectedAt := time.Date(2026, time.August, 14, 15, 0, 0, 123_000_000, time.UTC)
	collector.now = func() time.Time { return collectedAt }

	batches, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect() вернул ошибку: %v", err)
	}
	if source.bulkCalls != 1 || source.userCalls != 0 {
		t.Fatalf("вызовы source: bulk=%d user=%d", source.bulkCalls, source.userCalls)
	}
	if len(batches) != 1 || batches[0].Sequence != 1 || batches[0].SpoolID == "" ||
		!batches[0].CollectedAt.Equal(collectedAt) {
		t.Fatalf("созданный batch = %+v", batches)
	}
	items := decodeUsagePayload(t, batches[0].Payload)
	wantIDs := []string{testAccountingID, secondID}
	gotIDs := []string{items[0].GetAccountingId(), items[1].GetAccountingId()}
	if !slices.Equal(gotIDs, wantIDs) {
		t.Fatalf("accounting IDs = %v, ожидались %v", gotIDs, wantIDs)
	}
	if items[0].GetUplinkBytes() != 10 || items[1].GetDownlinkBytes() != 20 {
		t.Fatalf("items = %+v", items)
	}
}

func TestCollectChunksOneResetAtomically(t *testing.T) {
	store := openUsageStore(t)
	items := make([]xray.Usage, 0, 5)
	for index := 0; index < 5; index++ {
		items = append(items, xray.Usage{
			AccountingID: fmt.Sprintf("u.aaaaaaaaaaaaaaaaaaa%d", index+2),
			UplinkBytes:  uint64(index + 1),
		})
	}
	source := &fakeSource{usage: items}
	collector := newTestCollector(t, store, source)
	collector.maxItems = 2

	batches, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect() вернул ошибку: %v", err)
	}
	if len(batches) != 3 {
		t.Fatalf("batch count = %d, ожидалось 3", len(batches))
	}
	for index, batch := range batches {
		if batch.Sequence != uint64(index+1) {
			t.Errorf("batch %d sequence = %d", index, batch.Sequence)
		}
		if got := len(decodeUsagePayload(t, batch.Payload)); got > 2 {
			t.Errorf("batch %d содержит %d элементов", index, got)
		}
	}
}

func TestCollectSkipsEmptyResetWithoutAdvancingSequence(t *testing.T) {
	store := openUsageStore(t)
	collector := newTestCollector(t, store, &fakeSource{})

	batches, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect() вернул ошибку: %v", err)
	}
	if len(batches) != 0 {
		t.Fatalf("пустой сброс создал batch: %+v", batches)
	}
	metadata, err := store.Metadata(context.Background())
	if err != nil {
		t.Fatalf("Metadata() вернул ошибку: %v", err)
	}
	if metadata.UsageSequence != 0 {
		t.Fatalf("usage sequence = %d, ожидался 0", metadata.UsageSequence)
	}
}

func TestFinalizeUserUsesPointResetAndPersistsDelta(t *testing.T) {
	store := openUsageStore(t)
	source := &fakeSource{userUsage: []xray.Usage{
		{AccountingID: testAccountingID, UplinkBytes: 7, DownlinkBytes: 9},
	}}
	collector := newTestCollector(t, store, source)

	if err := collector.FinalizeUser(context.Background(), testAccountingID); err != nil {
		t.Fatalf("FinalizeUser() вернул ошибку: %v", err)
	}
	if source.userCalls != 1 || source.lastAccountingID != testAccountingID || source.bulkCalls != 0 {
		t.Fatalf("вызовы source: %+v", source)
	}
	pending, err := store.PendingUsageBatches(context.Background(), 10)
	if err != nil {
		t.Fatalf("PendingUsageBatches() вернул ошибку: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending = %+v", pending)
	}
	items := decodeUsagePayload(t, pending[0].Payload)
	if len(items) != 1 || items[0].GetUplinkBytes() != 7 || items[0].GetDownlinkBytes() != 9 {
		t.Fatalf("items = %+v", items)
	}
}

func TestCollectorChecksWritableStateBeforeReset(t *testing.T) {
	store := openUsageStore(t)
	source := &fakeSource{usage: []xray.Usage{{AccountingID: testAccountingID, UplinkBytes: 1}}}
	collector := newTestCollector(t, store, source)
	if err := store.Close(); err != nil {
		t.Fatalf("Close() вернул ошибку: %v", err)
	}

	if _, err := collector.Collect(context.Background()); err == nil {
		t.Fatal("Collect() не вернул ошибку закрытой SQLite")
	}
	if source.bulkCalls != 0 {
		t.Fatalf("счётчики сброшены при недоступном outbox: %d", source.bulkCalls)
	}
}

func TestCollectorSerializesBulkAndFinalReset(t *testing.T) {
	store := openUsageStore(t)
	bulkEntered := make(chan struct{})
	releaseBulk := make(chan struct{})
	userEntered := make(chan struct{})
	source := &blockingSource{
		bulkEntered: bulkEntered,
		releaseBulk: releaseBulk,
		userEntered: userEntered,
	}
	collector := newTestCollector(t, store, source)

	bulkDone := make(chan error, 1)
	go func() {
		_, err := collector.Collect(context.Background())
		bulkDone <- err
	}()
	<-bulkEntered

	userDone := make(chan error, 1)
	go func() {
		userDone <- collector.FinalizeUser(context.Background(), testAccountingID)
	}()
	select {
	case <-userEntered:
		t.Fatal("финальный сброс начался до завершения bulk-сброса")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseBulk)
	if err := <-bulkDone; err != nil {
		t.Fatalf("Collect() вернул ошибку: %v", err)
	}
	select {
	case <-userEntered:
	case <-time.After(time.Second):
		t.Fatal("финальный сброс не начался после bulk-сброса")
	}
	if err := <-userDone; err != nil {
		t.Fatalf("FinalizeUser() вернул ошибку: %v", err)
	}
}

func TestCollectorRejectsInvalidItems(t *testing.T) {
	tests := []struct {
		name  string
		items []xray.Usage
	}{
		{name: "invalid ID", items: []xray.Usage{{AccountingID: "invalid", UplinkBytes: 1}}},
		{name: "zero traffic", items: []xray.Usage{{AccountingID: testAccountingID}}},
		{
			name: "duplicate",
			items: []xray.Usage{
				{AccountingID: testAccountingID, UplinkBytes: 1},
				{AccountingID: testAccountingID, DownlinkBytes: 1},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := openUsageStore(t)
			collector := newTestCollector(t, store, &fakeSource{usage: test.items})
			if _, err := collector.Collect(context.Background()); err == nil {
				t.Fatal("Collect() не отклонил некорректные элементы")
			}
			count, err := store.PendingUsageBatchCount(context.Background())
			if err != nil || count != 0 {
				t.Fatalf("pending count = %d, error = %v", count, err)
			}
		})
	}
}

func TestNewValidatesDependencies(t *testing.T) {
	store := openUsageStore(t)
	if _, err := New(nil, &fakeSource{}); err == nil {
		t.Error("New() не отклонил nil state")
	}
	if _, err := New(store, nil); err == nil {
		t.Error("New() не отклонил nil source")
	}
}

func openUsageStore(t *testing.T) *state.Store {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatalf("задать права каталога: %v", err)
	}
	store, err := state.Open(context.Background(), filepath.Join(directory, "state.db"))
	if err != nil {
		t.Fatalf("открыть SQLite: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func newTestCollector(t *testing.T, store *state.Store, source Source) *Collector {
	t.Helper()
	collector, err := New(store, source)
	if err != nil {
		t.Fatalf("New() вернул ошибку: %v", err)
	}
	return collector
}

func decodeUsagePayload(t *testing.T, payload []byte) []*nodeagentv1.UserUsage {
	t.Helper()
	message := new(nodeagentv1.UsageBatch)
	if err := proto.Unmarshal(payload, message); err != nil {
		t.Fatalf("декодировать payload: %v", err)
	}
	if message.GetCursor() != nil || message.GetCollectedAtUnixMs() != 0 {
		t.Fatalf("payload содержит transport metadata: %+v", message)
	}
	return message.GetItems()
}

type fakeSource struct {
	usage            []xray.Usage
	userUsage        []xray.Usage
	err              error
	bulkCalls        int
	userCalls        int
	lastAccountingID string
}

func (source *fakeSource) ResetUsage(context.Context) ([]xray.Usage, error) {
	source.bulkCalls++
	return append([]xray.Usage(nil), source.usage...), source.err
}

func (source *fakeSource) ResetUserUsage(
	_ context.Context,
	accountingID string,
) ([]xray.Usage, error) {
	source.userCalls++
	source.lastAccountingID = accountingID
	return append([]xray.Usage(nil), source.userUsage...), source.err
}

type blockingSource struct {
	bulkEntered chan<- struct{}
	releaseBulk <-chan struct{}
	userEntered chan<- struct{}
}

func (source *blockingSource) ResetUsage(context.Context) ([]xray.Usage, error) {
	close(source.bulkEntered)
	<-source.releaseBulk
	return nil, nil
}

func (source *blockingSource) ResetUserUsage(context.Context, string) ([]xray.Usage, error) {
	close(source.userEntered)
	return nil, nil
}
