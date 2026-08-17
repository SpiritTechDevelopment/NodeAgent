package usage

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	nodeagentv1 "github.com/SpiritTechDevelopment/NodeAgent/internal/gen/spiritvpn/nodeagent/v1"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/state"
)

func TestDecodeBatchRestoresTransportMetadata(t *testing.T) {
	collectedAt := time.Date(2026, time.August, 14, 18, 0, 0, 123_000_000, time.UTC)
	payload := marshalUsageMessage(t, &nodeagentv1.UsageBatch{Items: []*nodeagentv1.UserUsage{
		{AccountingId: testAccountingID, UplinkBytes: 5, DownlinkBytes: 7},
	}})

	message, err := DecodeBatch(state.UsageBatch{
		SpoolID:     "spool-test",
		Sequence:    3,
		CollectedAt: collectedAt,
		Payload:     payload,
	})
	if err != nil {
		t.Fatalf("DecodeBatch() вернул ошибку: %v", err)
	}
	if message.GetCursor().GetSpoolId() != "spool-test" || message.GetCursor().GetSequence() != 3 {
		t.Fatalf("cursor = %+v", message.GetCursor())
	}
	if message.GetCollectedAtUnixMs() != collectedAt.UnixMilli() {
		t.Fatalf("collected_at = %d", message.GetCollectedAtUnixMs())
	}
	if len(message.GetItems()) != 1 || message.GetItems()[0].GetDownlinkBytes() != 7 {
		t.Fatalf("items = %+v", message.GetItems())
	}
}

func TestDecodeBatchRejectsInvalidEnvelope(t *testing.T) {
	validPayload := marshalUsageMessage(t, &nodeagentv1.UsageBatch{Items: []*nodeagentv1.UserUsage{
		{AccountingId: testAccountingID, UplinkBytes: 1},
	}})
	valid := state.UsageBatch{
		SpoolID:     "spool-test",
		Sequence:    1,
		CollectedAt: time.Now(),
		Payload:     validPayload,
	}
	tests := []struct {
		name   string
		modify func(*state.UsageBatch)
	}{
		{name: "empty spool", modify: func(batch *state.UsageBatch) { batch.SpoolID = "" }},
		{name: "zero sequence", modify: func(batch *state.UsageBatch) { batch.Sequence = 0 }},
		{name: "zero time", modify: func(batch *state.UsageBatch) { batch.CollectedAt = time.Time{} }},
		{name: "empty payload", modify: func(batch *state.UsageBatch) { batch.Payload = nil }},
		{name: "malformed payload", modify: func(batch *state.UsageBatch) { batch.Payload = []byte{0xff} }},
		{
			name: "transport metadata in payload",
			modify: func(batch *state.UsageBatch) {
				batch.Payload = marshalUsageMessage(t, &nodeagentv1.UsageBatch{
					Cursor: &nodeagentv1.UsageCursor{SpoolId: "nested", Sequence: 1},
					Items: []*nodeagentv1.UserUsage{
						{AccountingId: testAccountingID, UplinkBytes: 1},
					},
				})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			batch := valid
			test.modify(&batch)
			if _, err := DecodeBatch(batch); err == nil {
				t.Fatal("DecodeBatch() не отклонил некорректный envelope")
			}
		})
	}
}

func TestDecodeBatchRejectsInvalidItems(t *testing.T) {
	tooMany := make([]*nodeagentv1.UserUsage, maxItemsPerBatch+1)
	for index := range tooMany {
		tooMany[index] = &nodeagentv1.UserUsage{
			AccountingId: testAccountingID,
			UplinkBytes:  1,
		}
	}
	tests := []struct {
		name  string
		items []*nodeagentv1.UserUsage
	}{
		{name: "empty", items: nil},
		{name: "too many", items: tooMany},
		{name: "nil item", items: []*nodeagentv1.UserUsage{nil}},
		{
			name:  "invalid ID",
			items: []*nodeagentv1.UserUsage{{AccountingId: "invalid", UplinkBytes: 1}},
		},
		{
			name:  "zero traffic",
			items: []*nodeagentv1.UserUsage{{AccountingId: testAccountingID}},
		},
		{
			name: "duplicate",
			items: []*nodeagentv1.UserUsage{
				{AccountingId: testAccountingID, UplinkBytes: 1},
				{AccountingId: testAccountingID, DownlinkBytes: 1},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			batch := state.UsageBatch{
				SpoolID:     "spool-test",
				Sequence:    1,
				CollectedAt: time.Now(),
				Payload: marshalUsageMessage(t, &nodeagentv1.UsageBatch{
					Items: test.items,
				}),
			}
			if _, err := DecodeBatch(batch); err == nil {
				t.Fatal("DecodeBatch() не отклонил некорректные items")
			}
		})
	}
}

func marshalUsageMessage(t *testing.T, message *nodeagentv1.UsageBatch) []byte {
	t.Helper()
	payload, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("кодировать UsageBatch: %v", err)
	}
	return payload
}
