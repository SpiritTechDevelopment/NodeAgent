package usage

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	nodeagentv1 "github.com/SpiritTechDevelopment/NodeAgent/internal/gen/spiritvpn/nodeagent/v1"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/state"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/xray"
)

// DecodeBatch восстанавливает wire-представление сохранённого usage batch.
func DecodeBatch(batch state.UsageBatch) (*nodeagentv1.UsageBatch, error) {
	if batch.SpoolID == "" {
		return nil, errors.New("usage batch spool ID is required")
	}
	if batch.Sequence == 0 {
		return nil, errors.New("usage batch sequence must be positive")
	}
	if batch.CollectedAt.IsZero() {
		return nil, errors.New("usage batch collection time is required")
	}
	if len(batch.Payload) == 0 {
		return nil, errors.New("usage batch payload is required")
	}

	message := new(nodeagentv1.UsageBatch)
	if err := proto.Unmarshal(batch.Payload, message); err != nil {
		return nil, fmt.Errorf("decode usage batch payload: %w", err)
	}
	if message.GetCursor() != nil || message.GetCollectedAtUnixMs() != 0 {
		return nil, errors.New("usage batch payload contains transport metadata")
	}
	if err := validateProtoItems(message.GetItems()); err != nil {
		return nil, err
	}
	message.Cursor = &nodeagentv1.UsageCursor{
		SpoolId:  batch.SpoolID,
		Sequence: batch.Sequence,
	}
	message.CollectedAtUnixMs = batch.CollectedAt.UnixMilli()
	return message, nil
}

func validateProtoItems(items []*nodeagentv1.UserUsage) error {
	if len(items) == 0 {
		return errors.New("usage batch contains no items")
	}
	if len(items) > maxItemsPerBatch {
		return errors.New("usage batch contains too many items")
	}
	seen := make(map[string]struct{}, len(items))
	for index, item := range items {
		if item == nil {
			return fmt.Errorf("usage batch item %d is empty", index)
		}
		if err := xray.ValidateAccountingID(item.GetAccountingId()); err != nil {
			return fmt.Errorf("validate usage batch item %d: %w", index, err)
		}
		if item.GetUplinkBytes() == 0 && item.GetDownlinkBytes() == 0 {
			return fmt.Errorf("usage batch item %d has no traffic", index)
		}
		if _, exists := seen[item.GetAccountingId()]; exists {
			return errors.New("usage batch contains duplicate accounting ID")
		}
		seen[item.GetAccountingId()] = struct{}{}
	}
	return nil
}
