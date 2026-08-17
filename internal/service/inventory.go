package service

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	nodeagentv1 "github.com/SpiritTechDevelopment/NodeAgent/internal/gen/spiritvpn/nodeagent/v1"
	"github.com/SpiritTechDevelopment/NodeAgent/internal/xray"
)

const (
	defaultFallbackOutboundTag  = "block"
	defaultMaxInventoryUsers    = 2000
	inventoryUnavailableMessage = "Xray inventory is unavailable"
	inventoryTooLargeMessage    = "Xray inventory exceeds the configured limit"
	invalidInventoryMessage     = "Xray inventory is inconsistent"
)

type observedInventory struct {
	users      []*nodeagentv1.ActualUser
	observedAt time.Time
}

func (s *Service) observeInventory(ctx context.Context) (observedInventory, error) {
	s.mutations.Lock()
	defer s.mutations.Unlock()

	users, err := s.xray.Users(ctx)
	if err != nil {
		return observedInventory{}, status.Error(codes.Unavailable, inventoryUnavailableMessage)
	}
	if len(users) > s.maxInventoryUsers {
		return observedInventory{}, status.Error(codes.ResourceExhausted, inventoryTooLargeMessage)
	}
	rules, err := s.xray.UserRules(ctx)
	if err != nil {
		return observedInventory{}, status.Error(codes.Unavailable, inventoryUnavailableMessage)
	}
	rulesByUser := make(map[string]xray.UserRule, len(rules))
	for _, rule := range rules {
		if _, duplicate := rulesByUser[rule.AccountingID]; duplicate {
			return observedInventory{}, status.Error(codes.Internal, invalidInventoryMessage)
		}
		rulesByUser[rule.AccountingID] = rule
	}

	actualUsers := make([]*nodeagentv1.ActualUser, 0, len(users))
	seenUsers := make(map[string]struct{}, len(users))
	for _, user := range users {
		if _, duplicate := seenUsers[user.AccountingID]; duplicate {
			return observedInventory{}, status.Error(codes.Internal, invalidInventoryMessage)
		}
		seenUsers[user.AccountingID] = struct{}{}

		backendManaged := xray.ValidateAccountingID(user.AccountingID) == nil
		outboundTag := s.fallbackOutboundTag
		if backendManaged {
			if _, hasRule := rulesByUser[user.AccountingID]; hasRule {
				outboundTag, err = s.xray.TestUserRoute(ctx, user.AccountingID)
				switch {
				case err == nil:
				case errors.Is(err, xray.ErrRouteNotFound):
					outboundTag = s.fallbackOutboundTag
				default:
					return observedInventory{}, status.Error(codes.Unavailable, inventoryUnavailableMessage)
				}
			}
		}
		egressKey := outboundTag
		if outboundTag == s.localOutboundTag {
			egressKey = ""
		}
		actualUsers = append(actualUsers, &nodeagentv1.ActualUser{
			User: &nodeagentv1.User{
				AccountingId:   user.AccountingID,
				CredentialUuid: user.CredentialUUID,
				Flow:           user.Flow,
				EgressKey:      egressKey,
			},
			BackendManaged: backendManaged,
		})
	}
	return observedInventory{users: actualUsers, observedAt: s.now().UTC()}, nil
}
