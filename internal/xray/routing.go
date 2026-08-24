package xray

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/xtls/xray-core/app/router"
	routerCommand "github.com/xtls/xray-core/app/router/command"
	"github.com/xtls/xray-core/common/serial"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const userRuleTagPrefix = "spirit-agent:user:"

var accountingIDPattern = regexp.MustCompile(`^u\.[a-z2-7]{20}$`)

// UserRuleTag возвращает детерминированный Xray rule_tag пользователя.
func UserRuleTag(accountingID string) (string, error) {
	if err := ValidateAccountingID(accountingID); err != nil {
		return "", err
	}
	return userRuleTagPrefix + accountingID, nil
}

// RoutingTable содержит полную таблицу маршрутизации, которую агент хочет видеть
// в Xray: правила инфраструктуры, персональные правила пользователей и
// завершающий default-deny — строго в порядке приоритета.
type RoutingTable struct {
	config *router.Config
}

// Empty сообщает, что таблица не содержит конфигурации.
func (table RoutingTable) Empty() bool {
	return table.config == nil
}

// UserRules возвращает персональные правила, которые установит таблица, в
// стабильном порядке. Правила инфраструктуры и default-deny не возвращаются:
// агент ими не владеет и только переносит их без изменений.
func (table RoutingTable) UserRules() []UserRule {
	if table.config == nil {
		return nil
	}
	rules := make([]UserRule, 0, len(table.config.GetRule()))
	for _, item := range table.config.GetRule() {
		accountingID, managed := strings.CutPrefix(item.GetRuleTag(), userRuleTagPrefix)
		if !managed || !accountingIDPattern.MatchString(accountingID) {
			continue
		}
		rules = append(rules, UserRule{
			AccountingID: accountingID,
			OutboundTag:  item.GetTag(),
			RuleTag:      item.GetRuleTag(),
		})
	}
	slices.SortFunc(rules, func(left, right UserRule) int {
		return strings.Compare(left.AccountingID, right.AccountingID)
	})
	return rules
}

// RoutingTableForUsers собирает таблицу только из персональных правил.
//
// Это вспомогательный конструктор для вызывающих без файла конфигурации;
// боевой путь — ConfigFile.DesiredRouting, и только он сохраняет правила
// инфраструктуры и завершающий default-deny.
func RoutingTableForUsers(users []PersistentUser) (RoutingTable, error) {
	configuration := &router.Config{Rule: make([]*router.RoutingRule, 0, len(users))}
	for _, item := range users {
		ruleTag, err := UserRuleTag(item.User.AccountingID)
		if err != nil {
			return RoutingTable{}, err
		}
		if err := ValidateOutboundTag(item.OutboundTag); err != nil {
			return RoutingTable{}, err
		}
		configuration.Rule = append(configuration.Rule, &router.RoutingRule{
			TargetTag: &router.RoutingRule_Tag{Tag: item.OutboundTag},
			RuleTag:   ruleTag,
			UserEmail: []string{item.User.AccountingID},
		})
	}
	return RoutingTable{config: configuration}, nil
}

// ApplyRouting устанавливает таблицу маршрутизации целиком за один вызов.
//
// ShouldAppend=false означает не «вставить в начало», а «заменить всю routing
// configuration»: Xray очищает и правила, и балансировщики, после чего
// добавляет только присланные (app/router.Router.ReloadRules; ср. флаг
// `-append` у `xray api adrules`: "Append to the existing configuration instead
// of replacing it. Default false"). Поэтому вызывающая сторона обязана
// передавать каждое правило, которое должно остаться в роутере, — передача
// одного правила уничтожает все остальные, включая default-deny.
func (c *Client) ApplyRouting(ctx context.Context, table RoutingTable) error {
	if table.Empty() {
		return errors.New("Xray routing table is required")
	}
	if _, err := c.routing.AddRule(ctx, &routerCommand.AddRuleRequest{
		Config:       serial.ToTypedMessage(table.config),
		ShouldAppend: false,
	}); err != nil {
		return fmt.Errorf("apply Xray routing table: %w", err)
	}
	return nil
}

// RemoveUserRule удаляет только персональное правило указанного пользователя.
func (c *Client) RemoveUserRule(ctx context.Context, accountingID string) error {
	ruleTag, err := UserRuleTag(accountingID)
	if err != nil {
		return err
	}
	if _, err := c.routing.RemoveRule(ctx, &routerCommand.RemoveRuleRequest{
		RuleTag: ruleTag,
	}); err != nil {
		return fmt.Errorf("remove Xray user routing rule: %w", err)
	}
	return nil
}

// UserRules возвращает только правила, которыми владеет агент, в стабильном порядке.
func (c *Client) UserRules(ctx context.Context) ([]UserRule, error) {
	response, err := c.routing.ListRule(ctx, &routerCommand.ListRuleRequest{})
	if err != nil {
		return nil, fmt.Errorf("list Xray routing rules: %w", err)
	}
	if response == nil {
		return nil, errors.New("list Xray routing rules: empty response")
	}

	rules := make([]UserRule, 0, len(response.GetRules()))
	for _, item := range response.GetRules() {
		if item == nil {
			continue
		}
		accountingID, managed := strings.CutPrefix(item.GetRuleTag(), userRuleTagPrefix)
		if !managed || !accountingIDPattern.MatchString(accountingID) {
			continue
		}
		rules = append(rules, UserRule{
			AccountingID: accountingID,
			OutboundTag:  item.GetTag(),
			RuleTag:      item.GetRuleTag(),
		})
	}
	slices.SortFunc(rules, func(left, right UserRule) int {
		return strings.Compare(left.AccountingID, right.AccountingID)
	})
	return rules, nil
}

// TestUserRoute возвращает outbound, который Xray фактически выбирает для пользователя.
func (c *Client) TestUserRoute(ctx context.Context, accountingID string) (string, error) {
	if err := ValidateAccountingID(accountingID); err != nil {
		return "", err
	}
	response, err := c.routing.TestRoute(ctx, &routerCommand.TestRouteRequest{
		RoutingContext: &routerCommand.RoutingContext{User: accountingID},
		FieldSelectors: []string{"outbound"},
	})
	if err != nil {
		if status.Code(err) == codes.Unknown && strings.Contains(
			status.Convert(err).Message(),
			"not enough information for making a decision",
		) {
			return "", ErrRouteNotFound
		}
		return "", fmt.Errorf("test Xray user route: %w", err)
	}
	if response == nil || response.GetOutboundTag() == "" {
		return "", errors.New("test Xray user route: empty outbound tag")
	}
	return response.GetOutboundTag(), nil
}

// ValidateAccountingID проверяет namespace и формат идентификатора backend.
func ValidateAccountingID(accountingID string) error {
	if !accountingIDPattern.MatchString(accountingID) {
		return errors.New("accounting ID must match u.[a-z2-7]{20}")
	}
	return nil
}

// ValidateOutboundTag проверяет непустой Xray outbound tag без внешних пробелов.
func ValidateOutboundTag(outboundTag string) error {
	if strings.TrimSpace(outboundTag) == "" {
		return errors.New("Xray outbound tag is required")
	}
	if strings.TrimSpace(outboundTag) != outboundTag {
		return errors.New("Xray outbound tag must not contain surrounding whitespace")
	}
	return nil
}
