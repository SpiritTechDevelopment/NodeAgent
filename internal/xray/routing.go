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

// AddUserRule добавляет персональное правило в начало списка правил Xray, чтобы
// оно всегда находилось перед завершающим default-deny для клиентского inbound.
func (c *Client) AddUserRule(
	ctx context.Context,
	accountingID string,
	outboundTag string,
) error {
	ruleTag, err := UserRuleTag(accountingID)
	if err != nil {
		return err
	}
	if err := ValidateOutboundTag(outboundTag); err != nil {
		return err
	}

	configuration := serial.ToTypedMessage(&router.Config{
		Rule: []*router.RoutingRule{
			{
				TargetTag: &router.RoutingRule_Tag{Tag: outboundTag},
				RuleTag:   ruleTag,
				UserEmail: []string{accountingID},
			},
		},
	})
	if _, err := c.routing.AddRule(ctx, &routerCommand.AddRuleRequest{
		Config:       configuration,
		ShouldAppend: false,
	}); err != nil {
		return fmt.Errorf("add Xray user routing rule: %w", err)
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
