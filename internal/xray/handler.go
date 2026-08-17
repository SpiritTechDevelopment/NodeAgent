package xray

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"
	proxymanCommand "github.com/xtls/xray-core/app/proxyman/command"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/proxy/vless"
)

const flowVision = "xtls-rprx-vision"

// AddUser добавляет VLESS-пользователя в настроенный inbound.
func (c *Client) AddUser(ctx context.Context, user User) error {
	if err := ValidateUser(user); err != nil {
		return err
	}

	account := serial.ToTypedMessage(&vless.Account{
		Id:         user.CredentialUUID,
		Flow:       user.Flow,
		Encryption: "none",
	})
	operation := serial.ToTypedMessage(&proxymanCommand.AddUserOperation{
		User: &protocol.User{
			Level:   0,
			Email:   user.AccountingID,
			Account: account,
		},
	})
	if _, err := c.handler.AlterInbound(ctx, &proxymanCommand.AlterInboundRequest{
		Tag:       c.inboundTag,
		Operation: operation,
	}); err != nil {
		return fmt.Errorf("add Xray VLESS user: %w", err)
	}
	return nil
}

// RemoveUser удаляет VLESS-пользователя из настроенного inbound по accounting_id.
func (c *Client) RemoveUser(ctx context.Context, accountingID string) error {
	if err := ValidateAccountingID(accountingID); err != nil {
		return err
	}
	operation := serial.ToTypedMessage(&proxymanCommand.RemoveUserOperation{
		Email: accountingID,
	})
	if _, err := c.handler.AlterInbound(ctx, &proxymanCommand.AlterInboundRequest{
		Tag:       c.inboundTag,
		Operation: operation,
	}); err != nil {
		return fmt.Errorf("remove Xray VLESS user: %w", err)
	}
	return nil
}

// User возвращает фактического VLESS-пользователя по accounting_id.
func (c *Client) User(ctx context.Context, accountingID string) (User, error) {
	if err := ValidateAccountingID(accountingID); err != nil {
		return User{}, err
	}
	response, err := c.handler.GetInboundUsers(ctx, &proxymanCommand.GetInboundUserRequest{
		Tag:   c.inboundTag,
		Email: accountingID,
	})
	if err != nil {
		return User{}, fmt.Errorf("get Xray VLESS user: %w", err)
	}
	if response == nil || len(response.GetUsers()) == 0 || response.GetUsers()[0] == nil ||
		response.GetUsers()[0].GetAccount() == nil {
		return User{}, ErrUserNotFound
	}
	user, err := decodeUser(response.GetUsers()[0])
	if err != nil {
		return User{}, err
	}
	return user, nil
}

// Users возвращает полный список фактических VLESS-пользователей inbound.
func (c *Client) Users(ctx context.Context) ([]User, error) {
	response, err := c.handler.GetInboundUsers(ctx, &proxymanCommand.GetInboundUserRequest{
		Tag: c.inboundTag,
	})
	if err != nil {
		return nil, fmt.Errorf("list Xray VLESS users: %w", err)
	}
	if response == nil {
		return nil, errors.New("list Xray VLESS users: empty response")
	}

	users := make([]User, 0, len(response.GetUsers()))
	for index, raw := range response.GetUsers() {
		if raw == nil {
			return nil, fmt.Errorf("decode Xray VLESS user %d: empty user", index)
		}
		user, err := decodeUser(raw)
		if err != nil {
			return nil, fmt.Errorf("decode Xray VLESS user %d: %w", index, err)
		}
		users = append(users, user)
	}
	slices.SortFunc(users, func(left, right User) int {
		return strings.Compare(left.AccountingID, right.AccountingID)
	})
	return users, nil
}

func decodeUser(raw *protocol.User) (User, error) {
	if raw.GetAccount() == nil {
		return User{}, errors.New("Xray VLESS user has no account")
	}
	instance, err := raw.GetAccount().GetInstance()
	if err != nil {
		return User{}, fmt.Errorf("decode Xray VLESS account: %w", err)
	}
	account, ok := instance.(*vless.Account)
	if !ok {
		return User{}, fmt.Errorf("unexpected Xray account type %q", raw.GetAccount().GetType())
	}
	return User{
		AccountingID:   raw.GetEmail(),
		CredentialUUID: account.GetId(),
		Flow:           account.GetFlow(),
	}, nil
}

// ValidateUser проверяет backend-owned VLESS-пользователя до обращения к Xray.
func ValidateUser(user User) error {
	if err := ValidateAccountingID(user.AccountingID); err != nil {
		return err
	}
	credential, err := uuid.Parse(user.CredentialUUID)
	if err != nil || credential == uuid.Nil || credential.String() != user.CredentialUUID {
		return errors.New("credential UUID must be a canonical non-zero UUID")
	}
	if user.Flow != "" && user.Flow != flowVision {
		return errors.New("VLESS flow must be empty or xtls-rprx-vision")
	}
	return nil
}
