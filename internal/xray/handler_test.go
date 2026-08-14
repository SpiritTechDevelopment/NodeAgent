package xray

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"

	proxymanCommand "github.com/xtls/xray-core/app/proxyman/command"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/proxy/vless"
	"google.golang.org/grpc"
)

const testCredentialUUID = "11111111-1111-4111-8111-111111111111"

func TestAddUserBuildsVLESSOperation(t *testing.T) {
	for _, flow := range []string{"", flowVision} {
		t.Run("flow="+flow, func(t *testing.T) {
			handler := &fakeHandlerClient{}
			client := newHandlerTestClient(handler)
			user := User{
				AccountingID:   testAccountingID,
				CredentialUUID: testCredentialUUID,
				Flow:           flow,
			}

			if err := client.AddUser(context.Background(), user); err != nil {
				t.Fatalf("AddUser() вернул ошибку: %v", err)
			}
			request := handler.alterRequest
			if request.GetTag() != "vless-in" {
				t.Errorf("inbound tag = %q, ожидался vless-in", request.GetTag())
			}
			if got := request.GetOperation().GetType(); got != "xray.app.proxyman.command.AddUserOperation" {
				t.Fatalf("тип операции = %q", got)
			}

			instance, err := request.GetOperation().GetInstance()
			if err != nil {
				t.Fatalf("декодировать AddUserOperation: %v", err)
			}
			operation, ok := instance.(*proxymanCommand.AddUserOperation)
			if !ok {
				t.Fatalf("тип операции = %T", instance)
			}
			if operation.GetUser().GetEmail() != testAccountingID {
				t.Errorf("email = %q, ожидался %q", operation.GetUser().GetEmail(), testAccountingID)
			}
			if operation.GetUser().GetLevel() != 0 {
				t.Errorf("level = %d, ожидался 0", operation.GetUser().GetLevel())
			}

			accountInstance, err := operation.GetUser().GetAccount().GetInstance()
			if err != nil {
				t.Fatalf("декодировать VLESS account: %v", err)
			}
			account, ok := accountInstance.(*vless.Account)
			if !ok {
				t.Fatalf("тип account = %T", accountInstance)
			}
			if account.GetId() != testCredentialUUID {
				t.Errorf("UUID = %q, ожидался %q", account.GetId(), testCredentialUUID)
			}
			if account.GetFlow() != flow {
				t.Errorf("flow = %q, ожидался %q", account.GetFlow(), flow)
			}
			if account.GetEncryption() != "none" {
				t.Errorf("encryption = %q, ожидался none", account.GetEncryption())
			}
		})
	}
}

func TestRemoveUserBuildsOperation(t *testing.T) {
	handler := &fakeHandlerClient{}
	client := newHandlerTestClient(handler)

	if err := client.RemoveUser(context.Background(), testAccountingID); err != nil {
		t.Fatalf("RemoveUser() вернул ошибку: %v", err)
	}
	request := handler.alterRequest
	if request.GetTag() != "vless-in" {
		t.Errorf("inbound tag = %q, ожидался vless-in", request.GetTag())
	}
	instance, err := request.GetOperation().GetInstance()
	if err != nil {
		t.Fatalf("декодировать RemoveUserOperation: %v", err)
	}
	operation, ok := instance.(*proxymanCommand.RemoveUserOperation)
	if !ok {
		t.Fatalf("тип операции = %T", instance)
	}
	if operation.GetEmail() != testAccountingID {
		t.Errorf("email = %q, ожидался %q", operation.GetEmail(), testAccountingID)
	}
}

func TestUserReadsAndDecodesVLESSAccount(t *testing.T) {
	handler := &fakeHandlerClient{
		usersResponse: &proxymanCommand.GetInboundUserResponse{
			Users: []*protocol.User{testProtocolUser(testAccountingID, testCredentialUUID, flowVision)},
		},
	}
	client := newHandlerTestClient(handler)

	user, err := client.User(context.Background(), testAccountingID)
	if err != nil {
		t.Fatalf("User() вернул ошибку: %v", err)
	}
	if user.AccountingID != testAccountingID ||
		user.CredentialUUID != testCredentialUUID ||
		user.Flow != flowVision {
		t.Errorf("user = %+v", user)
	}
	if handler.usersRequest.GetTag() != "vless-in" || handler.usersRequest.GetEmail() != testAccountingID {
		t.Errorf("GetInboundUsers request = %+v", handler.usersRequest)
	}
}

func TestUserReturnsNotFound(t *testing.T) {
	for _, response := range []*proxymanCommand.GetInboundUserResponse{
		nil,
		{},
		{Users: []*protocol.User{nil}},
		{Users: []*protocol.User{{Email: testAccountingID}}},
	} {
		handler := &fakeHandlerClient{usersResponse: response}
		client := newHandlerTestClient(handler)
		if _, err := client.User(context.Background(), testAccountingID); !errors.Is(err, ErrUserNotFound) {
			t.Errorf("User() error = %v, ожидалась ErrUserNotFound", err)
		}
	}
}

func TestUsersReturnsCompleteSortedInventory(t *testing.T) {
	secondID := "svc-monitoring"
	handler := &fakeHandlerClient{
		usersResponse: &proxymanCommand.GetInboundUserResponse{
			Users: []*protocol.User{
				testProtocolUser(testAccountingID, testCredentialUUID, flowVision),
				testProtocolUser(secondID, "22222222-2222-4222-8222-222222222222", ""),
			},
		},
	}
	client := newHandlerTestClient(handler)

	users, err := client.Users(context.Background())
	if err != nil {
		t.Fatalf("Users() вернул ошибку: %v", err)
	}
	gotIDs := []string{users[0].AccountingID, users[1].AccountingID}
	wantIDs := []string{secondID, testAccountingID}
	if !slices.Equal(gotIDs, wantIDs) {
		t.Errorf("порядок пользователей = %v, ожидался %v", gotIDs, wantIDs)
	}
	if handler.usersRequest.GetEmail() != "" {
		t.Errorf("email полного запроса = %q, ожидался пустой", handler.usersRequest.GetEmail())
	}
}

func TestHandlerMethodsRejectInvalidDataBeforeRPC(t *testing.T) {
	handler := &fakeHandlerClient{}
	client := newHandlerTestClient(handler)
	ctx := context.Background()

	invalidUsers := []User{
		{AccountingID: "invalid", CredentialUUID: testCredentialUUID},
		{AccountingID: testAccountingID, CredentialUUID: "invalid"},
		{AccountingID: testAccountingID, CredentialUUID: "00000000-0000-0000-0000-000000000000"},
		{AccountingID: testAccountingID, CredentialUUID: testCredentialUUID, Flow: "invalid"},
	}
	for _, user := range invalidUsers {
		if err := client.AddUser(ctx, user); err == nil {
			t.Errorf("AddUser() не отклонил %+v", user)
		}
	}
	if err := client.RemoveUser(ctx, "invalid"); err == nil {
		t.Error("RemoveUser() не отклонил accounting_id")
	}
	if _, err := client.User(ctx, "invalid"); err == nil {
		t.Error("User() не отклонил accounting_id")
	}
	if handler.calls != 0 {
		t.Errorf("HandlerService вызван %d раз для невалидных данных", handler.calls)
	}
}

func TestHandlerMethodsWrapServiceAndDecodeErrors(t *testing.T) {
	wantErr := errors.New("handler unavailable")
	handler := &fakeHandlerClient{err: wantErr}
	client := newHandlerTestClient(handler)
	ctx := context.Background()
	user := User{AccountingID: testAccountingID, CredentialUUID: testCredentialUUID}

	if err := client.AddUser(ctx, user); !errors.Is(err, wantErr) {
		t.Errorf("AddUser() error = %v, ожидалась исходная ошибка", err)
	}
	if err := client.RemoveUser(ctx, testAccountingID); !errors.Is(err, wantErr) {
		t.Errorf("RemoveUser() error = %v, ожидалась исходная ошибка", err)
	}
	if _, err := client.User(ctx, testAccountingID); !errors.Is(err, wantErr) {
		t.Errorf("User() error = %v, ожидалась исходная ошибка", err)
	}
	if _, err := client.Users(ctx); !errors.Is(err, wantErr) {
		t.Errorf("Users() error = %v, ожидалась исходная ошибка", err)
	}

	handler.err = nil
	handler.usersResponse = nil
	if _, err := client.Users(ctx); err == nil {
		t.Error("Users() не отклонил пустой ответ")
	}
	handler.usersResponse = &proxymanCommand.GetInboundUserResponse{
		Users: []*protocol.User{nil},
	}
	if _, err := client.Users(ctx); err == nil {
		t.Error("Users() не отклонил пустого пользователя")
	}
	handler.usersResponse = &proxymanCommand.GetInboundUserResponse{
		Users: []*protocol.User{{Email: testAccountingID}},
	}
	if _, err := client.Users(ctx); err == nil {
		t.Error("Users() не отклонил пользователя без account")
	}
}

func newHandlerTestClient(handler handlerServiceClient) *Client {
	return newClientWithHandler(
		io.NopCloser(nilReader{}),
		handler,
		&fakeStatsClient{},
		&fakeRoutingClient{},
		"vless-in",
	)
}

func testProtocolUser(accountingID, credentialUUID, flow string) *protocol.User {
	return &protocol.User{
		Email: accountingID,
		Account: serial.ToTypedMessage(&vless.Account{
			Id:         credentialUUID,
			Flow:       flow,
			Encryption: "none",
		}),
	}
}

type fakeHandlerClient struct {
	alterRequest  *proxymanCommand.AlterInboundRequest
	usersRequest  *proxymanCommand.GetInboundUserRequest
	usersResponse *proxymanCommand.GetInboundUserResponse
	err           error
	calls         int
}

func (f *fakeHandlerClient) AlterInbound(
	_ context.Context,
	request *proxymanCommand.AlterInboundRequest,
	_ ...grpc.CallOption,
) (*proxymanCommand.AlterInboundResponse, error) {
	f.calls++
	f.alterRequest = request
	return &proxymanCommand.AlterInboundResponse{}, f.err
}

func (f *fakeHandlerClient) GetInboundUsers(
	_ context.Context,
	request *proxymanCommand.GetInboundUserRequest,
	_ ...grpc.CallOption,
) (*proxymanCommand.GetInboundUserResponse, error) {
	f.calls++
	f.usersRequest = request
	return f.usersResponse, f.err
}
