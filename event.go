package socket

type Event int

const (
	EventConnected Event = iota
	EventConnectError
	EventCloseAuthorizeFailed
	EventCloseOldClient
	EventCloseOldClientSubscribe
	EventDisconnect
	EventLogin
	EventRemoveOldClient
	EventSendMessage
	EventReceiveMessage
	EventAckMessage
	EventLoadMessage
)

var eventCodeText = map[Event]string{
	EventConnected:               "Connected",
	EventConnectError:            "ConnectError",
	EventCloseAuthorizeFailed:    "CloseAuthorizeFailed",
	EventCloseOldClient:          "CloseOldClient",
	EventCloseOldClientSubscribe: "CloseOldClientSubscribe",
	EventDisconnect:              "Disconnect",
	EventLogin:                   "Login",
	EventRemoveOldClient:         "RemoveOldClient",
	EventSendMessage:             "SendMessage",
	EventReceiveMessage:          "ReceiveMessage",
	EventAckMessage:              "AckMessage",
	EventLoadMessage:             "LoadMessage",
}

func EventCodeText(code Event) string {
	return eventCodeText[code]
}
