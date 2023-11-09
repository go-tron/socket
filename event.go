package socket

type Event int

const (
	EventConnected Event = iota
	EventConnectError
	EventDisconnect
	EventCloseConnect
	EventAuthorized
	EventSendMessage
	EventReceiveMessage
	EventAckMessage
	EventLoadMessage
)

var eventCodeText = map[Event]string{
	EventConnected:      "Connected",
	EventConnectError:   "ConnectError",
	EventDisconnect:     "Disconnect",
	EventCloseConnect:   "CloseConnect",
	EventAuthorized:     "EventAuthorized",
	EventSendMessage:    "SendMessage",
	EventReceiveMessage: "ReceiveMessage",
	EventAckMessage:     "AckMessage",
	EventLoadMessage:    "LoadMessage",
}

func EventCodeText(code Event) string {
	return eventCodeText[code]
}
