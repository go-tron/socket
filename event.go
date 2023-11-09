package socket

type Event int

const (
	EventConnected Event = iota
	EventError
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
	EventError:          "Error",
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
