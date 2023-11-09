package socket

import (
	"context"
	"github.com/go-tron/logger"
	"github.com/go-tron/socket/pb"
	"google.golang.org/protobuf/proto"
	"time"
)

type SendAttemptDelayFunc func(n uint) time.Duration
type BinaryMessageHandler func(client *Client, msg []byte) error
type TextMessageHandler func(client *Client, msg []byte) error

type SendAttempt struct {
	SendMaxAttempts      uint          //消息重试发送最大次数 默认20次
	SendAttemptDelay     time.Duration //消息重试发送延迟 默认6秒
	SendAttemptDelayFunc               //消息重试发送延迟函数 覆盖MessageSendAttemptDelay
}

type clientConfig struct {
	sendAttempt          *SendAttempt
	storage              clientStorage
	logger               logger.Logger
	textMessageHandler   TextMessageHandler
	binaryMessageHandler BinaryMessageHandler
}

type ClientOption func(*clientConfig)

func NewClient(c Conn, s *server, opts ...ClientOption) (client *Client) {
	defer func() {
		client.Log(EventCodeText(EventConnected), "", nil)
	}()

	for _, apply := range opts {
		apply(s.clientConfig)
	}

	client = &Client{
		clientConfig: s.clientConfig,
		Conn:         c,
		Server:       s,
		IP:           c.GetIP(),
	}
	c.SetClient(client)
	if s.HeartbeatTimeout > 0 {
		client.HeartbeatTimeout = s.HeartbeatTimeout
	}
	go func() {
		t := time.NewTicker(time.Second)
		for range t.C {
			client.HeartbeatTimeout--
			if client.HeartbeatTimeout == 0 {
				client.closeConnection(ErrorHeartbeatTimeout)
				return
			}
		}
		t.Stop()
	}()
	return client
}

type Client struct {
	*clientConfig
	Server           *server
	Conn             Conn
	IP               string
	ClientId         string
	Disconnected     bool
	CloseError       error
	context          context.Context
	removeFn         context.CancelFunc
	HeartbeatTimeout uint8
}

func (c *Client) Log(event string, msg string, err error) {
	if c.logger == nil {
		return
	}
	c.logger.Info(msg,
		logger.NewField("event", event),
		logger.NewField("err", err),
		logger.NewField("connectId", c.Conn.ID()),
		logger.NewField("ip", c.IP),
		logger.NewField("clientId", c.ClientId))
}

func (c *Client) Authorize(clientId string) {
	defer c.Log(EventCodeText(EventAuthorized), "", nil)
	c.ClientId = clientId
	go c.loadMessage()
	c.Server.removeOldClient(c)
	c.Server.addClient(c)
}

func (c *Client) AuthorizeFailed(err error) {
	time.Sleep(time.Millisecond * 200)
	c.closeConnection(err)
}

func (c *Client) closeConnection(err error) {
	defer c.Log(EventCodeText(EventCloseConnect), err.Error(), nil)
	c.CloseError = err
	//通知客户端断开
	c.Conn.OnError(err, true)
	go func() {
		//延迟1秒后服务端主动断开
		time.Sleep(time.Second)
		c.Conn.Close()
	}()
}

func (c *Client) onDisconnect(reason []byte) {
	defer c.Log(EventCodeText(EventDisconnect), string(reason), nil)
	c.Disconnected = true
	if c.removeFn != nil {
		c.removeFn()
	}
}

func (c *Client) receiveTextMessage(msg []byte) (err error) {
	defer c.Log(EventCodeText(EventReceiveMessage), string(msg), err)
	if c.textMessageHandler == nil {
		return ErrorMessageHandlerUnset
	}
	return c.textMessageHandler(c, msg)
}

func (c *Client) receiveBinaryMessage(msg []byte) (err error) {
	defer c.Log(EventCodeText(EventReceiveMessage), string(msg), err)
	if c.binaryMessageHandler == nil {
		return ErrorMessageHandlerUnset
	}
	return c.binaryMessageHandler(c, msg)
}

func (c *Client) sendMessageWithRetry(msg *WrappedMessage) (err error) {
	if !msg.checkNeedRetry() {
		defer func() {
			if err == nil && msg.checkRemoveAfterSend() {
				msg.remove("remove after send")
			}
		}()
		return c.sendMessage(msg)
	}
	for msg.attempts <= c.sendAttempt.SendMaxAttempts {
		if msg.arrived == 1 {
			break
		}
		if err := c.sendMessage(msg); err != nil {
			return err
		}
		var attemptDelay = c.sendAttempt.SendAttemptDelay
		if c.sendAttempt.SendAttemptDelayFunc != nil {
			attemptDelay = c.sendAttempt.SendAttemptDelayFunc(msg.attempts)
		}
		time.Sleep(attemptDelay)
	}
	return nil
}

func (c *Client) sendMessage(msg *WrappedMessage) (err error) {
	defer c.Log(EventCodeText(EventSendMessage), msg.Id, err)
	if c.Disconnected {
		return ErrorClientHasDisconnected
	}
	if err = msg.attemptSend(c.sendAttempt.SendMaxAttempts); err != nil {
		return err
	}
	//bytes, err := json.Marshal(msg)
	bytes, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	c.Conn.Send(bytes)
	return nil
}

func (c *Client) loadMessage() {
	msgList, summary, err := c.Server.messageConfig.storage.getClientMessageList(c.ClientId)
	c.Log(EventCodeText(EventLoadMessage), summary, err)
	if err != nil {
		return
	}
	for _, msg := range msgList {
		if err := initWrappedMessage(
			msg,
			messageWithConfig(c.Server.messageConfig),
		); err != nil {
			continue
		}
		msg.log("load", "", msg.loadErr)
		if msg.loadErr == nil {
			go c.sendMessageWithRetry(msg)
		}
	}
}

func (c *Client) Send(pm *pb.Message) (err error) {
	if pm.ClientId == "" {
		pm.ClientId = c.ClientId
	}
	return c.Server.Send(pm)
}

func (c *Client) SendLow(msg []byte) {
	c.Conn.Send(msg)
}
