package socket

import (
	"context"
	"encoding/json"
	"github.com/go-tron/logger"
	"github.com/go-tron/socket/pb"
	"google.golang.org/protobuf/proto"
)

type Conn interface {
	ID() string
	SetClient(*Client)
	GetClient() (*Client, error)
	GetAuthorizedClient() (*Client, error)
	Close() error
	GetIP() string
	Send([]byte)
	OnError(error, bool)
}

type Server interface {
	Close() error
	Serve() error
	Send(*pb.Message) error
	Broadcast(*pb.Message)
}

type Config struct {
	AppName              string               //应用名称
	AppPort              string               //应用端口
	NodeName             string               //节点名称
	HeartbeatTimeout     int                  //心跳超时
	ClientLogger         logger.Logger        //客户端日志
	ClientStorage        clientStorage        //客户端状态存储接口
	SendAttempt          *SendAttempt         //消息发送重试设置
	TextMessageHandler   TextMessageHandler   //客户端消息处理函数
	BinaryMessageHandler BinaryMessageHandler //客户端消息处理函数
	MessageLogger        logger.Logger        //消息日志
	MessageStorage       messageStorage       //消息存储接口
	MessageIdGenerator   messageIdGenerator   //消息ID生成接口
	Dispatch             dispatch             //节点注册发现接口 *不配置时为单机模式
	ProducerServer       producerServer       //消息生产服务
}

type Option func(*Config)

func WithProducerServer(val producerServer) Option {
	return func(opts *Config) {
		opts.ProducerServer = val
	}
}
func WithDispatch(val dispatch) Option {
	return func(opts *Config) {
		opts.Dispatch = val
	}
}
func WithMessageStorage(val messageStorage) Option {
	return func(opts *Config) {
		opts.MessageStorage = val
	}
}
func WithClientStorage(val clientStorage) Option {
	return func(opts *Config) {
		opts.ClientStorage = val
	}
}
func WithSendAttemptDelayFunc(val SendAttemptDelayFunc) Option {
	return func(opts *Config) {
		if opts.SendAttempt == nil {
			opts.SendAttempt = &SendAttempt{}
		}
		opts.SendAttempt.SendAttemptDelayFunc = val
	}
}

func WithMessageIdGenerator(val messageIdGenerator) Option {
	return func(opts *Config) {
		opts.MessageIdGenerator = val
	}
}
func WithTextMessageHandler(val TextMessageHandler) Option {
	return func(opts *Config) {
		opts.TextMessageHandler = val
	}
}
func WithBinaryMessageHandler(val BinaryMessageHandler) Option {
	return func(opts *Config) {
		opts.BinaryMessageHandler = val
	}
}

func newServer(config *serverConfig) *server {
	s := &server{
		serverConfig: config,
		ClientList:   make(ClientList, 0),
	}
	if s.dispatch != nil {
		go func() {
			for v := range s.dispatch.subscribeClient() {
				for nodeName, clientId := range v {
					s.removeOldClientSubscribe(nodeName, clientId)
				}
			}
		}()
		go func() {
			for msg := range s.dispatch.subscribeMessage() {
				s.Send(msg)
			}
		}()
	}
	if s.producerServer != nil {
		go func() {
			for msg := range s.producerServer.subscribeMessage() {
				s.Send(msg)
			}
		}()
	}
	if s.clientStorage != nil {
		s.clientStorage.resetNode(s.NodeName)
	}
	return s
}

type serverConfig struct {
	NodeName         string
	HeartbeatTimeout int
	dispatch         dispatch
	producerServer   producerServer
	messageConfig    *messageConfig
	clientConfig     *clientConfig
	clientStorage
}

type server struct {
	*serverConfig
	ClientList
}

func (s *server) removeOldClient(c *Client) {
	oc := s.ClientList.GetByClientId(c.ClientId)
	if oc == nil {
		return
	}
	if oc.Conn.ID() == c.Conn.ID() {
		return
	}
	s.ClientList.RemoveByConnectId(oc.Conn.ID())

	if oc.UniqueSig == c.UniqueSig {
		oc.closeConnection(ErrorDuplicateConnectWithSameUniqueSig, false)
	} else {
		oc.closeConnection(ErrorDuplicateConnect, false)
	}
}

func (s *server) removeOldClientSubscribe(nodeName string, clientId string) {
	if nodeName == s.NodeName {
		return
	}
	oc := s.ClientList.GetByClientId(clientId)
	if oc == nil {
		return
	}
	s.ClientList.RemoveByConnectId(oc.Conn.ID())
	oc.closeConnection(ErrorDuplicateConnect, false)
}

func (s *server) addClient(client *Client) {
	client.context, client.removeFn = context.WithCancel(context.Background())
	go func() {
		select {
		case <-client.context.Done():
			s.removeClient(client)
		}
	}()

	s.ClientList.Add(client)
	if client.storage != nil {
		client.storage.setStatusOnline(client.context, client.ClientId, s.NodeName)
	}
	if s.dispatch != nil {
		s.dispatch.publishClient(client.ClientId)
	}
}

func (s *server) removeClient(client *Client) {
	if client.ClientId == "" || client.Removed {
		return
	}
	client.Removed = true
	s.ClientList.RemoveByConnectId(client.Conn.ID())
	if !client.ActiveClose && client.storage != nil {
		client.storage.setStatusOffline(client.ClientId, s.NodeName)
	}
}

func (s *server) send(msg *WrappedMessage) (err error) {
	//客户端连接至当前服务器节点时,直接发送
	client := s.ClientList.GetByClientId(msg.ClientId)
	if client != nil {
		return client.sendMessageWithRetry(msg)
	}

	//单机模式下直接判断离线
	if s.dispatch == nil {
		return ErrorClientOffline
	}
	//获取客户端连接状态
	nodeName, err := s.clientStorage.getStatus(msg.ClientId)
	if err != nil {
		return ErrorGetClientStatus(err.Error())
	}
	if nodeName == "" {
		//客户端离线直接返回 消息已存时待上线再发
		return ErrorClientOffline
	} else if nodeName == s.NodeName {
		//客户端状态异常
		return ErrorClientStatusException
	} else {
		//客户端连接至集群其他服务器节点时,发布至对应服务器节点处理发送
		return s.dispatch.publishMessage(nodeName, msg.Message)
	}
}

func (s *server) SendWrapped(msg *WrappedMessage) (err error) {
	defer func() {
		if msg != nil {
			defer msg.log("Send", "", err)
		}
		if err == ErrorClientOffline {
			err = nil
		}
	}()
	if err := initWrappedMessage(
		msg,
		messageWithConfig(s.messageConfig),
	); err != nil {
		return err
	}
	return s.send(msg)
}

func (s *server) Send(pb *pb.Message) (err error) {
	var msg *WrappedMessage
	defer func() {
		if msg != nil {
			defer msg.log("Send", "", err)
		}
		if err == ErrorClientOffline {
			err = nil
		}
	}()
	msg, err = newMessage(
		pb,
		messageWithConfig(s.messageConfig),
	)
	if err != nil {
		return err
	}
	return s.send(msg)
}

func (s *server) OnTextMessage(c Conn, data []byte) (err error) {
	defer func() {
		if err != nil {
			c.OnError(err, false)
		}
	}()
	client, err := c.GetClient()
	if err != nil {
		return err
	}
	msg := &JsonMessage{}
	if err := json.Unmarshal(data, msg); err != nil {
		return err
	}

	if msg.Body.Cmd == uint32(pb.SocketCmd_SocketCmdHeartbeat) {
		m := &JsonMessage{
			Body: &JsonMessageBody{
				Cmd: uint32(pb.SocketCmd_SocketCmdHeartbeat),
			},
			ClientId: client.ClientId,
		}
		bytes, err := json.Marshal(m)
		if err != nil {
			return err
		}
		client.Conn.Send(bytes)
		return nil
	}
	return client.receiveTextMessage(msg, data)
}

func (s *server) OnBinaryMessage(c Conn, data []byte) (err error) {
	defer func() {
		if err != nil {
			c.OnError(err, false)
		}
	}()
	client, err := c.GetClient()
	if err != nil {
		return err
	}
	msg := &pb.Message{}
	if err := proto.Unmarshal(data, msg); err != nil {
		return err
	}

	cmd := msg.Body.Cmd
	if cmd == uint32(pb.SocketCmd_SocketCmdHeartbeat) {
		client.HeartbeatTime = s.HeartbeatTimeout
		m := &pb.Message{
			Body: &pb.MessageBody{
				Cmd: uint32(pb.SocketCmd_SocketCmdHeartbeat),
			},
			ClientId: client.ClientId,
		}
		bytes, err := proto.Marshal(m)
		if err != nil {
			return err
		}
		client.Conn.Send(bytes)
		return nil
	}
	return client.receiveBinaryMessage(msg, data)
}

func (s *server) OnConnect(c Conn) {
	NewClient(c, s)
}

func (s *server) OnDisconnect(c Conn, reason []byte) {
	client, err := c.GetClient()
	if err != nil {
		return
	}
	client.onDisconnect(reason)
}

func (s *server) OnError(c Conn, e error) {
	client, err := c.GetClient()
	if err != nil {
		return
	}
	client.onError(e)
}

func (s *server) Broadcast(msg *pb.Message) {
	bytes, err := proto.Marshal(msg)
	if err != nil {
		return
	}
	for _, c := range s.ClientList {
		if !c.Disconnected {
			c.Conn.Send(bytes)
		}
	}
}
