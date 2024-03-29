package socket

import (
	"context"
	"encoding/json"
	"github.com/go-tron/logger"
	"github.com/go-tron/redis"
	"github.com/go-tron/socket/pb"
	"google.golang.org/protobuf/proto"
	"slices"
	"strconv"
)

type Conn interface {
	ID() string
	SetClient(*Client)
	GetClient() (*Client, error)
	GetAuthorizedClient() (*Client, error)
	Close() error
	GetIP() string
	Send([]byte) error
	OnError(error, bool)
}

type Server interface {
	Close() error
	Serve() error
	Send(*pb.Message) error
	Broadcast(interface{})
	SendMany(interface{}, ...string)
}

type Config struct {
	AppName              string //应用名称
	AppPort              string //应用端口
	NodeName             string //节点名称
	HeartbeatTimeout     int    //心跳超时
	ClientCmdMap         map[int32]string
	ServerCmdMap         map[int32]string
	ClientLogger         logger.Logger        //客户端日志
	SendAttempt          *SendAttempt         //消息发送重试设置
	TextMessageHandler   TextMessageHandler   //客户端消息处理函数
	BinaryMessageHandler BinaryMessageHandler //客户端消息处理函数
	MessageLogger        logger.Logger        //消息日志
	MessageIdGenerator   messageIdGenerator   //消息ID生成接口
	Dispatch             dispatch             //节点注册发现接口 *不配置时为单机模式
	RedisConfig          *redis.Config
	RedisInstance        *redis.Redis
}

type Option func(*Config)

func WithClientCmdMap(val map[int32]string) Option {
	return func(opts *Config) {
		opts.ClientCmdMap = val
	}
}
func WithServerCmdMap(val map[int32]string) Option {
	return func(opts *Config) {
		opts.ServerCmdMap = val
	}
}
func WithDispatch(val dispatch) Option {
	return func(opts *Config) {
		opts.Dispatch = val
	}
}
func WithRedisConfig(val *redis.Config) Option {
	return func(opts *Config) {
		opts.RedisConfig = val
	}
}
func WithRedisInstance(val *redis.Redis) Option {
	return func(opts *Config) {
		opts.RedisInstance = val
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
				s.removeOldClientSubscribe(v)
			}
		}()
		go func() {
			for v := range s.dispatch.subscribeMessage() {
				s.Send(v)
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
	ClientCmdMap     map[int32]string
	ServerCmdMap     map[int32]string
	dispatch         dispatch
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

func (s *server) removeOldClientSubscribe(c *pb.Client) {
	if c.NodeName == s.NodeName {
		return
	}
	oc := s.ClientList.GetByClientId(c.ClientId)
	if oc == nil {
		return
	}
	s.ClientList.RemoveByConnectId(oc.Conn.ID())

	if oc.UniqueSig == c.UniqueSig {
		oc.closeConnection(ErrorDuplicateConnectWithSameUniqueSig, false)
	} else {
		oc.closeConnection(ErrorDuplicateConnect, false)
	}
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
	if s.clientStorage != nil {
		s.clientStorage.setStatusOnline(client.context, client.ClientId, s.NodeName)
	}
	if s.dispatch != nil {
		s.dispatch.publishClient(&pb.Client{
			ClientId:  client.ClientId,
			UniqueSig: client.UniqueSig,
		})
	}
}

func (s *server) removeClient(client *Client) {
	if client.ClientId == "" || client.Removed {
		return
	}
	client.Removed = true
	s.ClientList.RemoveByConnectId(client.Conn.ID())
	if !client.ActiveClose && s.clientStorage != nil {
		s.clientStorage.setStatusOffline(client.ClientId, s.NodeName)
	}
}

func (s *server) send(msg *WrappedMessage) (err error) {
	if msg.Body == nil {
		return nil
	}
	defer func() {
		if s.clientConfig.logger == nil {
			return
		}
		cmd := strconv.Itoa(int(msg.Body.Cmd))
		if s.ServerCmdMap != nil && s.ServerCmdMap[int32(msg.Body.Cmd)] != "" {
			cmd = s.ServerCmdMap[int32(msg.Body.Cmd)]
		}
		s.clientConfig.logger.Info(
			cmd,
			logger.NewField("event", "SendMessage"),
			logger.NewField("err", err),
			logger.NewField("clientId", msg.ClientId),
		)
	}()
	//客户端连接至当前服务器节点时,直接发送
	client := s.ClientList.GetByClientId(msg.ClientId)
	if client != nil {
		return client.sendMessageWithRetry(msg)
	}
	//单机模式下直接判断离线
	if s.dispatch == nil {
		return ErrorClientOffline
	}
	return s.dispatch.publishMessage(msg.Message)
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
	if msg.Body == nil {
		return ErrorBodyIsEmpty
	}

	client.HeartbeatTime = s.HeartbeatTimeout
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
		return client.Conn.Send(bytes)
	}

	defer func() {
		cmd := strconv.Itoa(int(msg.Body.Cmd))
		if s.ClientCmdMap != nil && s.ClientCmdMap[int32(msg.Body.Cmd)] != "" {
			cmd = s.ClientCmdMap[int32(msg.Body.Cmd)]
		}
		client.Log(EventReceiveMessage, cmd, err)
	}()
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
	if msg.Body == nil {
		return ErrorBodyIsEmpty
	}

	client.HeartbeatTime = s.HeartbeatTimeout
	if msg.Body.Cmd == uint32(pb.SocketCmd_SocketCmdHeartbeat) {
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
		return client.Conn.Send(bytes)
	}

	defer func() {
		cmd := strconv.Itoa(int(msg.Body.Cmd))
		if s.ClientCmdMap != nil && s.ClientCmdMap[int32(msg.Body.Cmd)] != "" {
			cmd = s.ClientCmdMap[int32(msg.Body.Cmd)]
		}
		client.Log(EventReceiveMessage, cmd, err)
	}()
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

func (s *server) Broadcast(msg interface{}) {
	var data []byte
	switch v := msg.(type) {
	case *pb.Message:
		bytes, err := proto.Marshal(v)
		if err != nil {
			return
		}
		data = bytes
	case []byte:
		data = v
	}
	if len(data) == 0 {
		return
	}
	for _, c := range s.ClientList {
		if !c.Disconnected {
			go c.Conn.Send(data)
		}
	}
}

func (s *server) SendMany(msg interface{}, clients ...string) {
	var data []byte
	switch v := msg.(type) {
	case *pb.Message:
		bytes, err := proto.Marshal(v)
		if err != nil {
			return
		}
		data = bytes
	case []byte:
		data = v
	}
	if len(data) == 0 {
		return
	}
	for _, c := range s.ClientList {
		if !c.Disconnected && slices.Contains(clients, c.ClientId) {
			go c.Conn.Send(data)
		}
	}
}
