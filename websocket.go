package socket

import (
	baseError "github.com/go-tron/base-error"
	"github.com/go-tron/config"
	"github.com/go-tron/logger"
	"github.com/go-tron/snowflake-id"
	"github.com/go-tron/socket/pb"
	"github.com/gorilla/websocket"
	uuid "github.com/satori/go.uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"log"
	"net/http"
	"reflect"
	"sync"
	"time"
)

type WebSocketConn struct {
	id string
	*websocket.Conn
	client *Client
	mu     sync.Mutex
}

func NewWebSocketConn(sc *websocket.Conn) *WebSocketConn {
	return &WebSocketConn{
		id:   uuid.NewV4().String(),
		Conn: sc,
	}
}
func (s *WebSocketConn) SetClient(client *Client) {
	s.client = client
}
func (s WebSocketConn) Close() error {
	return s.Conn.Close()
}
func (s WebSocketConn) ID() string {
	return s.id
}

func (s *WebSocketConn) GetAuthorizedClient() (*Client, error) {
	client, err := s.GetClient()
	if err != nil {
		return nil, err
	}
	if client.ClientId == "" {
		return nil, ErrorClientIdUnset
	}
	return client, nil
}

func (s *WebSocketConn) GetClient() (*Client, error) {
	client := s.client
	if client != nil {
		return client, nil
	}
	return nil, ErrorClientNotFound
}

func (s WebSocketConn) GetIP() string {
	ip := s.Conn.RemoteAddr().String()
	return ip
}

func (s WebSocketConn) Send(msg []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.WriteMessage(websocket.BinaryMessage, msg)
	if err != nil {
		log.Println("Send Error:", err)
	}
}

func (s WebSocketConn) OnError(err error, disconnect bool) {
	var e *baseError.Error
	if reflect.TypeOf(err).String() == "*baseError.Error" {
		e = err.(*baseError.Error)
	} else {
		e = baseError.System("100", err.Error())
	}
	content, _ := anypb.New(&pb.Error{
		Code:    e.Code,
		Message: e.Msg,
	})
	cmd := pb.SocketCmd_SocketCmdError
	if disconnect {
		cmd = pb.SocketCmd_SocketCmdDisconnect
	}
	m := &pb.Message{
		Body: &pb.MessageBody{
			Cmd:     uint32(cmd),
			Content: content,
		},
	}
	bytes, err := proto.Marshal(m)
	if err != nil {
		return
	}
	s.Send(bytes)
}

type WebSocketServer struct {
	*http.Server
	*server
}

func (s *WebSocketServer) Serve() error {
	return s.Server.ListenAndServe()
}

func (s *WebSocketServer) Close() error {
	return s.Server.Close()
}

func (s *WebSocketServer) Send(msg *pb.Message) (err error) {
	return s.server.Send(msg)
}

func NewWebSocketWithConfig(c *config.Config, opts ...Option) Server {
	config := &Config{
		AppName:          c.GetString("application.name"),
		AppPort:          c.GetString("application.port"),
		NodeName:         c.GetString("cluster.podName"),
		HeartbeatTimeout: c.GetInt("websocket.heartbeatTimeout"),
		SendAttempt: &SendAttempt{
			SendMaxAttempts:  c.GetUint("websocket.sendMaxAttempts"),
			SendAttemptDelay: c.GetDuration("websocket.sendAttemptDelay"),
		},
		MessageIdGenerator: snowflakeId.NewWithConfig15(c),
		ClientLogger:       logger.NewZapWithConfig(c, "websocket-client", "info"),
		MessageLogger:      logger.NewZapWithConfig(c, "websocket-message", "info"),
	}
	return NewWebSocket(config, opts...)
}

func NewWebSocket(config *Config, opts ...Option) Server {
	for _, apply := range opts {
		if apply != nil {
			apply(config)
		}
	}
	if config.AppName == "" {
		panic("AppName 必须设置")
	}
	if config.AppPort == "" {
		panic("AppPort 必须设置")
	}
	if config.NodeName == "" {
		panic("NodeName 必须设置")
	}
	if config.MessageStorage == nil {
		panic("MessageStorage 必须设置")
	}
	if config.Dispatch != nil && config.ClientStorage == nil {
		panic("Cluster模式 ClientStorage 必须设置")
	}
	if config.SendAttempt == nil {
		panic("SendAttempt 必须设置")
	}
	if config.SendAttempt.SendMaxAttempts == 0 {
		config.SendAttempt.SendMaxAttempts = 20
	}
	if config.SendAttempt.SendAttemptDelay == 0 {
		config.SendAttempt.SendAttemptDelay = time.Second * 6
	}

	messageConf := &messageConfig{
		logger:      config.MessageLogger,
		storage:     config.MessageStorage,
		idGenerator: config.MessageIdGenerator,
	}

	serverConf := &serverConfig{
		NodeName:         config.NodeName,
		HeartbeatTimeout: config.HeartbeatTimeout,
		dispatch:         config.Dispatch,
		producerServer:   config.ProducerServer,
		messageConfig:    messageConf,
		clientConfig: &clientConfig{
			sendAttempt:          config.SendAttempt,
			storage:              config.ClientStorage,
			logger:               config.ClientLogger,
			binaryMessageHandler: config.BinaryMessageHandler,
			textMessageHandler:   config.TextMessageHandler,
		},
		clientStorage: config.ClientStorage,
	}

	httpServer := &http.Server{
		Addr:              ":" + config.AppPort,
		ReadHeaderTimeout: 3 * time.Second,
	}
	websocketServer := &WebSocketServer{
		server: newServer(serverConf),
		Server: httpServer,
	}
	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		sc, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("upgrader err", err)
			return
		}
		conn := NewWebSocketConn(sc)
		websocketServer.OnConnect(conn)

		go func() {
			for {
				messageType, message, err := conn.ReadMessage()
				if err != nil {
					websocketServer.OnError(conn, err)
					//_, ok := err.(*websocket.CloseError)
					//_, ok := err.(*net.OpError)
					break
				}
				if messageType == websocket.TextMessage {
					websocketServer.OnTextMessage(conn, message)
				} else if messageType == websocket.BinaryMessage {
					websocketServer.OnBinaryMessage(conn, message)
				}
			}
		}()
	})
	return websocketServer
}
