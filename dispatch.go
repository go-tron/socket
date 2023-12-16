package socket

import (
	"context"
	"github.com/go-tron/config"
	"github.com/go-tron/etcd"
	"github.com/go-tron/logger"
	"github.com/go-tron/socket/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/stats"
	"net"
	"sync"
	"time"
)

type dispatch interface {
	subscribeClient() chan map[string]string
	subscribeMessage() chan *pb.Message
	publishClient(clientId string)
	publishMessage(msg *pb.Message) error
}

type DispatchGrpcConfig struct {
	AppName       string
	NodeName      string
	IP            string
	Port          string
	TTL           int64
	EtcdConfig    *etcd.Config
	EtcdInstance  *etcd.Client
	ClientStorage clientStorage
}
type DispatchGrpcOption func(*DispatchGrpcConfig)

func DispatchGrpcWithEtcdConfig(val *etcd.Config) DispatchGrpcOption {
	return func(conf *DispatchGrpcConfig) {
		conf.EtcdConfig = val
	}
}
func DispatchGrpcWithEtcdInstance(val *etcd.Client) DispatchGrpcOption {
	return func(conf *DispatchGrpcConfig) {
		conf.EtcdInstance = val
	}
}
func NewDispatchGrpcWithConfig(c *config.Config, opts ...DispatchGrpcOption) *DispatchGrpcServer {
	conf := &DispatchGrpcConfig{
		AppName:  c.GetString("application.name"),
		NodeName: c.GetString("cluster.podName"),
		IP:       c.GetString("cluster.podIP"),
		Port:     c.GetString("socket.dispatch.port"),
		TTL:      c.GetInt64("etcd.register.ttl"),
		EtcdConfig: &etcd.Config{
			Endpoints:   c.GetStringSlice("etcd.endpoints"),
			Username:    c.GetString("etcd.username"),
			Password:    c.GetString("etcd.password"),
			DialTimeout: c.GetDuration("etcd.dialTimeout"),
			Logger:      logger.NewZapWithConfig(c, "etcd", "error"),
		},
	}
	return NewDispatchGrpc(conf, opts...)
}

func NewDispatchGrpc(config *DispatchGrpcConfig, opts ...DispatchGrpcOption) *DispatchGrpcServer {
	for _, apply := range opts {
		if apply != nil {
			apply(config)
		}
	}
	if config.AppName == "" {
		panic("AppName 必须设置")
	}
	if config.NodeName == "" {
		panic("NodeName 必须设置")
	}
	if config.IP == "" {
		panic("IP 必须设置")
	}
	if config.Port == "" {
		panic("Port 必须设置")
	}
	if config.EtcdInstance == nil {
		if config.EtcdConfig == nil {
			panic("请设置etcd实例或者连接配置")
		}
		config.EtcdInstance = etcd.New(config.EtcdConfig)
	}
	if config.TTL == 0 {
		config.TTL = 15
	}

	s := &DispatchGrpcServer{
		NodeName:      config.NodeName,
		clientStorage: config.ClientStorage,
		NodeList:      &sync.Map{},
		connectChan:   make(chan map[string]string),
		messageChan:   make(chan *pb.Message),
	}
	go NewDispatchGrpcServer(":"+config.Port, s)

	etcd.MustNewRegister(&etcd.RegisterConfig{
		AppName:      config.AppName,
		Node:         config.NodeName,
		Addr:         config.IP + ":" + config.Port,
		TTL:          config.TTL,
		EtcdInstance: config.EtcdInstance,
	})

	discovery := etcd.MustNewDiscovery(&etcd.DiscoveryConfig{
		AppName:      config.AppName,
		EtcdInstance: config.EtcdInstance,
	})
	go func() {
		for v := range discovery.AddSubscribe() {
			for nodeName, addr := range v {
				if nodeName == config.NodeName {
					continue
				}
				val, ok := s.NodeList.LoadAndDelete(nodeName)
				if ok {
					val.(*DispatchGrpcClient).Conn.Close()
				}
				node, err := NewDispatchGrpcClient(addr)
				if err == nil {
					s.NodeList.Store(nodeName, node)
				}
			}
		}
	}()
	go func() {
		for nodeName := range discovery.RemoveSubscribe() {
			if nodeName == config.NodeName {
				continue
			}
			val, ok := s.NodeList.LoadAndDelete(nodeName)
			if ok {
				val.(*DispatchGrpcClient).Conn.Close()
			}
		}
	}()
	/*go func() {
		for {
			var v = ""
			s.NodeList.Range(func(key, value interface{}) bool {
				v += " " + key.(string)
				return true
			})
			fmt.Println(v)
			time.Sleep(6 * time.Second)
		}
	}()*/
	return s
}

type DispatchGrpcServer struct {
	NodeName      string
	NodeList      *sync.Map
	clientStorage clientStorage
	connectChan   chan map[string]string
	messageChan   chan *pb.Message
	pb.UnimplementedDispatchServer
}

func (s *DispatchGrpcServer) subscribeClient() chan map[string]string {
	return s.connectChan
}
func (s *DispatchGrpcServer) subscribeMessage() chan *pb.Message {
	return s.messageChan
}

func (s *DispatchGrpcServer) publishClient(clientId string) {
	s.NodeList.Range(func(key, value interface{}) bool {
		client := value.(*DispatchGrpcClient)
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		defer cancel()
		client.Client.ClientSrv(ctx, &pb.Client{
			NodeName: s.NodeName,
			ClientId: clientId,
		})
		return true
	})
}

func (s *DispatchGrpcServer) publishMessage(msg *pb.Message) (err error) {
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
	}

	value, ok := s.NodeList.Load(nodeName)
	if !ok {
		return ErrorGetNodeClient(nodeName)
	}
	client := value.(*DispatchGrpcClient)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	_, err = client.Client.MessageSrv(ctx, msg)
	return err
}

func (s *DispatchGrpcServer) ClientSrv(ctx context.Context, in *pb.Client) (*pb.Result, error) {
	go func() {
		s.connectChan <- map[string]string{in.NodeName: in.ClientId}
	}()
	return &pb.Result{Message: "ok"}, nil
}

func (s *DispatchGrpcServer) MessageSrv(ctx context.Context, in *pb.Message) (*pb.Result, error) {
	go func() {
		s.messageChan <- in
	}()
	return &pb.Result{Message: "ok"}, nil
}

func NewDispatchGrpcServer(addr string, server *DispatchGrpcServer) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	s := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second,
		PermitWithoutStream: true,
	}))
	pb.RegisterDispatchServer(s, server)

	if err := s.Serve(lis); err != nil {
		panic(err)
	}
}

type DispatchGrpcClient struct {
	Conn   *grpc.ClientConn
	Client pb.DispatchClient
}

func NewDispatchGrpcClient(addr string) (client *DispatchGrpcClient, err error) {
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		//grpc.WithStatsHandler(&StatsHandler{
		//	OnDisconnect: func() {
		//		conn.Close()
		//		onDisconnect()
		//	},
		//}),
	)
	if err != nil {
		return nil, err
	}

	return &DispatchGrpcClient{
		Conn:   conn,
		Client: pb.NewDispatchClient(conn),
	}, nil
}

type StatsHandler struct {
	OnDisconnect func()
}

func (h *StatsHandler) TagRPC(context.Context, *stats.RPCTagInfo) context.Context {
	return context.Background()
}
func (h *StatsHandler) HandleRPC(context.Context, stats.RPCStats) {
}
func (h *StatsHandler) TagConn(context.Context, *stats.ConnTagInfo) context.Context {
	return context.Background()
}
func (h *StatsHandler) HandleConn(c context.Context, s stats.ConnStats) {
	switch s.(type) {
	case *stats.ConnEnd:
		h.OnDisconnect()
		break
	}
}
