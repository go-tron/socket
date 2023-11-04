package socket

import (
	"context"
	"github.com/go-tron/config"
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
	publishMessage(nodeName string, msg *pb.Message) error
}

type DispatchGrpcConfig struct {
	NodeName  string
	IP        string
	Port      string
	Discovery discovery
}
type DispatchGrpcOption func(*DispatchGrpcConfig)

func DispatchGrpcWithDiscovery(val discovery) DispatchGrpcOption {
	return func(conf *DispatchGrpcConfig) {
		conf.Discovery = val
	}
}

func NewDispatchGrpcWithConfig(c *config.Config, opts ...DispatchGrpcOption) *DispatchServerGrpc {
	conf := &DispatchGrpcConfig{
		NodeName: c.GetString("cluster.podName"),
		IP:       c.GetString("cluster.podIP"),
		Port:     c.GetString("socket.dispatch.port"),
	}
	return NewDispatchGrpc(conf, opts...)
}

func NewDispatchGrpc(config *DispatchGrpcConfig, opts ...DispatchGrpcOption) *DispatchServerGrpc {
	for _, apply := range opts {
		if apply != nil {
			apply(config)
		}
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
	if config.Discovery == nil {
		panic("Discovery 必须设置")
	}

	s := &DispatchServerGrpc{
		NodeName:    config.NodeName,
		discovery:   config.Discovery,
		NodeList:    &sync.Map{},
		connectChan: make(chan map[string]string),
		messageChan: make(chan *pb.Message),
	}
	go NewDispatchServerGrpc(":"+config.Port, s)
	go s.discovery.nodeRegister(s.NodeName, config.IP+":"+config.Port)
	go func() {
		for v := range s.discovery.nodeAddSubscribe() {
			for nodeName, addr := range v {
				if nodeName == config.NodeName {
					continue
				}
				node, ok := s.NodeList.LoadAndDelete(nodeName)
				if ok {
					node.(*DispatchClientGrpc).Conn.Close()
				}
				client, err := NewDispatchClientGrpc(addr)
				if err == nil {
					s.NodeList.Store(nodeName, client)
				}
			}
		}
	}()
	go func() {
		for nodeName := range s.discovery.nodeRemoveSubscribe() {
			if nodeName == config.NodeName {
				continue
			}
			node, ok := s.NodeList.LoadAndDelete(nodeName)
			if ok {
				node.(*DispatchClientGrpc).Conn.Close()
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

type DispatchServerGrpc struct {
	NodeName    string
	NodeList    *sync.Map
	discovery   discovery
	connectChan chan map[string]string
	messageChan chan *pb.Message
	pb.UnimplementedDispatchServer
}

func (s *DispatchServerGrpc) subscribeClient() chan map[string]string {
	return s.connectChan
}
func (s *DispatchServerGrpc) subscribeMessage() chan *pb.Message {
	return s.messageChan
}

func (s *DispatchServerGrpc) publishClient(clientId string) {
	s.NodeList.Range(func(key, value interface{}) bool {
		client := value.(*DispatchClientGrpc)
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		defer cancel()
		client.Client.ClientSrv(ctx, &pb.Client{
			NodeName: s.NodeName,
			ClientId: clientId,
		})
		return true
	})
}

func (s *DispatchServerGrpc) publishMessage(nodeName string, msg *pb.Message) (err error) {
	var client *DispatchClientGrpc
	value, ok := s.NodeList.Load(nodeName)
	if !ok {
		return ErrorGetNodeClient(nodeName)
	}
	client = value.(*DispatchClientGrpc)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	_, err = client.Client.MessageSrv(ctx, msg)
	return err
}

func (s *DispatchServerGrpc) ClientSrv(ctx context.Context, in *pb.Client) (*pb.Result, error) {
	go func() {
		s.connectChan <- map[string]string{in.NodeName: in.ClientId}
	}()
	return &pb.Result{Message: "ok"}, nil
}

func (s *DispatchServerGrpc) MessageSrv(ctx context.Context, in *pb.Message) (*pb.Result, error) {
	go func() {
		s.messageChan <- in
	}()
	return &pb.Result{Message: "ok"}, nil
}

func NewDispatchServerGrpc(addr string, server *DispatchServerGrpc) {
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

type DispatchClientGrpc struct {
	Conn   *grpc.ClientConn
	Client pb.DispatchClient
}

func NewDispatchClientGrpc(addr string) (client *DispatchClientGrpc, err error) {
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

	return &DispatchClientGrpc{
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
