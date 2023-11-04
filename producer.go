package socket

import (
	"context"
	"github.com/go-tron/config"
	"github.com/go-tron/socket/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"net"
	"time"
)

type producerServer interface {
	subscribeMessage() chan *pb.Message
}

type ProducerServerGrpc struct {
	messageChan chan *pb.Message
	pb.UnimplementedProducerServer
}

func (s *ProducerServerGrpc) subscribeMessage() chan *pb.Message {
	return s.messageChan
}

func (s *ProducerServerGrpc) MessageSrv(ctx context.Context, in *pb.Message) (*pb.Result, error) {
	go func() {
		s.messageChan <- in
	}()
	return &pb.Result{Message: "ok"}, nil
}

type ProducerServerGrpcConfig struct {
	Port string
}
type ProducerServerGrpcOption func(*ProducerServerGrpcConfig)

func NewProducerServerGrpcWithConfig(c *config.Config, opts ...ProducerServerGrpcOption) *ProducerServerGrpc {
	conf := &ProducerServerGrpcConfig{
		Port: c.GetString("socket.producer.port"),
	}
	return NewProducerServerGrpc(conf, opts...)
}

func NewProducerServerGrpc(config *ProducerServerGrpcConfig, opts ...ProducerServerGrpcOption) *ProducerServerGrpc {
	for _, apply := range opts {
		if apply != nil {
			apply(config)
		}
	}
	if config.Port == "" {
		panic("Port 必须设置")
	}
	lis, err := net.Listen("tcp", ":"+config.Port)
	if err != nil {
		panic(err)
	}
	s := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second,
		PermitWithoutStream: true,
	}))

	p := &ProducerServerGrpc{
		messageChan: make(chan *pb.Message),
	}
	pb.RegisterProducerServer(s, p)
	go func() {
		if err := s.Serve(lis); err != nil {
			panic(err)
		}
	}()
	return p
}

type producer interface {
	Publish(msg *pb.Message) (err error)
}

type ProducerGrpc struct {
	Conn   *grpc.ClientConn
	Client pb.ProducerClient
}

func (c *ProducerGrpc) Publish(msg *pb.Message) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	_, err = c.Client.MessageSrv(ctx, msg)
	return err
}

func NewProducerGrpcWithConfig(c *config.Config) *ProducerGrpc {
	return NewProducerGrpc(c.GetString("socket.producer.port"))
}

func NewProducerGrpc(addr string) *ProducerGrpc {
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
		panic(err)
	}
	return &ProducerGrpc{
		Conn:   conn,
		Client: pb.NewProducerClient(conn),
	}
}
