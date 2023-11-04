package main

import (
	"context"
	"fmt"
	"github.com/go-tron/base-error"
	"github.com/go-tron/local-time"
	"github.com/go-tron/socket/pb"
	"google.golang.org/grpc"
	"net"
	"time"
)

type server struct {
	pb.UnimplementedDispatchServer
}

func (s *server) ClientSrv(ctx context.Context, in *pb.Client) (*pb.Result, error) {
	fmt.Println(in.GetNodeName())
	return nil, baseError.New("123", "2134")
}

func (s *server) MessageSrv(ctx context.Context, in *pb.Message) (*pb.Result, error) {
	fmt.Println(localTime.FromTimestamppb(in.ExpireAt).String())
	return &pb.Result{Message: "Message " + in.GetId()}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":6666")
	if err != nil {
		panic(err)
	}
	s := grpc.NewServer()
	pb.RegisterDispatchServer(s, &server{})
	go func() {
		for true {
			fmt.Println(s.GetServiceInfo())
			time.Sleep(time.Second)
		}
	}()
	if err := s.Serve(lis); err != nil {
		panic(err)
	}
	select {}
}
