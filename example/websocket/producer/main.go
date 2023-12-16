package main

import (
	"github.com/go-tron/etcd"
	localTime "github.com/go-tron/local-time"
	"github.com/go-tron/redis"
	"github.com/go-tron/socket"
	"github.com/go-tron/socket/pb"
	"google.golang.org/protobuf/types/known/anypb"
	"time"
)

func main() {
	var appName = "test-app"
	producer := socket.NewProducerGrpc(&socket.ProducerGrpcConfig{
		AppName:     appName,
		DefaultAddr: "127.0.0.1:10001",
		EtcdInstance: etcd.New(&etcd.Config{
			Endpoints:   []string{"http://127.0.0.1:10179", "http://127.0.0.1:10279", "http://127.0.0.1:10379"},
			Username:    "root",
			Password:    "Pf*rm1D^V&hBDAKC",
			DialTimeout: 5 * time.Second,
		}),
		RedisInstance: redis.New(&redis.Config{
			Addr:     "127.0.0.1:6379",
			Password: "GBkrIO9bkOcWrdsC",
		}),
	})

	content, err := anypb.New(&pb.Text{
		Message: "我哦我我我我",
	})
	if err != nil {
		panic(err)
	}

	err = producer.Publish(&pb.Message{
		Body: &pb.MessageBody{
			Cmd:     1,
			Content: content,
		},
		ClientId: "1",
		ExpireAt: localTime.Now().Add(time.Hour).ToTimestamppb(),
	})
	if err != nil {
		panic(err)
	}
}
