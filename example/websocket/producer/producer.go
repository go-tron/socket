package main

import (
	localTime "github.com/go-tron/local-time"
	"github.com/go-tron/socket"
	"github.com/go-tron/socket/pb"
	"google.golang.org/protobuf/types/known/anypb"
	"time"
)

func main() {
	producer := socket.NewProducerGrpc("127.0.0.1:10011")

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
