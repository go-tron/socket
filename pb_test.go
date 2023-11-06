package socket

import (
	"context"
	"fmt"
	"github.com/go-tron/redis"
	"github.com/go-tron/socket/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
)

func TestMessage_MarshalJSON(t *testing.T) {
	loginContent, err := anypb.New(&pb.Text{
		Message: "fkkk",
	})
	if err != nil {
		panic(err)
	}

	msg := &pb.Message{
		Id:  "2",
		Qom: 3,
		Body: &pb.MessageBody{
			Cmd:     1,
			Content: loginContent,
		},
		ExpireAt: timestamppb.Now(),
		ClientId: "2",
		Ack:      false,
	}

	bytes, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}

	var client = redis.New(&redis.Config{
		Addr:     "127.0.0.1:6379",
		Password: "GBkrIO9bkOcWrdsC",
	})
	_, err = client.Set(context.Background(), "test-a", bytes, 0).Result()
	if err != nil {
		panic(err)
	}

	val, err := client.Get(context.Background(), "test-a").Result()
	if err != nil {
		panic(err)
	}

	val = "asdasdasdsads"
	m := &pb.Message{}
	if err := proto.Unmarshal([]byte(val), m); err != nil {
		panic(err)
	}
	fmt.Println("Cmd", m.Body.Cmd)

	a := &pb.Text{}
	if err := m.Body.Content.UnmarshalTo(a); err != nil {
		panic(err)
	}
	fmt.Println("a", a)
}
