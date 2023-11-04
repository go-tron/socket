package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/go-tron/local-time"
	"github.com/go-tron/socket/pb"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"log"
	"time"

	"google.golang.org/grpc"
)

var (
	addr = flag.String("addr", "127.0.0.1:6666", "the address to connect to")
)

var kacp = keepalive.ClientParameters{
	Time:                10 * time.Second, // send pings every 10 seconds if there is no activity
	Timeout:             time.Second,      // wait 1 second for ping ack before considering the connection dead
	PermitWithoutStream: false,            // send pings even without active streams
}

func main() {
	flag.Parse()
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithKeepaliveParams(kacp))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewDispatchClient(conn)

	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()
			r, err := client.MessageSrv(ctx, &pb.Message{
				Id:  "234",
				Qom: 1,
				Body: &pb.MessageBody{
					Cmd:     222,
					Content: nil,
				},
				ExpireAt: localTime.Now().ToTimestamppb(),
				ClientId: "3",
			})
			if err != nil {
				log.Printf("messageClient error: %v", err)
			}
			log.Printf("messageClient: %s", r.GetMessage())
			time.Sleep(time.Second)
		}
	}()

	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			defer cancel()
			r, err := client.ClientSrv(ctx, &pb.Client{
				NodeName: "123323",
				ClientId: "3",
			})
			if err != nil {
				log.Printf("clientEventClient error: %v", err)
			}
			log.Printf("clientEventClient: %s", r.GetMessage())
			time.Sleep(time.Second)
		}
	}()

	go func() {
		for {
			fmt.Println(conn.GetState())
			time.Sleep(time.Second)
		}
	}()

	select {}
}
