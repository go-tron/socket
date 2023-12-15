package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-tron/etcd"
	localTime "github.com/go-tron/local-time"
	"github.com/go-tron/logger"
	"github.com/go-tron/nsq"
	"github.com/go-tron/redis"
	"github.com/go-tron/snowflake-id"
	"github.com/go-tron/socket"
	"github.com/go-tron/socket/example/websocket/pb"
	socketpb "github.com/go-tron/socket/pb"
	"github.com/spf13/pflag"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"net/http"
	"strconv"
	"time"
)

var socketPortFlag = pflag.StringP("socketPort", "s", "9000", "specify the socketPort")
var dispatchPortFlag = pflag.StringP("dispatchPort", "d", "10000", "specify the dispatchPort")

func main() {

	pflag.Parse()
	socketPort := *socketPortFlag
	dispatchPort := *dispatchPortFlag

	var appName = "test-app"
	var nodeName = "node-" + socketPort
	var nodeIP = "127.0.0.1"

	var redisClient = redis.New(&redis.Config{
		Addr:     "127.0.0.1:6379",
		Password: "GBkrIO9bkOcWrdsC",
	})
	var NsqProducer = nsq.NewProducer(&nsq.ProducerConfig{
		NsqdAddr:  "127.0.0.1:4150",
		NsqLogger: logger.NewZap("nsq-producer", "error"),
		MsgLogger: logger.NewZap("mq-producer", "info"),
	})
	var etcdClient = etcd.New(&etcd.Config{
		Endpoints:   []string{"http://127.0.0.1:10179", "http://127.0.0.1:10279", "http://127.0.0.1:10379"},
		Username:    "root",
		Password:    "Pf*rm1D^V&hBDAKC",
		DialTimeout: 5 * time.Second,
	})

	var server socket.Server
	server = socket.NewWebSocket(
		&socket.Config{
			AppName: appName,
			AppPort: socketPort,
			//HeartbeatTimeout: 20,
			NodeName:      nodeName,
			ClientLogger:  logger.NewZap("client", "info"),
			MessageLogger: logger.NewZap("message", "info"),
			SendAttempt: &socket.SendAttempt{
				SendAttemptDelay: time.Second,
				SendMaxAttempts:  5,
			},
		},
		socket.WithMessageStorage(socket.NewMessageStorageRedis(&socket.MessageStorageRedisConfig{
			AppName:       appName,
			RedisInstance: redisClient,
		})),
		socket.WithClientStorage(socket.NewClientStorageRedis(&socket.ClientStorageRedisConfig{
			AppName:       appName,
			RedisInstance: redisClient,
		})),
		socket.WithMessageIdGenerator(snowflakeId.New(0)),
		socket.WithTextMessageHandler(func(client *socket.Client, msg *socket.JsonMessage, data []byte) (err error) {
			fmt.Println("WithTextMessageHandler", client.ClientId, msg)
			bytes, err := json.Marshal(msg.Body.Content)
			var mm map[string]string
			if err := json.Unmarshal(bytes, &mm); err != nil {
				return nil
			}

			cmd := pb.ClientCmd(msg.Body.Cmd)
			if cmd == pb.ClientCmd_ClientCmdLogin {
				loginContent, err := anypb.New(&pb.ClientLogin{
					Token: mm["token"],
				})
				clientId, err := Authorize(client, &socketpb.Message{
					Body: &socketpb.MessageBody{
						Cmd:     1,
						Content: loginContent,
					},
				})
				if err != nil {
					client.AuthorizeFailed(err)
				} else {
					client.Authorize(clientId, mm["token"])
				}
				return nil
			}
			defer func() {
				var result = "发送成功"
				if err != nil {
					result = "发送失败:" + err.Error()
				}
				client.Conn.Send([]byte(result))
			}()

			if mm["clientId"] == "" {
				return errors.New("clientId不能为空")
			}
			if mm["qom"] == "" {
				return errors.New("qom不能为空")
			}
			if mm["body"] == "" {
				return errors.New("body不能为空")
			}
			content, err := anypb.New(&socketpb.Text{
				Message: mm["body"],
			})

			m := &socketpb.Message{
				Id: mm["id"],
				Body: &socketpb.MessageBody{
					Cmd:     10,
					Content: content,
				},
				ClientId: mm["clientId"],
			}

			qom, err := strconv.ParseInt(mm["qom"], 10, 32)
			if err != nil {
				return err
			}
			m.Qom = uint32(qom)
			if mm["expire"] != "" {
				expireIn, err := strconv.Atoi(mm["expire"])
				if err != nil {
					return err
				}
				ex := localTime.Now().Add(time.Second * time.Duration(expireIn))
				m.ExpireAt = ex.ToTimestamppb()
			}
			return client.Send(m)
		}),
		socket.WithBinaryMessageHandler(func(client *socket.Client, msg *socketpb.Message, data []byte) (err error) {
			cmd := pb.ClientCmd(msg.Body.Cmd)
			if cmd == pb.ClientCmd_ClientCmdLogin {
				clientId, err := Authorize(client, msg)
				if err != nil {
					client.AuthorizeFailed(err)
				} else {
					client.Authorize(clientId, clientId)
				}
				return nil
			}
			return NsqProducer.SendSync("client-message", data)
		}),
		socket.WithDispatch(socket.NewDispatchGrpc(&socket.DispatchGrpcConfig{
			NodeName: nodeName,
			IP:       nodeIP,
			Port:     dispatchPort,
			Discovery: socket.NewDiscoveryEtcd(&socket.DiscoveryEtcdConfig{
				AppName:      appName,
				TTL:          15,
				EtcdInstance: etcdClient,
			}),
			ClientStorage: socket.NewClientStorageRedis(&socket.ClientStorageRedisConfig{
				AppName:       appName,
				RedisInstance: redisClient,
			}),
		})),
	)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./public/index.html")
	})
	err := server.Serve()
	if err != nil {
		panic(err)
	}
}

func Authorize(client *socket.Client, msg *socketpb.Message) (string, error) {
	login := &pb.ClientLogin{}
	if msg.Body == nil || msg.Body.Content == nil {
		return "", errors.New("login content empty")
	}
	if err := msg.Body.Content.UnmarshalTo(login); err != nil {
		return "", errors.New("token invalid")
	}
	clientId := login.Token

	content, _ := anypb.New(&pb.ServerLogin{
		ClientId: clientId,
	})
	m := &socketpb.Message{
		Body: &socketpb.MessageBody{
			Cmd:     uint32(pb.ServerCmd_ServerCmdLogin),
			Content: content,
		},
		ClientId: clientId,
	}
	bytes, err := proto.Marshal(m)
	if err != nil {
		return "", errors.New("failed")
	}
	client.Conn.Send(bytes)
	return clientId, nil
}
