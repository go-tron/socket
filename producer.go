package socket

import (
	"context"
	"github.com/go-tron/config"
	"github.com/go-tron/etcd"
	"github.com/go-tron/logger"
	"github.com/go-tron/socket/pb"
	"sync"
	"time"
)

type ProducerGrpcConfig struct {
	AppName       string
	EtcdConfig    *etcd.Config
	EtcdInstance  *etcd.Client
	ClientStorage clientStorage
}
type ProducerGrpcOption func(*ProducerGrpcConfig)

func ProducerGrpcWithEtcdConfig(val *etcd.Config) ProducerGrpcOption {
	return func(conf *ProducerGrpcConfig) {
		conf.EtcdConfig = val
	}
}
func ProducerGrpcWithEtcdInstance(val *etcd.Client) ProducerGrpcOption {
	return func(conf *ProducerGrpcConfig) {
		conf.EtcdInstance = val
	}
}
func NewProducerGrpcWithConfig(c *config.Config, opts ...ProducerGrpcOption) *ProducerServerGrpc {
	conf := &ProducerGrpcConfig{
		AppName: c.GetString("application.name"),
		EtcdConfig: &etcd.Config{
			Endpoints:   c.GetStringSlice("etcd.endpoints"),
			Username:    c.GetString("etcd.username"),
			Password:    c.GetString("etcd.password"),
			DialTimeout: c.GetDuration("etcd.dialTimeout"),
			Logger:      logger.NewZapWithConfig(c, "etcd", "error"),
		},
	}
	return NewProducerGrpc(conf, opts...)
}

func NewProducerGrpc(config *ProducerGrpcConfig, opts ...ProducerGrpcOption) *ProducerServerGrpc {
	for _, apply := range opts {
		if apply != nil {
			apply(config)
		}
	}
	if config.AppName == "" {
		panic("AppName 必须设置")
	}
	if config.EtcdInstance == nil {
		if config.EtcdConfig == nil {
			panic("请设置etcd实例或者连接配置")
		}
		config.EtcdInstance = etcd.New(config.EtcdConfig)
	}

	s := &ProducerServerGrpc{
		clientStorage: config.ClientStorage,
		NodeList:      &sync.Map{},
	}

	discovery := etcd.MustNewDiscovery(&etcd.DiscoveryConfig{
		AppName:      config.AppName,
		EtcdInstance: config.EtcdInstance,
	})

	go func() {
		for v := range discovery.AddSubscribe() {
			for nodeName, addr := range v {
				node, ok := s.NodeList.LoadAndDelete(nodeName)
				if ok {
					node.(*DispatchGrpcClient).Conn.Close()
				}
				client, err := NewDispatchGrpcClient(addr)
				if err == nil {
					s.NodeList.Store(nodeName, client)
				}
			}
		}
	}()
	go func() {
		for nodeName := range discovery.RemoveSubscribe() {
			node, ok := s.NodeList.LoadAndDelete(nodeName)
			if ok {
				node.(*DispatchGrpcClient).Conn.Close()
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

type ProducerServerGrpc struct {
	NodeList      *sync.Map
	clientStorage clientStorage
}

func (s *ProducerServerGrpc) Publish(msg *pb.Message) (err error) {
	//获取客户端连接状态
	nodeName, err := s.clientStorage.getStatus(msg.ClientId)
	if err != nil {
		return ErrorGetClientStatus(err.Error())
	}
	if nodeName == "" {
		//客户端离线直接返回 消息已存时待上线再发
		return ErrorClientOffline
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
