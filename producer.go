package socket

import (
	"context"
	"github.com/go-tron/config"
	"github.com/go-tron/etcd"
	"github.com/go-tron/logger"
	"github.com/go-tron/redis"
	"github.com/go-tron/socket/pb"
	"sync"
	"time"
)

type ProducerGrpcConfig struct {
	AppName       string
	DefaultAddr   string
	EtcdConfig    *etcd.Config
	EtcdInstance  *etcd.Client
	RedisConfig   *redis.Config
	RedisInstance *redis.Redis
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
func ProducerGrpcWithRedisConfig(val *redis.Config) ProducerGrpcOption {
	return func(opts *ProducerGrpcConfig) {
		opts.RedisConfig = val
	}
}
func ProducerGrpcWithRedisInstance(val *redis.Redis) ProducerGrpcOption {
	return func(opts *ProducerGrpcConfig) {
		opts.RedisInstance = val
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
		RedisConfig: &redis.Config{
			Addr:         c.GetString("redis.addr"),
			Password:     c.GetString("redis.password"),
			Database:     c.GetInt("redis.database"),
			PoolSize:     c.GetInt("redis.poolSize"),
			MinIdleConns: c.GetInt("redis.minIdleConns"),
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
	if config.DefaultAddr == "" {
		panic("DefaultAddr 必须设置")
	}
	if config.EtcdInstance == nil {
		if config.EtcdConfig == nil {
			panic("请设置etcd实例或者连接配置")
		}
		config.EtcdInstance = etcd.New(config.EtcdConfig)
	}

	if config.RedisInstance == nil {
		if config.RedisConfig == nil {
			panic("请设置redis实例或者连接配置")
		}
		config.RedisInstance = redis.New(config.RedisConfig)
	}

	s := &ProducerServerGrpc{
		NodeList: &sync.Map{},
		clientStorage: NewClientStorageRedis(&ClientStorageRedisConfig{
			AppName:       config.AppName,
			RedisInstance: config.RedisInstance,
		}),
	}
	defaultClient, err := NewDispatchGrpcClient(config.DefaultAddr)
	if err != nil {
		panic(err)
	}
	s.DefaultNode = defaultClient

	discovery := etcd.MustNewDiscovery(&etcd.DiscoveryConfig{
		AppName:      config.AppName,
		EtcdInstance: config.EtcdInstance,
	})

	go func() {
		for v := range discovery.AddSubscribe() {
			for nodeName, addr := range v {
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

type ProducerServerGrpc struct {
	DefaultNode   *DispatchGrpcClient
	NodeList      *sync.Map
	clientStorage clientStorage
}

func (s *ProducerServerGrpc) findNode(clientId string) *DispatchGrpcClient {
	nodeName, _ := s.clientStorage.getStatus(clientId)
	if nodeName != "" {
		value, ok := s.NodeList.Load(nodeName)
		if ok {
			return value.(*DispatchGrpcClient)
		}
	}
	return s.DefaultNode
}

func (s *ProducerServerGrpc) Publish(msg *pb.Message) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	_, err = s.findNode(msg.ClientId).Client.MessageSrv(ctx, msg)
	return err
}
