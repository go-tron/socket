package socket

import (
	"context"
	"github.com/go-tron/config"
	"github.com/go-tron/redis"
	"time"
)

type clientStorage interface {
	setStatusOnline(ctx context.Context, clientId string, nodeName string)
	setStatusOffline(clientId string, nodeName string)
	getStatus(clientId string) (nodeName string, err error)
	resetNode(nodeName string)
}

type ClientStorageRedisConfig struct {
	AppName       string
	TTL           time.Duration
	RedisInstance *redis.Redis
	RedisConfig   *redis.Config
}
type ClientStorageRedisOption func(*ClientStorageRedisConfig)

func ClientStorageRedisWithRedisInstance(val *redis.Redis) ClientStorageRedisOption {
	return func(conf *ClientStorageRedisConfig) {
		conf.RedisInstance = val
	}
}
func ClientStorageRedisWithRedisConfig(val *redis.Config) ClientStorageRedisOption {
	return func(conf *ClientStorageRedisConfig) {
		conf.RedisConfig = val
	}
}

func NewClientStorageRedisWithConfig(c *config.Config, opts ...ClientStorageRedisOption) *ClientStorageRedis {
	conf := &ClientStorageRedisConfig{
		AppName: c.GetString("application.name"),
		TTL:     c.GetDuration("socket.clientStorage.redis.ttl"),
		RedisConfig: &redis.Config{
			Addr:         c.GetString("socket.clientStorage.redis.config.addr"),
			Password:     c.GetString("socket.clientStorage.redis.config.password"),
			Database:     c.GetInt("socket.clientStorage.redis.config.database"),
			PoolSize:     c.GetInt("socket.clientStorage.redis.config.poolSize"),
			MinIdleConns: c.GetInt("socket.clientStorage.redis.config.minIdleConns"),
		},
	}
	for _, apply := range opts {
		apply(conf)
	}
	return NewClientStorageRedis(conf)
}

func NewClientStorageRedis(conf *ClientStorageRedisConfig, opts ...ClientStorageRedisOption) *ClientStorageRedis {
	for _, apply := range opts {
		if apply != nil {
			apply(conf)
		}
	}
	if conf.AppName == "" {
		panic("AppName 必须设置")
	}
	if conf.RedisInstance == nil {
		if conf.RedisConfig == nil {
			panic("请设置redis实例或者连接配置")
		}
		conf.RedisInstance = redis.New(conf.RedisConfig)
	}
	if conf.TTL == 0 {
		conf.TTL = time.Second * 30
	}

	return &ClientStorageRedis{
		Redis:           conf.RedisInstance,
		TTL:             conf.TTL,
		ClientStatusKey: conf.AppName + "-client",
		NodeListKey:     conf.AppName + "-node",
	}
}

type ClientStorageRedis struct {
	Redis           *redis.Redis
	TTL             time.Duration
	ClientStatusKey string //客户端连接状态键名
	NodeListKey     string //节点列表键名
}

func (rs *ClientStorageRedis) setStatusOnline(ctx context.Context, clientId string, nodeName string) {
	go func() {
		rs.Redis.Set(context.Background(), rs.ClientStatusKey+":"+clientId, nodeName, rs.TTL).Result()
		rs.Redis.SAdd(context.Background(), rs.NodeListKey+":"+nodeName, clientId).Result()
		for {
			select {
			case <-ctx.Done():
				rs.Redis.SRem(context.Background(), rs.NodeListKey+":"+nodeName, clientId).Result()
				return
			case <-time.After(rs.TTL / 3):
				rs.Redis.Set(context.Background(), rs.ClientStatusKey+":"+clientId, nodeName, rs.TTL).Result()
			}
		}
	}()
}

func (rs *ClientStorageRedis) setStatusOffline(clientId string, nodeName string) {
	rs.Redis.SRem(context.Background(), rs.NodeListKey+":"+nodeName, clientId).Result()
	rs.Redis.Set(context.Background(), rs.ClientStatusKey+":"+clientId, "", 0).Result()
}

func (rs *ClientStorageRedis) getStatus(clientId string) (string, error) {
	val, err := rs.Redis.Get(context.Background(), rs.ClientStatusKey+":"+clientId).Result()
	if err != nil && err != redis.Nil {
		return "", err
	}
	return val, nil
}

func (rs *ClientStorageRedis) resetNode(nodeName string) {
	rs.Redis.Del(context.Background(), rs.NodeListKey+":"+nodeName)
}
