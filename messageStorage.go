package socket

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-tron/config"
	"github.com/go-tron/local-time"
	"github.com/go-tron/redis"
	"github.com/go-tron/socket/pb"
	"google.golang.org/protobuf/proto"
	"strconv"
	"strings"
)

type messageStorage interface {
	getClientMessageList(clientId string) (msgList []*WrappedMessage, summary string, err error)
	getMessage(id string) (*WrappedMessage, error)
	checkMessageUnique(id string) (ok bool, err error)
	revertMessageUnique(id string) (err error)
	saveMessage(msg *WrappedMessage) error
	attemptSendMessage(id string, sendMaxAttempts uint) (uint, error)
	receiveMessage(msg *WrappedMessage, persistence bool) error
	removeMessage(msg *WrappedMessage, persistence bool, reason string) error
}

type MessageStorageRedisConfig struct {
	AppName       string
	RedisInstance *redis.Redis
	RedisConfig   *redis.Config
}
type MessageStorageRedisOption func(*MessageStorageRedisConfig)

func MessageStorageRedisWithRedisInstance(val *redis.Redis) MessageStorageRedisOption {
	return func(conf *MessageStorageRedisConfig) {
		conf.RedisInstance = val
	}
}
func MessageStorageRedisWithRedisConfig(val *redis.Config) MessageStorageRedisOption {
	return func(conf *MessageStorageRedisConfig) {
		conf.RedisConfig = val
	}
}

func NewMessageStorageRedisWithConfig(c *config.Config, opts ...MessageStorageRedisOption) *MessageStorageRedis {
	conf := &MessageStorageRedisConfig{
		AppName: c.GetString("application.name"),
		RedisConfig: &redis.Config{
			Addr:         c.GetString("socket.messageStorage.redis.config.addr"),
			Password:     c.GetString("socket.messageStorage.redis.config.password"),
			Database:     c.GetInt("socket.messageStorage.redis.config.database"),
			PoolSize:     c.GetInt("socket.messageStorage.redis.config.poolSize"),
			MinIdleConns: c.GetInt("socket.messageStorage.redis.config.minIdleConns"),
		},
	}
	for _, apply := range opts {
		apply(conf)
	}
	return NewMessageStorageRedis(conf)
}

func NewMessageStorageRedis(conf *MessageStorageRedisConfig, opts ...MessageStorageRedisOption) *MessageStorageRedis {
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

	return &MessageStorageRedis{
		Redis:            conf.RedisInstance,
		MessageStoreKey:  conf.AppName + "-message",
		MessageUniqueKey: conf.AppName + "-message-unique",
		MessageClientKey: conf.AppName + "-message-client",
	}
}

type MessageStorageRedis struct {
	Redis            *redis.Redis
	MessageStoreKey  string //消息存储键名
	MessageUniqueKey string //消息ID唯一校验键名
	MessageClientKey string //客户端消息ID存储键名
}

func (rs *MessageStorageRedis) getClientMessageList(clientId string) (msgList []*WrappedMessage, summary string, err error) {
	var (
		count             = 0
		loadErrorCount    = 0
		removedErrorCount = 0
		parseErrorCount   = 0
		expiredErrorCount = 0
	)
	defer func() {
		summary = "历史消息:"
		if count > 0 {
			summary += fmt.Sprintf("%d", count)
		} else {
			summary += "无"
		}
		if loadErrorCount > 0 {
			summary += fmt.Sprintf(" 读取失败:%d", loadErrorCount)
		}
		if removedErrorCount > 0 {
			summary += fmt.Sprintf(" 已被清理:%d", removedErrorCount)
		}
		if parseErrorCount > 0 {
			summary += fmt.Sprintf(" 数据错误:%d", parseErrorCount)
		}
		if expiredErrorCount > 0 {
			summary += fmt.Sprintf(" 已过期:%d", expiredErrorCount)
		}
	}()
	idList, err := rs.Redis.LRange(context.Background(), rs.MessageClientKey+":"+clientId, 0, -1).Result()
	if err != nil {
		return msgList, summary, err
	}

	count = len(idList)
	for _, id := range idList {
		msg, err := rs.getMessage(id)
		if err != nil {
			rs.Redis.LRem(context.Background(), rs.MessageClientKey+":"+clientId, 0, id)
			var reason = err.Error()
			if reason == "load" {
				loadErrorCount++
			} else {
				msg.ClientId = clientId
				if strings.HasPrefix(reason, "parse") {
					parseErrorCount++
				} else if reason == "removed" {
					removedErrorCount++
				} else if reason == "expired" {
					expiredErrorCount++
				}
			}
		}
		msgList = append(msgList, msg)
	}
	return msgList, summary, err
}

func (rs *MessageStorageRedis) getMessage(id string) (msg *WrappedMessage, err error) {
	msg = &WrappedMessage{
		Message: &pb.Message{
			Id: id,
		},
	}
	defer func() {
		msg.loadErr = err
	}()

	m, err := rs.Redis.HGetAll(context.Background(), rs.MessageStoreKey+":"+id).Result()
	if err != nil {
		return msg, errors.New("load")
	}
	if m["clientId"] == "" || m["body"] == "" {
		return msg, errors.New("removed")
	}
	msg.ClientId = m["clientId"]

	body := &pb.MessageBody{}
	if err := proto.Unmarshal([]byte(m["body"]), body); err != nil {
		return msg, errors.New("unmarshal")
	}
	msg.Body = body

	qom, err := strconv.ParseInt(m["qom"], 10, 32)
	if err != nil {
		return msg, errors.New("parse qom error")
	}
	msg.Qom = uint32(qom)

	attempts, err := strconv.ParseUint(m["attempts"], 10, 64)
	if err != nil {
		return msg, errors.New("parse attempts error")
	}
	msg.attempts = uint(attempts)

	arrived, err := strconv.Atoi(m["arrived"])
	if err != nil {
		return msg, errors.New("parse arrived error")
	}
	msg.arrived = arrived

	if m["expireAt"] != "" {
		expireAt, err := localTime.ParseLocal(m["expireAt"])
		if err != nil {
			return msg, errors.New("parse expireAt error")
		}
		if expireAt.Before(localTime.Now()) {
			return msg, errors.New("expired")
		}
		msg.ExpireAt = expireAt.ToTimestamppb()
	}
	return msg, nil
}

func (rs *MessageStorageRedis) checkMessageUnique(id string) (bool, error) {
	return rs.Redis.HSetNX(context.Background(), rs.MessageUniqueKey, id, 1).Result()
}

func (rs *MessageStorageRedis) revertMessageUnique(id string) error {
	_, err := rs.Redis.HDel(context.Background(), rs.MessageUniqueKey, id).Result()
	return err
}

func (rs *MessageStorageRedis) saveMessage(msg *WrappedMessage) error {
	var (
		expireAt           = ""
		expireAtUnix int64 = 0
	)
	if msg.ExpireAt != nil {
		expireTime := localTime.FromTimestamppb(msg.ExpireAt)
		fmt.Println(expireTime)
		expireAt = expireTime.String()
		if !msg.checkNeedPersistence() {
			expireAtUnix = expireTime.Unix()
		}
	}

	body, err := proto.Marshal(msg.Body)
	if err != nil {
		return err
	}
	_, err = saveMessage.Run(context.Background(), rs.Redis, []string{rs.MessageStoreKey, rs.MessageClientKey}, msg.Id, msg.ClientId, expireAt, body, msg.Qom, expireAtUnix).Result()
	if err != nil {
		return err
	}
	return nil
}

func (rs *MessageStorageRedis) attemptSendMessage(id string, sendMaxAttempts uint) (uint, error) {
	v, err := attemptSendMessage.Run(context.Background(), rs.Redis, []string{rs.MessageStoreKey}, id, sendMaxAttempts).Result()
	if err != nil {
		return 0, err
	}
	return uint(v.(int64)), nil
}

func (rs *MessageStorageRedis) receiveMessage(msg *WrappedMessage, persistence bool) error {
	p := ""
	if persistence {
		p = "1"
	}
	_, err := AckMessage.Run(context.Background(), rs.Redis, []string{rs.MessageStoreKey, rs.MessageClientKey}, msg.Id, msg.ClientId, p).Result()
	return err
}
func (rs *MessageStorageRedis) removeMessage(msg *WrappedMessage, persistence bool, reason string) error {
	p := ""
	if persistence {
		p = "1"
	}
	_, err := removeMessage.Run(context.Background(), rs.Redis, []string{rs.MessageStoreKey, rs.MessageClientKey}, msg.Id, msg.ClientId, p, reason).Result()
	return err
}

var saveMessage = redis.NewScript(`
--[[/*
* KEYS[1] message-key
* KEYS[2] client-message-key
* ARGV[1] id
* ARGV[2] clientId
* ARGV[3] expireAt
* ARGV[4] body
* ARGV[5] qom
* ARGV[6] expireAtUnix
* result 1
*/]]
redis.call("hmset", KEYS[1] .. ":" .. ARGV[1], "clientId", ARGV[2], "expireAt", ARGV[3], "body", ARGV[4], "qom", ARGV[5], "attempts", 0, "arrived", 0)
redis.call("rpush", KEYS[2] .. ":" .. ARGV[2], ARGV[1])
if ARGV[6] ~= "0" then
    return redis.call('EXPIREAT', KEYS[1] .. ":" .. ARGV[1], ARGV[6])
end
return 1
`)

var attemptSendMessage = redis.NewScript(`
--[[/*
* KEYS[1] message-key
* ARGV[1] id
* ARGV[2] maxAttempts
* result currentAttempts
*/]]
local result = redis.call('hmget', KEYS[1] .. ":" .. ARGV[1], "clientId", "arrived", "attempts")
if not result[1] then
	return redis.error_reply("not exists:" ..  ARGV[1])
end
if result[2] == "1" then
	return redis.error_reply("has arrived:" ..  ARGV[1])
end
if tonumber(result[3]) + 1 > tonumber(ARGV[2]) then
	return redis.error_reply("attempts reach limit")
end
return redis.call('hincrby', KEYS[1] .. ":" .. ARGV[1], "attempts", 1)
`)

var AckMessage = redis.NewScript(`
--[[/*
* KEYS[1] message-key
* KEYS[2] client-message-key
* ARGV[1] id
* ARGV[2] clientId
* ARGV[3] needPersistence
* result removed number
*/]]
local result = redis.call('hmget', KEYS[1] .. ":" .. ARGV[1], "clientId", "qom")
if not result[1] then
	if ARGV[2] then
		redis.call("lrem", KEYS[2] .. ":" .. ARGV[2], 0, ARGV[1])
	end
	return redis.error_reply("not exists:" ..  ARGV[1])
end
redis.call('hset', KEYS[1] .. ":" .. ARGV[1], "arrived", 1)
if ARGV[3] == "1" then
	redis.call('hset', KEYS[1] .. ":" .. ARGV[1], "result", "ok")
	redis.call("rename", KEYS[1] .. ":" .. ARGV[1], KEYS[1] .. "-archived:" .. ARGV[1])
	redis.call("rpush", KEYS[2] .."-archived:" .. result[1], ARGV[1])
else
	redis.call('expire', KEYS[1] .. ":" .. ARGV[1], 10)
end
return redis.call("lrem", KEYS[2] .. ":" .. result[1], 0, ARGV[1])
`)

var removeMessage = redis.NewScript(`
--[[/*
* KEYS[1] message-key
* KEYS[2] client-message-key
* ARGV[1] id
* ARGV[2] clientId
* ARGV[3] needPersistence
* ARGV[4] reason
* result removed number
*/]]
local result = redis.call('hmget', KEYS[1] .. ":" .. ARGV[1], "clientId", "qom")
if not result[1] then
	if ARGV[2] then
		redis.call("lrem", KEYS[2] .. ":" .. ARGV[2], 0, ARGV[1])
	end
	return redis.error_reply("not exists:" ..  ARGV[1])
end
if ARGV[3] == "1" then
	redis.call('hset', KEYS[1] .. ":" .. ARGV[1], "result", ARGV[4])
	redis.call("rename", KEYS[1] .. ":" .. ARGV[1], KEYS[1] .. "-archived:" .. ARGV[1])
	redis.call("rpush", KEYS[2] .."-archived:" .. result[1], ARGV[1])
else
	redis.call("del", KEYS[1] .. ":" .. ARGV[1])
end
return redis.call("lrem", KEYS[2] .. ":" .. result[1], 0, ARGV[1])
`)
