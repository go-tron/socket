package socket

import (
	"github.com/go-tron/config"
	"github.com/go-tron/etcd"
	"github.com/go-tron/logger"
)

type discovery interface {
	nodeRegister(nodeName string, addr string)
	nodeAddSubscribe() chan map[string]string
	nodeRemoveSubscribe() chan string
}

type DiscoveryEtcdConfig struct {
	AppName      string
	TTL          int64
	EtcdInstance *etcd.Client
	EtcdConfig   *etcd.Config
}
type DiscoveryEtcdOption func(*DiscoveryEtcdConfig)

func DiscoveryEtcdWithEtcdInstance(val *etcd.Client) DiscoveryEtcdOption {
	return func(conf *DiscoveryEtcdConfig) {
		conf.EtcdInstance = val
	}
}
func DiscoveryEtcdWithEtcdConfig(val *etcd.Config) DiscoveryEtcdOption {
	return func(conf *DiscoveryEtcdConfig) {
		conf.EtcdConfig = val
	}
}

func NewDiscoveryEtcdWithConfig(c *config.Config, opts ...DiscoveryEtcdOption) *DiscoveryEtcd {
	conf := &DiscoveryEtcdConfig{
		AppName: c.GetString("application.name"),
		TTL:     c.GetInt64("socket.discovery.etcd.ttl"),
		EtcdConfig: &etcd.Config{
			Endpoints:   c.GetStringSlice("socket.discovery.etcd.config.endpoints"),
			Username:    c.GetString("socket.discovery.etcd.config.username"),
			Password:    c.GetString("socket.discovery.etcd.config.password"),
			DialTimeout: c.GetDuration("socket.discovery.etcd.config.dialTimeout"),
			Logger:      logger.NewZapWithConfig(c, "discovery-etcd", "error"),
		},
	}
	for _, apply := range opts {
		apply(conf)
	}
	return NewDiscoveryEtcd(conf)
}

func NewDiscoveryEtcd(conf *DiscoveryEtcdConfig, opts ...DiscoveryEtcdOption) *DiscoveryEtcd {
	for _, apply := range opts {
		if apply != nil {
			apply(conf)
		}
	}
	if conf.AppName == "" {
		panic("AppName 必须设置")
	}
	if conf.EtcdInstance == nil {
		if conf.EtcdConfig == nil {
			panic("请设置redis实例或者连接配置")
		}
		conf.EtcdInstance = etcd.New(conf.EtcdConfig)
	}
	if conf.TTL == 0 {
		conf.TTL = 15
	}

	d, err := etcd.NewDiscovery(&etcd.DiscoveryConfig{
		AppName:      conf.AppName,
		EtcdInstance: conf.EtcdInstance,
	})
	if err != nil {
		panic(err)
	}

	return &DiscoveryEtcd{
		appName: conf.AppName,
		RegisterConfig: &etcd.RegisterConfig{
			AppName:      conf.AppName,
			TTL:          conf.TTL,
			EtcdInstance: conf.EtcdInstance,
		},
		Discovery: d,
	}
}

type DiscoveryEtcd struct {
	appName string
	ttl     int64
	*etcd.RegisterConfig
	*etcd.Discovery
}

func (s *DiscoveryEtcd) nodeRegister(nodeName string, addr string) {
	s.RegisterConfig.Node = nodeName
	s.RegisterConfig.Addr = addr
	_, err := etcd.NewRegister(s.RegisterConfig)
	if err != nil {
		panic(err)
	}
}

func (s *DiscoveryEtcd) nodeAddSubscribe() chan map[string]string {
	return s.Discovery.AddSubscribe()
}

func (s *DiscoveryEtcd) nodeRemoveSubscribe() chan string {
	return s.Discovery.RemoveSubscribe()
}
