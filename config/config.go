package config

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/Soul-Mate/yc-snowflake/snowflake"
	"io/ioutil"
	"os"
	"strconv"
)

type fileConfig struct {
	LogFile   string `json:"log_file"`
	WorkerId  int    `json:"worker_id"`
	SkipCheck bool   `json:"no_worker_skip_check"`
	Etcd      struct {
		EnableTLS      bool   `json:"enable_tls"`
		Insecure       bool   `json:"Insecure"`
		CACert         string `json:"ca_cert"`
		ClientCertAuth bool   `json:"client_cert_auth"`
		ClientCert     string `json:"client_cert"`
		ClientKey      string `json:"client_key"`
		Cluster        string `json:"Cluster"`
	} `json:"etcd"`
	GRpc struct {
		Host       string `json:"Host"`
		Port       string `json:"Port"`
		CertFile   string `json:"cert_file"`
		KeyFile    string `json:"key_file"`
		ServerName string `json:"server_name"`
	} `json:"grpc"`
	Http struct {
		Host       string `json:"Host"`
		Port       string `json:"Port"`
		ClientAuth bool   `json:"client_auth"`
		ServerAuth bool   `json:"server_auth"`
		CaFile     string `json:"ca_file"`
		CertFile   string `json:"cert_file"`
		KeyFile    string `json:"key_file"`
	} `json:"http"`
}

type EtcdConfig struct {
	EnableTLS      bool
	Insecure       bool
	CaFile         string
	ClientAuth     bool
	ClientCertFile string
	ClientKeyFile  string
	Cluster        string
}

type gRpcConfig struct {
	EnableTls  bool
	Host       string
	Port       string
	CertFile   string
	KeyFile    string
	ServerName string
}

type httpConfig struct {
	Host       string
	Port       string
	ClientAuth bool
	serverAuth bool
	CaFile     string
	CertFile   string
	KeyFile    string
}

type Config struct {
	ConfigFile string
	LogFile    string
	WorkerId   int
	SkipCheck  bool
	Etcd       EtcdConfig
	GRpc       gRpcConfig
	Http       httpConfig
}

func (conf *Config) ParseCmd() {
	flag.StringVar(&conf.ConfigFile, "config-file", "", "use config file to init the program")
	flag.StringVar(&conf.LogFile, "log-file", "", "")
	flag.IntVar(&conf.WorkerId, "worker-id", 0, "")
	flag.BoolVar(&conf.SkipCheck, "no-worker-skip-check", false, "")
	flag.BoolVar(&conf.Etcd.EnableTLS, "etcd-enable-tls", false, "")
	flag.BoolVar(&conf.Etcd.Insecure, "etcd-insecure", false, "")
	flag.StringVar(&conf.Etcd.CaFile, "etcd-trusted-ca-file", "", "")
	flag.BoolVar(&conf.Etcd.ClientAuth, "etcd-client-cert-auth", false, "")
	flag.StringVar(&conf.Etcd.ClientCertFile, "etcd-cert-file", "", "")
	flag.StringVar(&conf.Etcd.ClientKeyFile, "etcd-key-file", "", "")
	flag.StringVar(&conf.Etcd.Cluster, "etcd-cluster", "", "")
	flag.StringVar(&conf.GRpc.Host, "grpc-Host", "", "")
	flag.StringVar(&conf.GRpc.Port, "grpc-Port", WORKER_GRPC_PORT_DEFAULT, "")
	flag.BoolVar(&conf.GRpc.EnableTls, "grpc-enable-tls", false, "")
	flag.StringVar(&conf.GRpc.CertFile, "grpc-cert-file", "", "")
	flag.StringVar(&conf.GRpc.KeyFile, "grpc-key-file", "", "")
	flag.StringVar(&conf.GRpc.ServerName, "grpc-server-name", "", "")
	flag.StringVar(&conf.Http.Host, "http-Host", "", "")
	flag.StringVar(&conf.Http.Port, "http-Port", WORKER_HTTP_PORT_DEFAULT, "")
	flag.BoolVar(&conf.Http.ClientAuth, "http-client-auth", false, "")
	flag.BoolVar(&conf.Http.serverAuth, "http-serve-auth", false, "")
	flag.StringVar(&conf.Http.CaFile, "http-ca-file", "", "")
	flag.StringVar(&conf.Http.CertFile, "http-cert-file", "", "")
	flag.StringVar(&conf.Http.KeyFile, "http-key-file", "", "")
	flag.Parse()
}

// 检查worker id config
func (conf *Config) CheckWorkerIdConfig() error {
	if conf.WorkerId == 0 {
		// 从环境变量中加载worker id的值
		if workerIdStr := os.Getenv(WORKER_ID_ENV_KEY); workerIdStr != "" {
			if workerIdInt, err := strconv.Atoi(workerIdStr); err != nil {
				return fmt.Errorf("the value in the %s env var seems to be invalid", WORKER_ID_ENV_KEY)
			} else {
				conf.WorkerId = workerIdInt
			}
		}
	}
	if conf.WorkerId <= 0 || conf.WorkerId > snowflake.MaxWorkerId+1 {
		return errors.New("worker id does not exist or is invalid")
	}

	return nil
}

// 从指定配置文件中初始化程序配置参数值 如果存在
func (conf *Config) InitConfigFromFile() error {
	var (
		b   []byte
		err error
	)

	if conf.ConfigFile == "" {
		if configFileEnv := os.Getenv(CONF_FILE_ENV_KEY); configFileEnv == "" {
			return nil
		} else {
			conf.ConfigFile = configFileEnv
		}
	}

	if b, err = ioutil.ReadFile(conf.ConfigFile); err != nil {
		return fmt.Errorf("config.InitConfigFromFile error: %v\n", err)
	}

	fc := &fileConfig{}

	if err = json.Unmarshal(b, fc); err != nil {
		return fmt.Errorf("config.InitConfigFromFile error: %v\n", err)
	}

	conf.WorkerId = fc.WorkerId
	conf.LogFile = fc.LogFile
	conf.SkipCheck = fc.SkipCheck

	conf.Etcd.Insecure = fc.Etcd.Insecure
	conf.Etcd.CaFile = fc.Etcd.CACert
	conf.Etcd.ClientAuth = fc.Etcd.ClientCertAuth
	conf.Etcd.EnableTLS = fc.Etcd.EnableTLS
	conf.Etcd.ClientCertFile = fc.Etcd.ClientCert
	conf.Etcd.ClientKeyFile = fc.Etcd.ClientKey
	conf.Etcd.Cluster = fc.Etcd.Cluster


	conf.Http.Host = fc.Http.Host
	conf.Http.Port = fc.Http.Port
	conf.Http.ClientAuth = fc.Http.ClientAuth
	conf.Http.serverAuth = fc.Http.ServerAuth
	conf.Http.CertFile = fc.Http.CertFile
	conf.Http.KeyFile = fc.Http.KeyFile

	conf.GRpc.Host = fc.GRpc.Host
	conf.GRpc.Port = fc.GRpc.Port
	conf.GRpc.CertFile = fc.GRpc.CertFile
	conf.GRpc.KeyFile = fc.GRpc.KeyFile
	conf.GRpc.ServerName = fc.GRpc.ServerName

	return nil
}

// 初始化http服务配置
func (conf *Config) InitHttpServiceConfig() error {
	if err := conf.checkHttpClientAuthConfig(); err != nil {
		return err
	}

	if conf.Http.Host == "" {
		if httpHostEnv := os.Getenv(WORKER_HTTP_HOST_ENV_KEY); httpHostEnv != "" {
			conf.Http.Host = httpHostEnv
		} else {
			conf.Http.Host = WORKER_HTTP_HOST_DEFAULT
		}
	}

	if conf.Http.Port == "" {
		if htpPortEnv := os.Getenv(WORKER_HTTP_PORT_ENV_KEY); htpPortEnv != "" {
			conf.Http.Port = htpPortEnv
		} else {
			conf.Http.Port = WORKER_HTTP_PORT_DEFAULT
		}
	}

	return nil
}

// 检验http服务开启client auth选项时, 所需配置参数值是否正确
func (conf *Config) checkHttpClientAuthConfig() error {
	if conf.Http.ClientAuth {
		// 缺少ca证书
		if conf.Http.CaFile == "" {
			return errors.New("enable -http-client-auth option, but lack of ca file.")
		}

		// 缺少server cert
		if conf.Http.CertFile == "" {
			return errors.New("enable  -http-client-auth option, but lack of cert file.")
		}

		// 缺少server key
		if conf.Http.KeyFile == "" {
			return errors.New("enable -http-client-auth option, but lack of key file.")
		}
	}
	return nil
}

// 初始化rpc服务配置
func (conf *Config) InitRpcServiceConfig() error {
	if err := conf.checkRpcEnableTlsConfig(); err != nil {
		return err
	}

	if conf.GRpc.Host == "" {
		if httpHostEnv := os.Getenv(WORKER_GRPC_HOST_ENV_KEY); httpHostEnv != "" {
			conf.GRpc.Host = httpHostEnv
		} else {
			conf.GRpc.Host = WORKER_GRPC_HOST_DEFAULT
		}
	}

	if conf.GRpc.Port == "" {
		if httpPortEnv := os.Getenv(WORKER_GRPC_PORT_ENV_KEY); httpPortEnv != "" {
			conf.GRpc.Port = httpPortEnv
		} else {
			conf.GRpc.Port = WORKER_GRPC_PORT_DEFAULT
		}
	}

	return nil
}

// 检验rpc服务开启tls选项是, 所需配置参数是否正确
func (conf *Config) checkRpcEnableTlsConfig() error {
	if conf.GRpc.EnableTls {
		// 缺少cert
		if conf.GRpc.CertFile == "" {
			return errors.New("enable -grpc-enable-tls option,  but the cert file is missing")
		}

		// 缺少server key
		if conf.GRpc.KeyFile == "" {
			return errors.New("enable -grpc-enable-tls option, but the key file is missing")
		}
	}
	return nil
}

// 初始化etcd服务配置
func (conf *Config) InitEtcdServiceConfig() error {
	// etcd集群地址
	if conf.Etcd.Cluster == "" {
		if clusterEnv := os.Getenv(ETCD_CLUSTER_ENV_KEY); clusterEnv != "" {
			conf.Etcd.Cluster = clusterEnv
		} else {
			return errors.New("initEtcdClient error: missing etcd cluster address.")
		}
	}

	// 启用了tls认证
	if conf.Etcd.EnableTLS && !conf.Etcd.Insecure {
		// 缺少ca证书
		if err := conf.checkEtcdServiceCaFileConfig(); err != nil {
			return err
		}
	}

	// 启用了客户端认证
	if conf.Etcd.ClientAuth {
		// 检查ca证书
		if err := conf.checkEtcdServiceCaFileConfig(); err != nil {
			return err
		}

		// 检查client auth证书配置
		if err := conf.checkEtcdServiceClientAuthConfig(); err != nil {
			return err
		}
	}

	return nil
}

// 检验etcd服务ca文件配置
func (conf *Config) checkEtcdServiceCaFileConfig() error {
	if conf.Etcd.CaFile == "" {
		if caFileEnv := os.Getenv(CA_FILE_ENV_KEY); caFileEnv != "" {
			conf.Etcd.CaFile = caFileEnv
		} else {
			return errors.New("you enabled enable-tls, but the cert file path is missing")
		}
	}

	return nil
}

// 检查etcd服务开启了客户端认证时证书的配置
// 主要检查cert和key
func (conf *Config) checkEtcdServiceClientAuthConfig() error {
	if conf.Etcd.ClientCertFile == "" {
		if clientCertEnv := os.Getenv(CLIENT_CERT_FILE_ENV_KEY); clientCertEnv == "" {
			conf.Etcd.ClientCertFile = clientCertEnv
		} else {
			return errors.New("you enabled client-cert-auth,  but the client cert file path is missing")
		}
	}

	if conf.Etcd.ClientKeyFile == "" {
		if clientKeyEnv := os.Getenv(CLIENT_KEY_FILE_ENV_KEY); clientKeyEnv == "" {
			conf.Etcd.ClientKeyFile = clientKeyEnv
		} else {
			return errors.New("you enabled client-cert-auth,  but the key file path is missing")
		}
	}

	return nil
}
