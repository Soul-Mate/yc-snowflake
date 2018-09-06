package main

import (
	"github.com/Soul-Mate/yc-snowflake/snowflake"
	"os"
	"flag"
	"fmt"
	"io/ioutil"
	"github.com/coreos/etcd/client"
	"crypto/x509"
	"crypto/tls"
	"context"
	"strconv"
	"encoding/json"
	"time"
	"log"
	"strings"
	"errors"
	"github.com/etcd-io/etcd/clientv3"
	"unsafe"
	"math"
)

const maxEndureMs = 5

const (
	WORKER_RPC_HOST_DEFAULT  = "127.0.0.1"
	WORKER_RPC_PORT_DEFAULT  = "3212"
	CONF_FILE_ENV_KEY        = "YC_SNOWFLAKE_CONF"
	WORKER_ID_ENV_KEY        = "YC_SNOWFLAKE_WORKER_ID"
	WORKER_RPC_HOST_ENV_KEY  = "YC_SNOWFLAKE_WORKER_RPC_HOST"
	WORKER_RPC_PORT_ENV_KEY  = "YC_SNOWFLAKE_WORKER_RPC_PORT"
	CLUSTER_ENV_KEY          = "YC_SNOWFLAKE_CLUSTER"
	CA_FILE_ENV_KEY          = "ETCD_TRUSTED_CA_FILE"
	CLIENT_CERT_FILE_ENV_KEY = "ETCD_CERT_FILE"
	CLIENT_KEY_FILE_ENV_KEY  = "ETCD_KEY_FILE"
)

type FileConfig struct {
	WorkerId       int    `json:"worker_id"`
	SkipCheck      bool   `json:"no_worker_skip_check"`
	EnableTLS      bool   `json:"enable_tls"`
	Insecure       bool   `json:"insecure"`
	CACert         string `json:"ca_cert"`
	ClientCertAuth bool   `json:"client_cert_auth"`
	ClientCert     string `json:"client_cert"`
	ClientKey      string `json:"client_key"`
	Cluster        string `json:"cluster"`
	Rpc            struct {
		Host string `json:"host"`
		Port string `json:"port"`
	} `json:"rpc"`
	Http struct {
		Host string `json:"host"`
		Port string `json:"port"`
	} `json:"http"`
}

type YCSnowflakeConfig struct {
	configFile     string
	workerId       int
	skipCheck      bool
	enableTLS      bool
	insecure       bool
	ca             string
	clientCertAuth bool
	clientCert     string
	clientKey      string
	cluster        string
	rpcHost        string
	rpcPort        string
	httpHost       string
	httpPort       string
}

func NewYCSnowflakeConfig() (*YCSnowflakeConfig) {
	cfg := &YCSnowflakeConfig{}
	return cfg
}

func (cfg *YCSnowflakeConfig) parse() {
	flag.StringVar(&cfg.configFile, "config-file", "", "")
	flag.IntVar(&cfg.workerId, "WorkerValue-id", 0, "")
	flag.BoolVar(&cfg.skipCheck, "no-worker-skip-check", false, "")
	flag.BoolVar(&cfg.enableTLS, "enable-tls", false, "")
	flag.BoolVar(&cfg.insecure, "insecure", false, "")
	flag.StringVar(&cfg.ca, "trusted-ca-file", "", "")
	flag.BoolVar(&cfg.clientCertAuth, "client-cert-auth", false, "")
	flag.StringVar(&cfg.clientCert, "cert-file", "", "")
	flag.StringVar(&cfg.clientKey, "key-file", "", "")
	flag.StringVar(&cfg.cluster, "initial-cluster", "", "")
	flag.StringVar(&cfg.rpcHost, "WorkerValue-rpc-host", "", "")
	flag.StringVar(&cfg.rpcPort, "WorkerValue-rpc-port", "", "")
	flag.StringVar(&cfg.httpHost, "WorkerValue-http-host", "", "")
	flag.StringVar(&cfg.httpPort, "WorkerValue-http-port", "", "")
	flag.Parse()
}

func (cfg *YCSnowflakeConfig) initConfigFromFile() error {
	b, err := ioutil.ReadFile(cfg.configFile)
	if err != nil {
		return err
	}
	fc := new(FileConfig)
	if err = json.Unmarshal(b, fc); err != nil {
		return err
	}
	cfg.workerId = fc.WorkerId
	cfg.skipCheck = fc.SkipCheck
	cfg.enableTLS = fc.EnableTLS
	cfg.insecure = fc.Insecure
	cfg.ca = fc.CACert
	cfg.clientCertAuth = fc.ClientCertAuth
	cfg.clientCert = fc.ClientCert
	cfg.clientKey = fc.ClientKey
	cfg.cluster = fc.Cluster
	cfg.rpcHost = fc.Rpc.Host
	cfg.rpcPort = fc.Rpc.Port
	cfg.httpHost = fc.Http.Host
	cfg.httpPort = fc.Http.Port
	return nil
}

func (cfg *YCSnowflakeConfig) workerIdInvalid() bool {
	// max WorkerValue id is 1024
	return cfg.workerId <= 0 || cfg.workerId > snowflake.MaxWorkerId+1
}

type YCSnowflake struct {
	etcdKey      string
	temporaryKey string
	ctx          context.Context
	Config       *YCSnowflakeConfig
	client       *clientv3.Client
	etcdCli      client.Client
}

type WorkerValue struct {
	Id            int    `json:"id"`
	LastTimestamp uint64 `json:"last_timestamp"`
}

type WorkerServerValue struct {
	RPCHost string `json:"rpc_host"`
	RPCPort string `json:"rpc_port"`
}

func NewYCSnowflake(cfg *YCSnowflakeConfig) *YCSnowflake {
	return &YCSnowflake{
		etcdKey:      "/yc_snowflake_forever",
		temporaryKey: "/yc_snowflake_temporary",
		ctx:          context.Background(),
		Config:       cfg,
	}
}

func (ysf *YCSnowflake) Start() {
	ysf.Config.parse()

	if cli, err := ysf.createClient(); err != nil {
		log.Fatal(err)
	} else {
		ysf.client = cli
	}

	// 获取worker保存在etcd中的值
	w, err := ysf.getWorkerValue()
	if err != nil {
		log.Fatal(err)
	}
	// 新的节点
	if w == nil && err == nil {
		// 校验当前节点系统时间
		if err = ysf.checkSysTimestamp(); err != nil {
			log.Fatal(err)
		}
		// 设置节点值
		ysf.putWorkerValue()
	} else {
		ts := timestamp()
		// 发生了回拨，此刻时间小于上次发号时间
		if ts < w.LastTimestamp {
			//时间偏差大小小于5ms，则等待两倍时间
			if offset := w.LastTimestamp - ts; offset < maxEndureMs {
				time.Sleep(time.Millisecond * time.Duration(offset<<1))
			} else {
				log.Fatal("时间不正确")
			}
		} else {
			// 校验当前节点系统时间
			if err = ysf.checkSysTimestamp(); err != nil {
				log.Fatal(err)
			}
		}
	}

	// 定时上报系统时间戳
	go ysf.reportTimestamp(ysf.ctx)

	// 设置节点服务信息
	if err = ysf.putWorkerServerValue(); err != nil {
		log.Fatal(err)
	}

	rpcServe := newCheckSysTimestampServer(ysf.Config.rpcHost, ysf.Config.rpcPort)

	if err = rpcServe.start(); err != nil {
		log.Fatal(err)
	}
}

func (ysf *YCSnowflake) createClient() (*clientv3.Client, error) {
	var err error

	// 从文件中解析程序配置
	if ysf.Config.configFile == "" {
		// 从环境变量中获得程序配置文件路径
		if confEnv := os.Getenv(CONF_FILE_ENV_KEY); confEnv != "" {
			ysf.Config.configFile = confEnv

			if err = ysf.Config.initConfigFromFile(); err != nil {
				return nil, err
			}
		}
	} else {
		if err = ysf.Config.initConfigFromFile(); err != nil {
			return nil, err
		}
	}

	// 验证worker id的合法性
	if err = ysf.validatorWorkerId(); err != nil {
		return nil, err
	}

	// etcd 集群地址
	if ysf.Config.cluster == "" {
		if clusterEnv := os.Getenv(CLUSTER_ENV_KEY); clusterEnv != "" {
			ysf.Config.cluster = clusterEnv
		} else {
			return nil, errors.New("missing etcd cluster address")
		}
	}

	// rpc server host
	// 默认使用 127.0.0.1
	if ysf.Config.rpcHost == "" {
		if rpcHostEnv := os.Getenv(WORKER_RPC_HOST_ENV_KEY); rpcHostEnv != "" {
			ysf.Config.rpcHost = rpcHostEnv
		} else {
			ysf.Config.rpcHost = WORKER_RPC_HOST_DEFAULT
		}
	}

	// rpc server port
	// 默认使用端口 3212
	if ysf.Config.rpcPort == "" {
		if rpcPortEnv := os.Getenv(WORKER_RPC_PORT_ENV_KEY); rpcPortEnv != "" {
			ysf.Config.rpcPort = rpcPortEnv
		} else {
			ysf.Config.rpcPort = WORKER_RPC_PORT_DEFAULT
		}
	}

	cfg := clientv3.Config{}

	cfg.Endpoints = strings.Split(ysf.Config.cluster, ",")

	if tlsConfig, err := ysf.createTLSConfig(); tlsConfig != nil && err == nil {
		cfg.TLS = tlsConfig
	}

	return clientv3.New(cfg)
}

func (ysf *YCSnowflake) validatorWorkerId() error {
	if ysf.Config.workerId == 0 {
		// 从环境变量中加载worker id的值
		if workerIdStr := os.Getenv(WORKER_ID_ENV_KEY); workerIdStr != "" {
			if workerIdInt, err := strconv.Atoi(workerIdStr); err != nil {
				return fmt.Errorf("the value in the %s env var seems to be invalid", WORKER_ID_ENV_KEY)
			} else {
				ysf.Config.workerId = workerIdInt
			}
		}
	}

	if ysf.Config.workerIdInvalid() {
		return errors.New("worker id does not exist or is invalid")
	}

	return nil
}

func (ysf *YCSnowflake) createTLSConfig() (*tls.Config, error) {
	var (
		err            error
		certPool       *x509.CertPool
		cliCertificate tls.Certificate
	)

	if ysf.Config.enableTLS && ysf.Config.insecure {
		return &tls.Config{
			InsecureSkipVerify: true,
		}, nil
	}

	// tls证书配置
	if ysf.Config.enableTLS {
		if err = ysf.validatorCa(); err != nil {
			return nil, err

		}

		if certPool, err = ysf.wrapNewCertPool(); err != nil {
			return nil, err
		}

		return &tls.Config{RootCAs: certPool}, nil
	}

	// 开启了客户端认证
	// tls配置
	if ysf.Config.clientCertAuth {
		if err = ysf.validatorCa(); err != nil {
			return nil, err
		}

		if err = ysf.validatorClientTLSConfig(); err != nil {
			return nil, err
		}

		if certPool, err = ysf.wrapNewCertPool(); err != nil {
			return nil, err
		}

		if cliCertificate, err = tls.LoadX509KeyPair(ysf.Config.clientCert, ysf.Config.clientKey); err != nil {
			return nil, err
		}

		return &tls.Config{
			RootCAs:      certPool,
			Certificates: []tls.Certificate{cliCertificate},
		}, nil
	}

	return nil, nil
}

func (ysf *YCSnowflake) wrapNewCertPool() (*x509.CertPool, error) {
	var (
		err     error
		caBytes []byte
	)
	if caBytes, err = ioutil.ReadFile(ysf.Config.ca); err != nil {
		return nil, err
	}
	certPool := x509.NewCertPool()

	if ok := certPool.AppendCertsFromPEM(caBytes); ok {
		return nil, errors.New("add cert error")
	}
	return certPool, nil
}

func (ysf *YCSnowflake) validatorCa() error {
	if ysf.Config.ca == "" {
		if caFileEnv := os.Getenv(CA_FILE_ENV_KEY); caFileEnv != "" {
			ysf.Config.ca = caFileEnv
		} else {
			return errors.New("you enabled enable-tls, but the ca path is empty")
		}
	}
	return nil
}

func (ysf *YCSnowflake) validatorClientTLSConfig() error {
	if ysf.Config.clientCert == "" {
		if clientCertEnv := os.Getenv(CLIENT_CERT_FILE_ENV_KEY); clientCertEnv == "" {
			ysf.Config.clientCert = clientCertEnv
		} else {
			return errors.New("you enabled client-cert-auth, but the client cert path is empty")
		}
	}

	if ysf.Config.clientKey == "" {
		if clientKeyEnv := os.Getenv(CLIENT_KEY_FILE_ENV_KEY); clientKeyEnv == "" {
			ysf.Config.clientKey = clientKeyEnv
		} else {
			return errors.New("you enabled client-cert-auth, but the client key path is empty")
		}
	}
	return nil
}

func (ysf *YCSnowflake) getWorkerValue() (*WorkerValue, error) {
	kv := clientv3.NewKV(ysf.client)
	key := ysf.workerKey()
	res, err := kv.Get(ysf.ctx, key)
	if err != nil {
		return nil, err
	}
	fmt.Println(res.Kvs)
	for _, item := range res.Kvs {
		if string(item.Key) == key {
			// 如果解析失败, put新的值 防止kv被污染,然后进行重试
			if _, err = ysf.unMarshalWorkerValue(item.Value); err != nil {
				if err = ysf.putWorkerValue(); err != nil {
					return nil, err
				}

				if res, err = kv.Get(ysf.ctx, key); err != nil {
					return nil, err
				}

				for _, item := range res.Kvs {
					if string(item.Key) == key {
						return ysf.unMarshalWorkerValue(item.Value)
					}
				}
			}
		}
	}

	return nil, nil
}

func (ysf *YCSnowflake) putWorkerValue() error {
	value, err := ysf.marshalWorkerValue()
	if err != nil {
		return err
	}
	key := ysf.workerKey()
	kv := clientv3.NewKV(ysf.client)
	_, err = kv.Put(ysf.ctx, key, value)

	if err != nil {
		return err
	}

	return nil
}

func (ysf *YCSnowflake) workerKey() string {
	return ysf.etcdKey + "/" + strconv.Itoa(ysf.Config.workerId)
}

func (ysf *YCSnowflake) workerTemporaryKey() string {
	return ysf.temporaryKey + "/" + strconv.Itoa(ysf.Config.workerId)
}

func (ysf *YCSnowflake) marshalWorkerValue() (string, error) {
	worker := &WorkerValue{
		Id:            ysf.Config.workerId,
		LastTimestamp: timestamp(),
	}

	b, err := json.Marshal(worker)

	if err != nil {
		return "", err
	}

	return *(*string)(unsafe.Pointer(&b)), nil
}

func (ysf *YCSnowflake) unMarshalWorkerValue(value []byte) (*WorkerValue, error) {
	w := new(WorkerValue)
	if err := json.Unmarshal(value, w); err != nil {
		return nil, err
	}
	return w, nil
}

func (ysf *YCSnowflake) checkSysTimestamp() error {
	var (
		successWorkerServer       []*WorkerServerValue
		successWorkerSysTimestamp []*SysTimestamp
		errorNodeCount            int
	)
	kv := clientv3.NewKV(ysf.client)
	res, err := kv.Get(ysf.ctx, ysf.temporaryKey, clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))

	if err != nil {
		return err
	}

	for _, item := range res.Kvs {
		w, err := ysf.unMarshalWorkerServerValue(item.Value)
		if err != nil {
			// TODO 记录日志
			errorNodeCount++
		} else if string(item.Key) != ysf.workerTemporaryKey() {
			successWorkerServer = append(successWorkerServer, w)
		}
	}
	fmt.Println(res.Kvs)
	if res.Count <= 0 || len(successWorkerServer) <= 0 {
		if ysf.Config.skipCheck {
			return nil
		}

		err = fmt.Errorf("%s\nsystem time info\n\tcurrent time:\t\t%s\n\tcurrent timestamp:\t%d",
			"no worker server provider system time verify, you can enable -no-worker-skip-check.",
			time.Now().Format("2006-01-02 15:04:05"),
			timestamp())
		return err
	}

	if errorNodeCount > int(res.Count/2+1) {
		return errors.New("more than half failed to parse multiple worker server data")
	}

	for _, node := range successWorkerServer {
		sysTimestamp, err := rpcGetSysTimestamp(node.RPCHost, node.RPCPort)
		if err == nil {
			fmt.Printf("get sys timestamp: %v\n", sysTimestamp)
			successWorkerSysTimestamp = append(successWorkerSysTimestamp, sysTimestamp)
		} else {
			log.Println(err)
		}
	}

	if len(successWorkerSysTimestamp) < int(res.Count/2+1) {
		return errors.New("more than half of the node calls failed")
	}

	var (
		avg   uint64
		total uint64
	)

	for _, result := range successWorkerSysTimestamp {
		total += result.Timestamp
	}

	avg = total / uint64(len(successWorkerSysTimestamp))

	// 误差在100毫秒以内
	if math.Abs(float64(timestamp()-avg)) > 100 {
		return errors.New("the node system timestamp error exceeds the threshold")
	}

	return nil
}

func (ysf *YCSnowflake) putWorkerServerValue() error {
	kv := clientv3.NewKV(ysf.client)
	value, err := ysf.marshalWorkerServerValue()
	if err != nil {
		return err
	}

	key := ysf.temporaryKey + "/" + strconv.Itoa(ysf.Config.workerId)
	_, err = kv.Put(ysf.ctx, key, value)
	if err != nil {
		return err
	}

	return nil
}

func (ysf *YCSnowflake) marshalWorkerServerValue() (string, error) {
	b, err := json.Marshal(&WorkerServerValue{
		RPCHost: ysf.Config.rpcHost,
		RPCPort: ysf.Config.rpcPort,
	})

	if err != nil {
		return "", err
	}

	return *(*string)(unsafe.Pointer(&b)), nil
}

func (ysf *YCSnowflake) unMarshalWorkerServerValue(value []byte) (*WorkerServerValue, error) {
	workerServerValue := new(WorkerServerValue)
	if err := json.Unmarshal(value, workerServerValue); err != nil {
		return nil, err
	}

	return workerServerValue, nil
}

func (ysf *YCSnowflake) reportTimestamp(ctx context.Context) {
	t := time.Tick(time.Second * 3)
	for {
		select {
		case <-t:
			if err := ysf.putWorkerValue(); err != nil {
				log.Printf("report timestamp error: %v", err)
			} else {
				log.Printf("report timestamp success.")
			}
		case <-ctx.Done():
			log.Printf("report done.")
			break
		default:
		}
	}
}

func timestamp() uint64 {
	return uint64(time.Now().UnixNano() / int64(time.Millisecond))
}
