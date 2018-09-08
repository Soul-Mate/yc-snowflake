package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/Soul-Mate/yc-snowflake/http"
	"github.com/Soul-Mate/yc-snowflake/pb"
	"github.com/Soul-Mate/yc-snowflake/grpc/client"
	"github.com/Soul-Mate/yc-snowflake/grpc/server"
	"github.com/Soul-Mate/yc-snowflake/snowflake"
	"github.com/etcd-io/etcd/clientv3"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

const (
	// 最大忍受时间发生回拨时常 单位(毫秒)
	maxEndureMs = 5
	// 最大系统时间误差 单位(毫秒)
	maxSysTimeErrorMs        = 100
	WORKER_RPC_HOST_DEFAULT  = "127.0.0.1"
	WORKER_RPC_PORT_DEFAULT  = "3212"
	WORKER_HTTP_PORT_DEFAULT = "3213"
	CONF_FILE_ENV_KEY        = "YC_SNOWFLAKE_CONF"
	WORKER_ID_ENV_KEY        = "YC_SNOWFLAKE_WORKER_ID"
	WORKER_RPC_HOST_ENV_KEY  = "YC_SNOWFLAKE_WORKER_RPC_HOST"
	WORKER_RPC_PORT_ENV_KEY  = "YC_SNOWFLAKE_WORKER_RPC_PORT"
	WORKER_HTTP_PORT_ENV_KEY = "YC_SNOWFLAKE_WORKER_HTTP_PORT"
	CLUSTER_ENV_KEY          = "YC_SNOWFLAKE_CLUSTER"
	CA_FILE_ENV_KEY          = "ETCD_TRUSTED_CA_FILE"
	CLIENT_CERT_FILE_ENV_KEY = "ETCD_CERT_FILE"
	CLIENT_KEY_FILE_ENV_KEY  = "ETCD_KEY_FILE"
)

type FileConfig struct {
	LogFile        string `json:"log_file"`
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
	} `json:"grpc"`
	Http struct {
		Host     string `json:"host"`
		Port     string `json:"port"`
		CertAuth bool   `json:"cert_auth"`
		CertFile string `json:"cert_file"`
		KeyFile  string `json:"key_file"`
	} `json:"http"`
}

type YCSnowflakeConfig struct {
	configFile      string
	logFile         string
	workerId        int
	skipCheck       bool
	enableTLS       bool
	insecure        bool
	ca              string
	clientCertAuth  bool
	clientCert      string
	clientKey       string
	cluster         string
	rpcHost         string
	rpcPort         string
	httpHost        string
	httpPort        string
	httpCertAuth    bool
	httpTLSCertFile string
	httpTLSKeyFile  string
}

func (cfg *YCSnowflakeConfig) parse() {
	flag.StringVar(&cfg.configFile, "config-file", "", "")
	flag.StringVar(&cfg.logFile, "log-file", "", "")
	flag.IntVar(&cfg.workerId, "WorkerValue-id", 0, "")
	flag.BoolVar(&cfg.skipCheck, "no-worker-skip-check", false, "")
	flag.BoolVar(&cfg.enableTLS, "enable-tls", false, "")
	flag.BoolVar(&cfg.insecure, "insecure", false, "")
	flag.StringVar(&cfg.ca, "trusted-ca-file", "", "")
	flag.BoolVar(&cfg.clientCertAuth, "client-cert-auth", false, "")
	flag.StringVar(&cfg.clientCert, "cert-file", "", "")
	flag.StringVar(&cfg.clientKey, "key-file", "", "")
	flag.StringVar(&cfg.cluster, "initial-cluster", "", "")
	flag.StringVar(&cfg.rpcHost, "worker-grpc-host", "", "")
	flag.StringVar(&cfg.rpcPort, "worker-grpc-port", "", "")
	flag.StringVar(&cfg.httpHost, "http-host", "", "")
	flag.StringVar(&cfg.httpPort, "http-port", "", "")
	flag.BoolVar(&cfg.httpCertAuth, "http-cert-auth", false, "")
	flag.StringVar(&cfg.httpTLSCertFile, "http-cert-file", "", "")
	flag.StringVar(&cfg.httpTLSKeyFile, "http-key-file", "", "")
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
	cfg.logFile = fc.LogFile
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
	cfg.httpCertAuth = fc.Http.CertAuth
	cfg.httpTLSCertFile = fc.Http.CertFile
	cfg.httpTLSKeyFile = fc.Http.KeyFile
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
	cancel       context.CancelFunc
	Config       *YCSnowflakeConfig
	client       *clientv3.Client
	logger       *WrapLog
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

	ysf := new(YCSnowflake)
	ysf.Config = cfg
	ysf.etcdKey = "/yc_snowflake_forever"
	ysf.temporaryKey = "/yc_snowflake_temporary"
	ysf.ctx, ysf.cancel = context.WithCancel(context.Background())
	return ysf
}

func (ysf *YCSnowflake) Start() {
	var (
		err error
	)

	ysf.Config.parse()

	// 从文件中解析程序配置
	if ysf.Config.configFile == "" {
		// 从环境变量中获得程序配置文件路径
		if confEnv := os.Getenv(CONF_FILE_ENV_KEY); confEnv != "" {
			ysf.Config.configFile = confEnv
			if err = ysf.Config.initConfigFromFile(); err != nil {
				log.Fatalf("init config file error: %v", err)
			}
		}
	} else {
		if err = ysf.Config.initConfigFromFile(); err != nil {
			log.Fatal("failed to initialize configuration")
		}
	}

	// 验证worker id的合法性
	if err = ysf.validatorWorkerId(); err != nil {
		log.Fatal(err)
	}

	// grpc server host
	// 默认使用 127.0.0.1
	if ysf.Config.rpcHost == "" {
		if rpcHostEnv := os.Getenv(WORKER_RPC_HOST_ENV_KEY); rpcHostEnv != "" {
			ysf.Config.rpcHost = rpcHostEnv
		} else {
			ysf.Config.rpcHost = WORKER_RPC_HOST_DEFAULT
		}
	}

	// grpc server port
	// 默认使用端口 3212
	if ysf.Config.rpcPort == "" {
		if rpcPortEnv := os.Getenv(WORKER_RPC_PORT_ENV_KEY); rpcPortEnv != "" {
			ysf.Config.rpcPort = rpcPortEnv
		} else {
			ysf.Config.rpcPort = WORKER_RPC_PORT_DEFAULT
		}
	}

	// http server port
	if ysf.Config.httpPort == "" {
		if rpcPortEnv := os.Getenv(WORKER_HTTP_PORT_ENV_KEY); rpcPortEnv != "" {
			ysf.Config.httpPort = rpcPortEnv
		} else {
			ysf.Config.httpPort = WORKER_HTTP_PORT_DEFAULT
		}
	}

	// 创建etcd客户端
	if ysf.client, err = ysf.createClient(); err != nil {
		log.Fatal(err)

	}

	// 获取worker保存在etcd中的值
	w, err := ysf.getWorkerValue()
	if err != nil {
		log.Fatal(err)
	}

	// 初始化wrap log
	if ysf.logger, err = newWrapLog(ysf.Config.logFile); err != nil {
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

	// 启动http server
	go ysf.startHttpServer()

	// 设置节点服务信息
	if err = ysf.putWorkerServerValue(); err != nil {
		log.Fatal(err)
	}

	if err = server.Server(ysf.Config.rpcHost, ysf.Config.rpcPort); err != nil {
		log.Fatal(err)
	}
}

func (ysf *YCSnowflake) createClient() (*clientv3.Client, error) {
	// etcd 集群地址
	if ysf.Config.cluster == "" {
		if clusterEnv := os.Getenv(CLUSTER_ENV_KEY); clusterEnv != "" {
			ysf.Config.cluster = clusterEnv
		} else {
			return nil, errors.New("missing etcd cluster address")
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
	res, err := kv.Get(context.Background(), key)
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

				if res, err = kv.Get(context.Background(), key); err != nil {
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
	_, err = kv.Put(context.Background(), key, value)

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
		successWorkerSysTimestamp []*pb.SysTimestamp
	)
	kv := clientv3.NewKV(ysf.client)
	res, err := kv.Get(context.Background(), ysf.temporaryKey, clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))

	if err != nil {
		return err
	}

	for _, item := range res.Kvs {
		w, err := ysf.unMarshalWorkerServerValue(item.Value)
		if string(item.Key) != ysf.workerTemporaryKey() && err == nil {
			successWorkerServer = append(successWorkerServer, w)
		}
	}

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

	for _, node := range successWorkerServer {
		rpcClient, err := client.NewClient(node.RPCHost, node.RPCPort, client.WithTimeout(time.Second * 3))
		if err != nil {
			ysf.logger.Printf(LErr, "grpc client(%s:%s) init error: %v",
				node.RPCHost, node.RPCPort, err)
		} else {
			sysTimestamp, err := rpcClient.GetSysTimestamp()
			if err != nil {
				ysf.logger.Printf(LErr, "grpc client(%s:%s) call GetSysTimestamp error: %v",
					node.RPCHost, node.RPCPort, err)
			} else {
				successWorkerSysTimestamp = append(successWorkerSysTimestamp, sysTimestamp)
			}
			if err = rpcClient.Close(); err != nil {
				ysf.logger.Printf(LErr, "grpc client(%s:%s) close error: %v",
					node.RPCHost, node.RPCPort, err)
			}
		}
	}

	if len(successWorkerSysTimestamp) <= 0 {
		return nil
	}

	var (
		avg   uint64
		total uint64
	)

	for _, result := range successWorkerSysTimestamp {
		total += result.Timestamp
	}

	avg = total / uint64(len(successWorkerSysTimestamp))

	ysf.logger.Printf(LDebug, "求得平均值误差: %d", avg)

	// 误差在100毫秒以内
	if math.Abs(float64(timestamp()-avg)) > maxSysTimeErrorMs {
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
	_, err = kv.Put(context.Background(), key, value)
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
	done := false
	t := time.Tick(time.Second * 3)
	for {
		select {
		case <-t:
			if err := ysf.putWorkerValue(); err != nil {
				ysf.logger.Printf(LDebug, "[worker-%d]: report timestamp error: %v", err, ysf.Config.workerId)
			} else {
				ysf.logger.Printf(LDebug, "[worker-%d]: report timestamp success.", ysf.Config.workerId)
			}
		case <-ctx.Done():
			ysf.logger.Printf(LDebug, "report done.")
			done = true
		default:
		}
		if done {
			break
		}
	}
}

func timestamp() uint64 {
	return uint64(time.Now().UnixNano() / int64(time.Millisecond))
}

func (ysf *YCSnowflake) startHttpServer() {
	if ysf.Config.httpCertAuth {
		err := http.ServerTLS(ysf.Config.httpHost, ysf.Config.httpPort, ysf.Config.httpTLSCertFile, ysf.Config.httpTLSKeyFile, ysf.Config.workerId)
		if err != nil {
			log.Fatalf("start https server error: %v", err)
		}
	}
	if err := http.Server(ysf.Config.httpHost, ysf.Config.httpPort, ysf.Config.workerId); err != nil {
		ysf.cancel()
		log.Fatalf("start https server error: %v", err)
	}
}
