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
)

const maxEndureMs = 5

const (
	WORKER_RPC_HOST_DEFAULT  = "localhost"
	WORKER_RPC_PORT_DEFAULT  = "3212"
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
	fcfg := new(FileConfig)
	if err = json.Unmarshal(b, fcfg); err != nil {
		return err
	}
	cfg.workerId = fcfg.WorkerId
	cfg.enableTLS = fcfg.EnableTLS
	cfg.insecure = fcfg.Insecure
	cfg.ca = fcfg.CACert
	cfg.clientCertAuth = fcfg.ClientCertAuth
	cfg.clientCert = fcfg.ClientCert
	cfg.clientKey = fcfg.ClientKey
	cfg.cluster = fcfg.Cluster
	cfg.rpcHost = fcfg.Rpc.Host
	cfg.rpcPort = fcfg.Rpc.Port
	cfg.httpHost = fcfg.Http.Host
	cfg.httpPort = fcfg.Http.Port
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

	w, err := ysf.getWorkerValue()

	// worker不存在
	if w == nil && err == nil {
		//ysf.putWorkerValue()
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
			err = ysf.checkSysTimestamp()
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	if err = ysf.putWorkerServerValue(); err != nil {
		log.Fatal(err)
	}

	rpcServe := newCheckSysTimestampServer(ysf.Config.rpcHost, ysf.Config.rpcPort)
	rpcServe.start()

	//// 新的机器节点
	//if resp, ok := ysf.workerNotFoundInEtcd(); ok {
	//	// 创建节点错误
	//	if resp, err := ysf.createWorkerInEtcd(); err != nil {
	//		log.Fatal(err)
	//	} else {
	//		fmt.Printf("create WorkerValue in etcd: %v", *resp)
	//	}
	//
	//	// 做集群机器的时间校验
	//	fmt.Println("校验时间")
	//} else {
	//	// 获取当前机器节点的值
	//	// TODO 错误处理
	//	data, err := ysf.unMarshalWorkerValue(resp.Node.Value)
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//
	//	lastTimestamp, err := strconv.ParseInt(data["last_timestamp"], 10, 64)
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//	// 发生了回拨，此刻时间小于上次发号时间
	//	ts := timestamp()
	//	if ts < lastTimestamp {
	//		//时间偏差大小小于5ms，则等待两倍时间
	//		if offset := lastTimestamp - ts; offset <= maxEndureMs {
	//			time.Sleep(time.Millisecond * time.Duration(offset<<1))
	//		} else {
	//			log.Fatalf("时间不正确!")
	//		}
	//	} else {
	//		// 做集群机器的时间校验
	//		fmt.Println("校验时间")
	//	}
	//}
	//
	//// 启动定时上报进程
	//go ysf.reportTimestamp()
	//
	//// 将rpc信息写入到temporary中
	//
	//// 启动rpc server
	//
	//// 启动http server
	//time.Sleep(time.Hour)
}

func (ysf *YCSnowflake) createClient() (*clientv3.Client, error) {
	var err error
	if ysf.Config.configFile != "" {
		if err = ysf.Config.initConfigFromFile(); err != nil {
			return nil, err
		}
	}

	if err = ysf.validatorWorkerId(); err != nil {
		return nil, err
	}

	if ysf.Config.cluster == "" {
		if clusterEnv := os.Getenv(CLUSTER_ENV_KEY); clusterEnv != "" {
			ysf.Config.cluster = clusterEnv
		} else {
			return nil, errors.New("missing etcd cluster address")
		}
	}

	if ysf.Config.rpcHost == "" {
		if rpcHostEnv := os.Getenv(WORKER_RPC_HOST_ENV_KEY); rpcHostEnv != "" {
			ysf.Config.rpcHost = rpcHostEnv
		} else {
			ysf.Config.rpcHost = WORKER_RPC_HOST_DEFAULT
		}
	}

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
		return errors.New("WorkerValue id does not exist or is invalid")
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

	if ysf.Config.enableTLS {
		if err = ysf.validatorCa(); err != nil {
			return nil, err

		}

		if certPool, err = ysf.wrapNewCertPool(); err != nil {
			return nil, err
		}

		return &tls.Config{RootCAs: certPool}, nil
	}

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

	for _, item := range res.Kvs {
		if string(item.Key) == key {
			return ysf.unMarshalWorkerValue(item.Value)
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
		successWorkerServer []*WorkerServerValue
		errorNodeCount      int
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
		} else {
			successWorkerServer = append(successWorkerServer, w)
		}
	}

	if res.Count <= 0 {
		err = fmt.Errorf("%s\nsystem time info\n\tcurrent time:\t\t%s\n\tcurrent timestamp:\t%d",
			"no worker server provider system time verify, you can enable -no-worker-skip-check.",
			time.Now().Format("2006-01-02 15:04:05"),
			timestamp())
		return err
	}
	// 如果失败的大于半数, 则取消校验
	if errorNodeCount > int((res.Count-1)/2+1) {
		return errors.New("more than half failed to parse multiple worker server data")
	}
	fmt.Println(res.Kvs)
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

func keyNotFound(err error) bool {
	if err != nil {
		if etcdError, ok := err.(client.Error); ok {
			if etcdError.Code == client.ErrorCodeKeyNotFound ||
				etcdError.Code == client.ErrorCodeNotFile ||
				etcdError.Code == client.ErrorCodeNotDir {
				return true
			}
		}
	}
	return false
}

func (ysf *YCSnowflake) reportTimestamp() {
	//t := time.Tick(time.Second * 3)
	//for {
	//	<-t
	//	resp, err := ysf.createWorkerInEtcd()
	//	if err != nil {
	//		fmt.Println(err)
	//	}
	//	fmt.Println(resp.Node.Value)
	//}
}

func timestamp() uint64 {
	return uint64(time.Now().UnixNano() / int64(time.Millisecond))
}

//func (ysf *YCSnowflake) temporaryKeyNotFound() bool {
//	api := client.NewKeysAPI(ysf.etcdCli)
//	_, err := api.Get(ysf.ctx, ysf.temporaryKey, &client.GetOptions{
//		Recursive: false,
//		Sort:      false,
//		Quorum:    true,
//	})
//
//	if keyNotFound(err) {
//		return true
//	}
//	return false
//}

//func (ysf *YCSnowflake) createNodeInfoInEtcd() error {
//	api := client.NewKeysAPI(ysf.etcdCli)
//	if ysf.temporaryKeyNotFound() {
//		_, err := api.Set(ysf.ctx, ysf.temporaryKey, "", &client.SetOptions{
//			Dir: true,
//		})
//		if err != nil {
//			return err
//		}
//	}
//	key := ysf.temporaryKey + "/" + strconv.Itoa(ysf.Config.workerId)
//	b, err := json.Marshal(map[string]string{
//		"host": *ysf.Config.rpcHost,
//		"port": *ysf.Config.rpcPort,
//	})
//	if err != nil {
//		return err
//	}
//	_, err = api.Set(ysf.ctx, key, string(b), &client.SetOptions{
//		Dir: false,
//	})
//
//	return err
//}

//func (ysf *YCSnowflake) checkSystemTimestamp() {
//	api := client.NewKeysAPI(ysf.etcdCli)
//	api.Get(ysf.ctx, ysf.temporaryKey, nil)
//}
