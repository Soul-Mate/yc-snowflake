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
	"net/http"
	"context"
	"strconv"
	"encoding/json"
	"time"
	"log"
	"strings"
)

const maxEndureMs = 5

type YCSnowflakeConfig struct {
	enableTLS      *bool
	caCertFile     *string
	clientCert     *string
	clientKey      *string
	clientCertAuth *bool
	workerID       *int
	clusterHosts   []string
}

func NewYCSnowflakeConfig() (*YCSnowflakeConfig) {
	cfg := &YCSnowflakeConfig{}
	return cfg
}

var clusterHosts *string

func (cfg *YCSnowflakeConfig) parse() {
	cfg.workerID = flag.Int("worker-id", 0, "")
	cfg.enableTLS = flag.Bool("enable-tls", false, "")
	cfg.clientCertAuth = flag.Bool("client-cert-auth", false, "")
	cfg.caCertFile = flag.String("cacert", "", "")
	cfg.clientCert = flag.String("client-cert", "", "")
	cfg.clientKey = flag.String("client-key", "", "")
	clusterHosts = flag.String("cluster-host", "", "")
	flag.Parse()
}

func (cfg *YCSnowflakeConfig) clientTLSCfgEmpty() bool {
	return *cfg.caCertFile == "" || *cfg.clientCert == "" || *cfg.clientKey == ""
}

func (cfg *YCSnowflakeConfig) clusterHostsEmpty() bool {
	return len(cfg.clusterHosts) <= 0 || cfg.clusterHosts[0] == ""
}

func (cfg *YCSnowflakeConfig) fromEnvTLSCfg() {
	// 从环境变量中获取
	*cfg.caCertFile = os.Getenv("ETCDCTL_CA_FILE")
	*cfg.clientCert = os.Getenv("ETCDCTL_CERT_FILE")
	*cfg.clientKey = os.Getenv("ETCDCTL_KEY_FILE")
}

func (cfg *YCSnowflakeConfig) workerInValid() bool {
	// max worker id is 1024
	return *cfg.workerID <= 0 || *cfg.workerID > snowflake.MaxWorkerId+1
}

type YCSnowflake struct {
	etcdKey      string
	temporaryKey string
	ctx          context.Context
	Config       *YCSnowflakeConfig
	etcdCli      client.Client
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

	ysf.Config.clusterHosts = strings.Split(*clusterHosts, ",")

	if ysf.Config.workerInValid() {
		fmt.Fprintf(os.Stderr, "please provide the worker id\n")
		os.Exit(1)
	}

	if ysf.Config.clusterHostsEmpty() {
		fmt.Fprintf(os.Stderr, "please provide connect hosts\n")
		os.Exit(1)
	}

	// 启用HTTPS的客户端到服务器端传输安全
	if *ysf.Config.enableTLS {
		if err := ysf.initTLSClient(); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
		// 启用HTTPS客户端证书的客户端到服务器端认证
	} else if *ysf.Config.clientCertAuth {
		// 客户端证书为空
		// 从环境变量中获取
		if ysf.Config.clientTLSCfgEmpty() {
			ysf.Config.fromEnvTLSCfg()
		}

		if ysf.Config.clientTLSCfgEmpty() {
			fmt.Fprintf(os.Stderr, "client tls config empty!\n")
			os.Exit(1)
		}

		if err := ysf.initTLSCertClient(); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
		// 普通的http客户端
	} else {
		if err := ysf.initClient(); err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	}

	// 新的机器节点
	if resp, ok := ysf.workerNotFoundInEtcd(); ok {
		// 创建节点错误
		if resp, err := ysf.createWorkerInEtcd(); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("create worker in etcd: %v", *resp)
		}
	} else {
		// 获取当前机器节点的值
		// TODO 错误处理
		data, err := ysf.unMarshalWorkerValue(resp.Node.Value)
		if err != nil {
			log.Fatal(err)
		}

		lastTimestamp, err := strconv.ParseInt(data["last_timestamp"], 10, 64)
		if err != nil {
			log.Fatal(err)
		}
		// 发生了回拨，此刻时间小于上次发号时间
		ts := timestamp()
		if ts < lastTimestamp {
			//时间偏差大小小于5ms，则等待两倍时间
			if offset := lastTimestamp - ts; offset <= maxEndureMs {
				time.Sleep(time.Millisecond * time.Duration(offset<<1))
			} else {
				log.Fatalf("时间不正确!")
			}
		} else {
			// 确认当前时间是否是正确的
			fmt.Println("校验时间")
		}

		// 启动定时上报进程
		go ysf.reportTimestamp()

		time.Sleep(time.Hour)
	}
}

func (ysf *YCSnowflake) initClient() error {
	cfg := client.Config{
		Endpoints: ysf.Config.clusterHosts,
	}

	cli, err := client.New(cfg)
	if err != nil {
		return fmt.Errorf("cannot create etcd client: %v\n", err)
	}

	ysf.etcdCli = cli
	return nil
}

func (ysf *YCSnowflake) initTLSClient() error {
	caBytes, err := ioutil.ReadFile(*ysf.Config.caCertFile)
	if err != nil {
		return err
	}

	certPool := x509.NewCertPool()
	if ok := certPool.AppendCertsFromPEM(caBytes); !ok {
		return fmt.Errorf("the %s file may be invalid!\n", *ysf.Config.caCertFile)
	}

	tr := &http.Transport{}
	if *ysf.Config.caCertFile == "" {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	} else {
		certPool := x509.NewCertPool()
		if ok := certPool.AppendCertsFromPEM(caBytes); !ok {
			return fmt.Errorf("the %s file may be invalid!\n", *ysf.Config.caCertFile)
		}
		tr.TLSClientConfig = &tls.Config{RootCAs: certPool}
	}

	cfg := client.Config{
		Endpoints: ysf.Config.clusterHosts,
		Transport: tr,
	}

	cli, err := client.New(cfg)
	if err != nil {
		return fmt.Errorf("cannot create etcd client: %v\n", err)
	}

	ysf.etcdCli = cli
	return nil
}

func (ysf *YCSnowflake) initTLSCertClient() error {
	caBytes, err := ioutil.ReadFile(*ysf.Config.caCertFile)
	if err != nil {
		return err
	}

	certPool := x509.NewCertPool()
	if ok := certPool.AppendCertsFromPEM(caBytes); !ok {
		return fmt.Errorf("the %s file may be invalid!\n", *ysf.Config.caCertFile)
	}

	cliCert, err := tls.LoadX509KeyPair(*ysf.Config.clientCert, *ysf.Config.clientKey)
	if err != nil {
		return fmt.Errorf("load x509 key pair error: %v\n", err)
	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:      certPool,
			Certificates: []tls.Certificate{cliCert},
		},
	}

	cfg := client.Config{
		Endpoints: ysf.Config.clusterHosts,
		Transport: tr,
	}

	cli, err := client.New(cfg)
	if err != nil {
		return fmt.Errorf("cannot create etcd client: %v\n", err)
	}

	ysf.etcdCli = cli
	return nil
}

func (ysf *YCSnowflake) initRootKey() error {
	api := client.NewKeysAPI(ysf.etcdCli)
	_, err := api.Set(ysf.ctx, ysf.etcdKey, "", &client.SetOptions{
		Dir: true,
	})
	if err != nil {
		return fmt.Errorf("init %s key error: %v", ysf.etcdKey, err)
	}
	return nil
}

func (ysf *YCSnowflake) workerNotFoundInEtcd() (*client.Response, bool) {
	api := client.NewKeysAPI(ysf.etcdCli)
	key := ysf.etcdKey + "/" + strconv.Itoa(*ysf.Config.workerID)
	resp, err := api.Get(ysf.ctx, key, &client.GetOptions{
		Recursive: false,
		Sort:      false,
		Quorum:    true,
	})
	if keyNotFound(err) {
		return nil, true
	}
	return resp, false
}

func (ysf *YCSnowflake) errHandler(err error) {
	if err != nil {
		switch err {
		case context.Canceled:
			log.Fatalf("ctx is canceled by another routine: %v", err)
		case context.DeadlineExceeded:
			log.Fatalf("ctx is attached with a deadline is exceeded: %v", err)
		}
	}
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

func (ysf *YCSnowflake) createWorkerInEtcd() (*client.Response, error) {
	api := client.NewKeysAPI(ysf.etcdCli)
	v := ysf.marshalWorkerValue()
	key := ysf.etcdKey + "/" + strconv.Itoa(*ysf.Config.workerID)
	return api.Set(ysf.ctx, key, string(v), nil)
}

func (ysf *YCSnowflake) marshalWorkerValue() []byte {
	strconv.Itoa(int(time.Now().UnixNano() / int64(time.Millisecond)))
	b, _ := json.Marshal(map[string]string{
		"worker_id":      strconv.Itoa(*ysf.Config.workerID),
		"last_timestamp": strconv.Itoa(int(time.Now().UnixNano() / int64(time.Millisecond))),
	})
	return b
}

func (ysf *YCSnowflake) unMarshalWorkerValue(v string) (map[string]string, error) {
	data := make(map[string]string)
	err := json.Unmarshal([]byte(v), &data)
	return data, err
}

func (ysf *YCSnowflake) reportTimestamp() {
	t := time.Tick(time.Second * 3)
	for {
		<-t
		resp, err := ysf.createWorkerInEtcd()
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(resp.Node.Value)
	}
}

func timestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func (ysf *YCSnowflake) checkSystemTimestamp() {
	api := client.NewKeysAPI(ysf.etcdCli)
	api.Get(ysf.ctx, ysf.temporaryKey, nil)
}
