package etcd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"github.com/Soul-Mate/yc-snowflake/config"
	"github.com/Soul-Mate/yc-snowflake/snowflake"
	"github.com/etcd-io/etcd/clientv3"
	"io/ioutil"
	"strconv"
	"strings"
	"unsafe"
)

type WorkerValue struct {
	Id            int    `json:"id"`
	LastTimestamp uint64 `json:"last_timestamp"`
}

type WorkerServerValue struct {
	RPCHost string `json:"rpc_host"`
	RPCPort string `json:"rpc_port"`
}

type EtcdService struct {
	cli          *clientv3.Client
	key          string
	temporaryKey string
}

func NewEtcdService() *EtcdService {
	return &EtcdService{
		key:          "/yc_snowflake_forever",
		temporaryKey: "/yc_snowflake_temporary",
	}
}

func (e *EtcdService) InitClientv3(conf config.EtcdConfig) error {
	cfg := clientv3.Config{}
	cfg.Endpoints = strings.Split(conf.Cluster, ",")
	if tlsConfig, err := e.createTlsConfig(conf); tlsConfig != nil && err == nil {
		cfg.TLS = tlsConfig
	}

	cli, err := clientv3.New(cfg)
	if err != nil {
		return err
	}

	e.cli = cli
	return nil
}

func (e *EtcdService) createTlsConfig(conf config.EtcdConfig) (*tls.Config, error) {
	// client insecure
	if conf.EnableTLS && conf.Insecure {
		return &tls.Config{
			InsecureSkipVerify: true,
		}, nil
	}

	// enable tls connection
	if conf.EnableTLS {
		if certPool, err := e.wrapNewCertPool(conf.CaFile); err != nil {
			return nil, err
		} else {
			return &tls.Config{RootCAs: certPool}, nil
		}
	}

	// enable client auth
	if conf.ClientAuth {
		if certPool, err := e.wrapNewCertPool(conf.CaFile); err != nil {
			return nil, err
		} else {
			cliCertificate, err := tls.LoadX509KeyPair(conf.ClientCertFile, conf.ClientKeyFile)
			if err != nil {
				return nil, err
			} else {
				return &tls.Config{
					RootCAs:      certPool,
					Certificates: []tls.Certificate{cliCertificate},
				}, nil
			}
		}
	}

	return nil, nil
}

func (e *EtcdService) wrapNewCertPool(caFile string) (*x509.CertPool, error) {
	var (
		err     error
		caBytes []byte
	)
	if caBytes, err = ioutil.ReadFile(caFile); err != nil {
		return nil, err
	}
	certPool := x509.NewCertPool()

	if ok := certPool.AppendCertsFromPEM(caBytes); ok {
		return nil, errors.New("the e clinet use ca file cannot certPool.AppendCertsFromPEM")
	}
	return certPool, nil
}

func (e *EtcdService) GetWorkerValue(workerId int) (*WorkerValue, error) {
	kv := clientv3.NewKV(e.cli)
	key := e.workerKey(workerId)
	res, err := kv.Get(context.Background(), key)
	if err != nil {
		return nil, err
	}

	for _, item := range res.Kvs {
		if string(item.Key) == key {
			if wv, err := e.unMarshalWorkerValue(item.Value); err == nil {
				return wv, err
			}

			// 如果解析失败, put新的值 防止kv被污染,然后进行重试
			if err = e.PutWorkerValue(workerId); err != nil {
				return nil, err
			}

			if res, err = kv.Get(context.Background(), key); err != nil {
				return nil, err
			}

			for _, item := range res.Kvs {
				if string(item.Key) == key {
					return e.unMarshalWorkerValue(item.Value)
				}
			}
		}
	}

	return nil, nil
}

func (e *EtcdService) PutWorkerValue(workerId int) error {
	value, err := e.marshalWorkerValue(workerId)
	if err != nil {
		return err
	}
	key := e.workerKey(workerId)
	kv := clientv3.NewKV(e.cli)
	_, err = kv.Put(context.Background(), key, value)

	if err != nil {
		return err
	}

	return nil
}

func (e *EtcdService) marshalWorkerValue(workerId int) (string, error) {
	worker := &WorkerValue{
		Id:            workerId,
		LastTimestamp: snowflake.Timestamp(),
	}

	b, err := json.Marshal(worker)

	if err != nil {
		return "", err
	}

	return *(*string)(unsafe.Pointer(&b)), nil
}

func (e *EtcdService) unMarshalWorkerValue(value []byte) (*WorkerValue, error) {
	w := new(WorkerValue)
	if err := json.Unmarshal(value, w); err != nil {
		return nil, err
	}
	return w, nil
}

func (e *EtcdService) workerKey(workerId int) string {
	return e.key + "/" + strconv.Itoa(workerId)
}

func (e *EtcdService) PutWorkerServerValue(workerId int, host, port string) error {
	key := e.workerTemporaryKey(workerId)
	if b, err := e.marshalWorkerServerValue(host, port); err != nil {
		return err
	} else {
		kv := clientv3.NewKV(e.cli)
		if _, err = kv.Put(context.Background(), key, string(b)); err != nil {
			return err
		}
	}
	return nil
}

func (e *EtcdService) GetWorkerServerValues(workerId int) (*clientv3.GetResponse, []*WorkerServerValue, error) {
	var (
		successWorkerServer []*WorkerServerValue
	)
	kv := clientv3.NewKV(e.cli)
	res, err := kv.Get(context.Background(), e.temporaryKey, clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))

	if err != nil {
		return nil, nil, err
	}

	for _, item := range res.Kvs {
		w, err := e.unMarshalWorkerServerValue(item.Value)

		if string(item.Key) != e.workerTemporaryKey(workerId) && err == nil {
			successWorkerServer = append(successWorkerServer, w)
		}
	}
	return res, successWorkerServer, nil
}

func (e *EtcdService) marshalWorkerServerValue(host, port string) ([]byte, error) {
	ws := &WorkerServerValue{
		RPCHost: host,
		RPCPort: port,
	}

	return json.Marshal(ws)
}

func (e *EtcdService) unMarshalWorkerServerValue(value []byte) (*WorkerServerValue, error) {
	workerServerValue := new(WorkerServerValue)
	if err := json.Unmarshal(value, workerServerValue); err != nil {
		return nil, err
	}

	return workerServerValue, nil
}

func (e *EtcdService) workerTemporaryKey(workerId int) string {
	return e.temporaryKey + "/" + strconv.Itoa(workerId)
}
