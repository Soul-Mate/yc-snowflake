package client

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
)

type Client struct {
	address string
	goCli   *http.Client
}

type options struct {
	caFile   string
	certFile string
	keyFile  string
	insecure bool
}

type ClientOption func(*options)

func WithInsecure() ClientOption {
	return func(opts *options) {
		opts.insecure = true
	}
}

func WithRootCAs(caFile string) ClientOption {
	return func(opts *options) {
		opts.insecure = false
		opts.caFile = caFile
	}
}

func WithCertificate(certFile, keyFile string) ClientOption {
	return func(opts *options) {
		opts.insecure = false
		opts.certFile = certFile
		opts.keyFile = keyFile
	}
}

func NewClient(address string, clientOptions ...ClientOption) (*Client, error) {
	opts := &options{}
	tlsConfig := &tls.Config{}

	for _, clientOption := range clientOptions {
		clientOption(opts)
	}

	if opts.insecure {
		tlsConfig.InsecureSkipVerify = true
	}

	if opts.caFile != "" {
		data, err := ioutil.ReadFile(opts.caFile)
		if err != nil {
			return nil, err
		}

		pool := x509.NewCertPool()
		if ok := pool.AppendCertsFromPEM(data); !ok {
			return nil, err
		}
		tlsConfig.RootCAs = pool
	}

	if opts.certFile != "" && opts.keyFile != "" {
		cliCa, err := tls.LoadX509KeyPair(opts.certFile, opts.keyFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{cliCa}
	}

	if opts.insecure {
		tlsConfig.InsecureSkipVerify = true
	}

	client := new(Client)
	client.goCli = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}
	client.address = address
	return client, nil
}

func (cli *Client) GetUUID() (uint64, error) {
	path := "/uuid"
	resp, err := cli.goCli.Get(cli.address + path)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	responseMap := make(map[string]interface{})
	if err = json.Unmarshal(data, &responseMap); err != nil {
		return 0, err
	}
	if uuid, ok := responseMap["uuid"]; !ok {
		return 0, errors.New("uuid key does not exist in the response map")
	} else {
		if uuidUint64, ok := uuid.(float64); !ok {
			return 0, errors.New("uuid cannot be converted to uint64 type")
		} else {
			return uint64(uuidUint64), nil
		}
	}
}
