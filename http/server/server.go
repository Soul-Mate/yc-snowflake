package server

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Soul-Mate/yc-snowflake/snowflake"
	"io/ioutil"
	"net/http"
)

type options struct {
	clientAuth bool
	caFile     string
	certFile   string
	keyFile    string
}

type ServerOption func(*options)

func WithTLS(certFile, keyFile string) ServerOption {
	return func(opt *options) {
		opt.certFile = certFile
		opt.keyFile = keyFile
	}
}

func WithRootCA(caFile string) ServerOption {
	return func(opt *options) {
		opt.clientAuth = true
		opt.caFile = caFile
	}
}

func Server(host, port string, workerId int, serverOptions ...ServerOption) error {
	var server *http.Server
	opts := new(options)
	for _, serverOption := range serverOptions {
		serverOption(opts)
	}

	// 开启对客户端的认证
	if opts.clientAuth {
		if opts.caFile == "" {
			return errors.New("ca certificate required to enable client authentication")
		}

		data, err := ioutil.ReadFile(opts.caFile)
		if err != nil {
			return fmt.Errorf("failed to read ca certificate: %v\n", err)
		}

		pool := x509.NewCertPool()
		if ok := pool.AppendCertsFromPEM(data); !ok {
			return fmt.Errorf("add ca certificate failed.\n")
		}

		server = &http.Server{
			Addr:    host + ":" + port,
			Handler: nil,
			TLSConfig: &tls.Config{
				ClientCAs:  pool,
				ClientAuth: tls.RequireAndVerifyClientCert,
			},
		}

	} else {
		server = &http.Server{
			Addr:    host + ":" + port,
			Handler: nil,
		}
	}

	http.HandleFunc("/uuid", id(workerId))

	if opts.certFile != "" && opts.keyFile != "" {
		return server.ListenAndServeTLS(opts.certFile, opts.keyFile)
	}

	return server.ListenAndServe()
}

func id(workerId int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("404 not found"))
			fmt.Fprintln(w)
			return
		}
		id := snowflake.New(workerId).Gen()
		data, err := makeSuccessResponse(http.StatusOK, "snowflake generate id success", id)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("500 internal server error"))
			fmt.Fprintln(w)
			fmt.Println(err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
		fmt.Fprintln(w)
		return
	}
}

func makeSuccessResponse(code int, message string, v interface{}) ([]byte, error) {
	data := make(map[string]interface{})
	data["code"] = code
	data["message"] = message
	data["uuid"] = v
	return json.Marshal(&data)
}
