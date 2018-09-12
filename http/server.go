package http

import (
	"encoding/json"
	"fmt"
	"github.com/Soul-Mate/yc-snowflake/snowflake"
	"net/http"
)

type options struct {
	clientAuth bool
	certFile   string
	keyFile    string
}

type ServerOption func(*options)

func TLS(certFile, keyFile string) ServerOption {
	return func(opt *options) {
		opt.certFile = certFile
		opt.keyFile = keyFile
	}
}



func Server(host string, port string, workerId int) error {
	return server(host, port, "", "", workerId)
}

//func ServerTLS(host, port, certFile, keyFile string, workerId int) error{
//	return server(host, port, certFile, keyFile, workerId)
//}

func server(host, port, certFile, keyFile string, workerId int) error {
	http.HandleFunc("/id", idHandler(workerId))
	if certFile != "" && keyFile != "" {
		return http.ListenAndServeTLS(host+":"+port, certFile, keyFile, nil)
	}

	return http.ListenAndServe(host+":"+port, nil)
}

func idHandler(workerId int) http.HandlerFunc {
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
	data["id"] = v
	return json.Marshal(&data)
}
