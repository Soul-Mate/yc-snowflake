package test

import (
	"github.com/Soul-Mate/yc-snowflake/http/server"
	"testing"
)

func Test_HttpServer(t *testing.T) {
	err := server.Server("", "8887", 2)
	if err != nil {
		t.Error(err)
	}
}

func Test_HttpsServer(t *testing.T) {
	err := server.Server("", "8888", 1,
		server.WithClientAuth("./ca.pem", "./server.pem", "./server-key.pem"))
	if err != nil {
		t.Error(err)
	}
}
