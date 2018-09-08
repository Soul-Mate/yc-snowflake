package test

import (
	"github.com/Soul-Mate/yc-snowflake/grpc/server"
	"testing"
)

func Test_Server(t *testing.T) {
	if err := server.Server("", "32121"); err != nil {
		t.Error(err)
	}
}

func Test_CredsServer(t *testing.T) {
	if err := server.Server("", "32122", server.ServerCreds("./server.pem", "./server-key.pem")); err != nil {
		t.Error(err)
	}
}
