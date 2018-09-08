package test

import (
	"github.com/Soul-Mate/yc-snowflake/grpc/client"
	"log"
	"testing"
)

func Test_Client(t *testing.T)  {
	cli, err := client.NewClient("", "32121")
	if err != nil {
		t.Error(err)
		return
	}

	sysTimestamp, err := cli.GetSysTimestamp()
	if err != nil {
		t.Error(err)
		return
	}
	log.Printf("get system timestamp: %d", sysTimestamp.Timestamp)
	if err = cli.Close(); err != nil {
		t.Error(err)
		return
	}
}

func Test_CerdClient(t *testing.T)  {
	cli, err := client.NewClient("", "32122",
		client.WithCredentials("./server.pem", "www.example.net"))
	if err != nil {
		t.Error(err)
		return
	}

	sysTimestamp, err := cli.GetSysTimestamp()
	if err != nil {
		t.Error(err)
		return
	}
	log.Printf("get system timestamp: %d", sysTimestamp.Timestamp)
	if err = cli.Close(); err != nil {
		t.Error(err)
	}
}
