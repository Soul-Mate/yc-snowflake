package test

import (
	"fmt"
	"github.com/Soul-Mate/yc-snowflake/http/client"
	"testing"
)

func Test_HttpClient(t *testing.T) {
	cli, err := client.NewClient("http://127.0.0.1:8887")
	if err != nil {
		t.Error(err)
		return
	}

	uuid, err := cli.GetUUID()
	if err != nil {
		t.Error(err)
	}
	fmt.Println("get uuid in http server: ", uuid)

}

func Test_HttpsClient(t *testing.T) {
	cli, err := client.NewClient("https://127.0.0.1:8888",
		client.WithRootCAs("./ca.pem"),
		client.WithCertificate("./client.pem", "client-key.pem"))
	if err != nil {
		t.Error(err)
		return
	}
	uuid, err := cli.GetUUID()
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println("get uuid in https server: ", uuid)
}
