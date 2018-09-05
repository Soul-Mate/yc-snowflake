package main

import (
	"net"
	"google.golang.org/grpc"
	"context"
	"time"
	"log"
)

type CheckSysTimestampServer struct {
	host string
	port string
}

type CheckSysTimestampClient struct {
	host     string
	port     string
	protocol string
}

func newCheckSysTimestampServer(host, port string) *CheckSysTimestampServer {
	return &CheckSysTimestampServer{
		host: host,
		port: port,
	}
}

func (s *CheckSysTimestampServer) GetSysTimestamp(ctx context.Context, void *Void) (*SysTimestamp, error) {
	sysTimestamp := new(SysTimestamp)
	sysTimestamp.Timestamp = uint64(time.Now().UnixNano() / int64(time.Millisecond))
	return sysTimestamp, nil
}

func (s *CheckSysTimestampServer) start() error {
	address := s.host + ":" + s.port
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	rpcServer := grpc.NewServer()
	RegisterTaskServiceServer(rpcServer, s)
	log.Printf("rpc server start:\n\thost:\t%s\n\tport:\t%s", s.host, s.port)
	return rpcServer.Serve(lis)
}

func newCheckSysTimestampClient(protocol, host, port string) *CheckSysTimestampClient{
	return &CheckSysTimestampClient{
		protocol:protocol,
		host:host,
		port:port,
	}
}

func (s *CheckSysTimestampClient) CreateClient() (TaskServiceClient, error) {
	ctx, _ := context.WithTimeout(context.Background(), time.Second * 3)
	cc, err := grpc.DialContext(ctx, s.protocol + ":" + s.host + ":" + s.port)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	cli := NewTaskServiceClient(cc)
	return cli, nil
}

func (s *CheckSysTimestampClient) Call(cli TaskServiceClient) (*SysTimestamp, error) {
	ctx, _ := context.WithTimeout(context.Background(), time.Second * 3)
	return cli.GetSysTimestamp(ctx, &Void{})
}