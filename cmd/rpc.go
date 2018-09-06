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
	address := ":" + s.port
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	rpcServer := grpc.NewServer()
	RegisterTaskServiceServer(rpcServer, s)
	log.Printf("rpc server start:\n\thost:\t%s\n\tport:\t%s", s.host, s.port)
	return rpcServer.Serve(lis)
}

func rpcGetSysTimestamp(host, port string) (*SysTimestamp, error) {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
	cc, err := grpc.DialContext(ctx, host+":"+port, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	cli := NewTaskServiceClient(cc)
	return cli.GetSysTimestamp(ctx, &Void{})
}
