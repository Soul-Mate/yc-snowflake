package main

import (
	"net"
	"google.golang.org/grpc"
	"context"
	"time"
)

type CheckSysTimestampServer struct {
	ip string
	port string
	address string
}

func newCheckSysTimestampServer(ip, port string) *CheckSysTimestampServer {
	return &CheckSysTimestampServer{
		ip:ip,
		port:port,
	}
}

func (s *CheckSysTimestampServer) GetSysTimestamp(ctx context.Context, void *Void) (*SysTimestamp, error) {
	sysTimestamp := new(SysTimestamp)
	sysTimestamp.Timestamp = uint64(time.Now().UnixNano() / int64(time.Millisecond))
	return sysTimestamp, nil
}

func (s *CheckSysTimestampServer) start() error {
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	rpcServer := grpc.NewServer()
	RegisterTaskServiceServer(rpcServer, s)
	return rpcServer.Serve(lis)
}
