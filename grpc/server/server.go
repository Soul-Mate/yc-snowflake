package server

import (
	"context"
	"github.com/Soul-Mate/yc-snowflake/pb"
	"github.com/Soul-Mate/yc-snowflake/snowflake"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"net"
)

type VerifySysTimestampServer struct{}

func (s *VerifySysTimestampServer) GetSysTimestamp(ctx context.Context, void *pb.Void) (*pb.SysTimestamp, error) {
	sysTimestamp := new(pb.SysTimestamp)
	sysTimestamp.Timestamp = snowflake.Timestamp()
	return sysTimestamp, nil
}

func (s *VerifySysTimestampServer) GetUUID(ctx context.Context, req *pb.RequestUUID) (*pb.ResponseUUID, error) {
	response := new(pb.ResponseUUID)
	response.Uuid = snowflake.New(int(req.WorkerId)).Gen()
	return response, nil
}

type options struct {
	ctx  context.Context
	certFile string
	keyFile  string
}

type ServerOption func(*options)

func ServerCreds(certFile, keyFile string) ServerOption {
	return func(opt *options) {
		opt.certFile = certFile
		opt.keyFile = keyFile
	}
}

func Server(host, port string, serverOptions ...ServerOption) error {
	opts := &options{}
	for _, serverOption := range serverOptions {
		serverOption(opts)
	}
	var rpcServer *grpc.Server
	if opts.certFile != "" && opts.keyFile != "" {
		creds, err := credentials.NewServerTLSFromFile(opts.certFile, opts.keyFile);
		if err != nil {
			return err
		}
		rpcServer = grpc.NewServer(grpc.Creds(creds))
	} else {
		rpcServer = grpc.NewServer()
	}

	lis, err := net.Listen("tcp", host+":"+port)

	if err != nil {
		return err
	}

	pb.RegisterTaskServiceServer(rpcServer, &VerifySysTimestampServer{})
	return rpcServer.Serve(lis)
}
