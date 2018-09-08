package client

import (
	"context"
	"github.com/Soul-Mate/yc-snowflake/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"time"
)

type Client struct {
	conn *grpc.ClientConn
}

type options struct {
	certFile   string
	serverName string
	timeout    time.Duration
}

type ClientOption func(*options)

func WithCredentials(certFile, serverName string) ClientOption {
	return func(opt *options) {
		opt.certFile = certFile
		opt.serverName = serverName
	}
}

func WithTimeout(timeout time.Duration) ClientOption {
	return func(opt *options) {
		opt.timeout = timeout
	}
}

func NewClient(host, port string, clientOptions ...ClientOption) (*Client, error) {
	var (
		err        error
		clientConn *grpc.ClientConn
	)
	opts := new(options)

	for _, clientOption := range clientOptions {
		clientOption(opts)
	}

	ctx := context.Background()
	if opts.timeout != 0 {
		ctx, _ = context.WithTimeout(ctx, opts.timeout)
	}

	if opts.certFile != "" {
		creds, err := credentials.NewClientTLSFromFile(opts.certFile, opts.serverName)
		if err != nil {
			return nil, err
		}

		clientConn, err = grpc.DialContext(ctx, host+":"+port, grpc.WithTransportCredentials(creds))
		if err != nil {
			return nil, err
		}
	} else {
		clientConn, err = grpc.DialContext(ctx, host+":"+port, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
	}

	return &Client{
		conn: clientConn,
	}, nil
}

func (cli *Client) GetSysTimestamp() (*pb.SysTimestamp, error) {
	taskClient := pb.NewTaskServiceClient(cli.conn)
	return taskClient.GetSysTimestamp(context.Background(), &pb.Void{})
}

func (cli *Client) GetUUID(workerId uint64) (*pb.ResponseUUID, error) {
	taskClient := pb.NewTaskServiceClient(cli.conn)
	req := &pb.RequestUUID{
		WorkerId: workerId,
	}
	return taskClient.GetUUID(context.Background(), req)
}

func (cli *Client) Close() error {
	return cli.conn.Close()
}
