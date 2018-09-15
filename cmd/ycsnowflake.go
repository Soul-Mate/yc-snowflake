package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/Soul-Mate/yc-snowflake/config"
	"github.com/Soul-Mate/yc-snowflake/etcd"
	"github.com/Soul-Mate/yc-snowflake/grpc/client"
	grpcServer "github.com/Soul-Mate/yc-snowflake/grpc/server"
	httpServer "github.com/Soul-Mate/yc-snowflake/http/server"
	"github.com/Soul-Mate/yc-snowflake/logger"
	"github.com/Soul-Mate/yc-snowflake/pb"
	"github.com/Soul-Mate/yc-snowflake/snowflake"
	"math"
	"os"
	"time"
)

const (
	// 最大忍受时间发生回拨时常 单位(毫秒)
	maxEndureMs = 5
	// 最大系统时间误差 单位(毫秒)
	maxSysTimeErrorMs = 100
)

type YCSnowflake struct {
	etcdService *etcd.EtcdService
	conf        *config.Config
	ctx         context.Context
	cancel      context.CancelFunc
	logger      *logger.WrapLog
}

func NewYCSnowflake() *YCSnowflake {
	ysf := new(YCSnowflake)
	ysf.conf = &config.Config{}
	ysf.etcdService = etcd.NewEtcdService()
	ysf.logger = logger.NewLogger(os.Stdout)
	ysf.ctx, ysf.cancel = context.WithCancel(context.Background())
	return ysf
}

func (ysf *YCSnowflake) Start() {
	var err error
	ysf.conf.ParseCmd()

	// 如果指定了配置文件,则从配置文件中初始化程序
	if err = ysf.conf.InitConfigFromFile(); err != nil {
		ysf.logger.Error(err.Error())
	}

	// 验证worker id的有效性
	if err = ysf.conf.CheckWorkerIdConfig(); err != nil {
		ysf.logger.Error(err.Error())
	}

	// 初始化并验证rpc配置
	if err = ysf.conf.InitRpcServiceConfig(); err != nil {
		ysf.logger.Error(err.Error())
	}

	// 初始化并验证http配置
	if err = ysf.conf.InitHttpServiceConfig(); err != nil {
		ysf.logger.Error(err.Error())
	}

	// 初始化并验证etcd service
	if err = ysf.conf.InitEtcdServiceConfig(); err != nil {
		ysf.logger.Error(err.Error())
	}

	// 初始化etcd clientv3
	if err = ysf.etcdService.InitClientv3(ysf.conf.Etcd); err != nil {
		ysf.logger.Error(err.Error())
	}

	wv, err := ysf.etcdService.GetWorkerValue(ysf.conf.WorkerId)
	if err != nil {
		ysf.logger.Error(err.Error())
	}

	// 当前节点如果是新的worker：
	// 通过rpc调用所有节点进行时间值校验
	// 在etcd中设置新的节点信息

	// 当前节点如果是旧的worker:
	// 比较当前时间和存储在etcd最后一次的时间, 如果发生了回拨(此刻时间小于上次发号时间)
	// 如果时间偏差在一个可接受的范围内(5ms) 则等待两倍的时间, 否则上报异常
	// 如果没有发生回拨, 则校验时间
	if wv == nil {
		if err = ysf.checkSysTimestamp(); err != nil {
			ysf.logger.Error(err.Error())
		}

		// 设置新的worker 节点值
		if err = ysf.etcdService.PutWorkerValue(ysf.conf.WorkerId); err != nil {
			ysf.logger.Error(err.Error())
		}

	} else {
		ts := snowflake.Timestamp()
		// 时间发生了回拨
		if ts < wv.LastTimestamp {
			// 等待两倍时常
			if offset := wv.LastTimestamp - ts; offset < maxEndureMs {
				time.Sleep(time.Millisecond * time.Duration(offset<<1))
			} else {
				ysf.logger.Error(err.Error())
			}

		} else {
			if err = ysf.checkSysTimestamp(); err != nil {
				ysf.logger.Error(err.Error())
			}
		}
	}

	if err = ysf.etcdService.PutWorkerServerValue(ysf.conf.WorkerId, ysf.conf.GRpc.Host, ysf.conf.GRpc.Port); err != nil {
		ysf.logger.Error(err.Error())
	}

	// 日志输出追加到用户自定义文件
	if err = ysf.logger.SetFileWriter(ysf.conf.LogFile); err != nil {
		ysf.logger.Error(err.Error())
	}

	// 启动http server
	go ysf.startHttpServer()

	// 定时上报系统时间戳
	go ysf.reportTimestamp(ysf.ctx)

	// 启用 grpc server
	if err = ysf.startGRpcServer(); err != nil {
		ysf.logger.Error(err.Error())
	}
}

func (ysf *YCSnowflake) checkSysTimestamp() error {
	var (
		successWorkerSysTimestamp []*pb.SysTimestamp
	)
	res, successWorkerServer, err := ysf.etcdService.GetWorkerServerValues(ysf.conf.WorkerId)
	if res.Count <= 0 || len(successWorkerServer) <= 0 {
		if ysf.conf.SkipCheck {
			return nil
		}

		err = fmt.Errorf("%s\nsystem time info\n\tcurrent time:\t\t%s\n\tcurrent timestamp:\t%d",
			"no worker server provider system time verify, you can enable -no-worker-skip-check.",
			time.Now().Format("2006-01-02 15:04:05"),
			timestamp())
		return err
	}

	for _, node := range successWorkerServer {
		rpcClient, err := client.NewClient(node.RPCHost, node.RPCPort, client.WithTimeout(time.Second*3))
		if err != nil {
			ysf.logger.Printf(logger.LWarn, "grpc client(%s:%s) init error: %v",
				node.RPCHost, node.RPCPort, err)
		} else {
			sysTimestamp, err := rpcClient.GetSysTimestamp()
			if err != nil {
				ysf.logger.Printf(logger.LWarn, "grpc client(%s:%s) call GetSysTimestamp error: %v",
					node.RPCHost, node.RPCPort, err)
			} else {
				successWorkerSysTimestamp = append(successWorkerSysTimestamp, sysTimestamp)
			}
			if err = rpcClient.Close(); err != nil {
				ysf.logger.Printf(logger.LWarn, "grpc client(%s:%s) close error: %v",
					node.RPCHost, node.RPCPort, err)
			}
		}
	}

	if len(successWorkerSysTimestamp) <= 0 {
		return nil
	}

	var (
		avg   uint64
		total uint64
	)

	for _, result := range successWorkerSysTimestamp {
		total += result.Timestamp
	}

	avg = total / uint64(len(successWorkerSysTimestamp))

	ysf.logger.Printf(logger.LDebug, "求得平均值误差: %d", avg)

	// 误差在100毫秒以内
	if math.Abs(float64(timestamp()-avg)) > maxSysTimeErrorMs {
		return errors.New("the node system timestamp error exceeds the threshold")
	}

	return nil
}

func (ysf *YCSnowflake) reportTimestamp(ctx context.Context) {
	done := false
	t := time.Tick(time.Second * 3)
	for {
		select {
		case <-t:
			if err := ysf.etcdService.PutWorkerValue(ysf.conf.WorkerId); err != nil {
				ysf.logger.Printf(logger.LDebug, "[worker-%d]: report timestamp error: %v", err, ysf.conf.WorkerId)
			} else {
				ysf.logger.Printf(logger.LDebug, "[worker-%d]: report timestamp success.", ysf.conf.WorkerId)
			}
		case <-ctx.Done():
			ysf.logger.Printf(logger.LDebug, "report done.")
			done = true
		default:
		}
		if done {
			break
		}
	}
}

func timestamp() uint64 {
	return uint64(time.Now().UnixNano() / int64(time.Millisecond))
}

func (ysf *YCSnowflake) startGRpcServer() error {
	if ysf.conf.GRpc.EnableTls {
		err := grpcServer.Server(ysf.conf.GRpc.Host, ysf.conf.GRpc.Port,
			grpcServer.ServerCreds(ysf.conf.GRpc.CertFile, ysf.conf.GRpc.KeyFile))
		if err != nil {
			return fmt.Errorf("startGRpcServer error: %v\n", err)
		}

	} else {
		if err := grpcServer.Server(ysf.conf.GRpc.Host, ysf.conf.GRpc.Port); err != nil {
			return fmt.Errorf("startGRpcServer error: %v\n", err)
		}
	}

	return nil
}

func (ysf *YCSnowflake) startHttpServer() error {
	if ysf.conf.Http.ClientAuth {
		return httpServer.Server(ysf.conf.Http.Host, ysf.conf.Http.Port, ysf.conf.WorkerId,
			httpServer.WithClientAuth(ysf.conf.Http.CaFile, ysf.conf.Http.CertFile, ysf.conf.Http.KeyFile))

	} else if ysf.conf.Http.CertFile != "" && ysf.conf.Http.KeyFile != "" {
		return httpServer.Server(ysf.conf.Http.Host, ysf.conf.Http.Port, ysf.conf.WorkerId,
			httpServer.WithTLS(ysf.conf.Http.CertFile, ysf.conf.Http.KeyFile))

	} else {
		return httpServer.Server(ysf.conf.Http.Host, ysf.conf.Http.Port, ysf.conf.WorkerId)
	}
}

