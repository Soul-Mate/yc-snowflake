package snowflake

import "time"

const (
	nano2Mill = 1000000
	// 起始的毫秒时间戳
	epoch = 1535606279258
	// 机器占用的bit数
	workerBits = 10
	// 序列号占用的bit数
	sequenceBits = 12
	// 最多承载的机器数量
	MaxWorkerId = -1 ^ (-1 << workerBits)
	// 最大承载的序列号数量
	maxSequence = -1 ^ (-1 << sequenceBits)
	// 机器bit偏移位数
	workerLeftShift = sequenceBits
	// 时间戳bit偏移数量
	timestampLeftShift = workerBits + sequenceBits
)

type SnowFlake struct {
	workerId      int
	sequence      int
	lastTimestamp uint64
}

func New(workerId int) *SnowFlake {
	if workerId > MaxWorkerId {
		panic("worker id error")
	}
	return &SnowFlake{
		workerId: workerId,
	}
}

func (sf *SnowFlake) Gen() uint64 {
	ts := sf.timestamp()
	if ts < sf.lastTimestamp {
		panic("clock error")
	}

	if ts == sf.lastTimestamp {
		// 递增序列号
		// 如果超过了序列号的范围, 等待下一时钟
		if sf.sequence = (sf.sequence + 1) & maxSequence; sf.sequence == 0 {
			sf.tilNextMillSecond(ts)
		}
	} else {
		// 不同时钟周期, 序列号从0开始
		sf.sequence = 0
	}
	sf.lastTimestamp = ts
	return (sf.lastTimestamp << timestampLeftShift) | uint64(sf.workerId<<workerLeftShift) | uint64(sf.sequence)

}

func (sf *SnowFlake) tilNextMillSecond(ts uint64) uint64 {
	if ts == sf.lastTimestamp {
		time.Sleep(time.Millisecond)
		ts = sf.timestamp()
	}
	return ts
}

func (sf *SnowFlake) timestamp() uint64 {
	return uint64(time.Now().UnixNano()/int64(nano2Mill) - epoch)
}
