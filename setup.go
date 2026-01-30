package bitcask_go

import (
	"bitcask-go/index"
	"os"
)

type Setup struct {
	DirPath      string // 数据库数据目录
	DataFileSize int64
	SyncWrites   bool            // 是否要每次写入后同步
	IndexType    index.IndexType // 索引类型
}

// IteratorSetup TODO: 为什么要添加这个 Prefix 字段呢？
type IteratorSetup struct {
	Prefix  []byte // 遍历前缀为指定的 Key，默认为空。似乎是可以能够对，就是针对更为具体的 Key。
	Reverse bool   // 是否反向遍历，默认 false 为正向
}

type WriteBatchSetup struct {
	MaxBatchNum uint // 一个批次中最大数据量
	SyncWrites  bool // 每一次事务提是否持久化
}

type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota + 1

	// ART Adpative Radix Tree 自适应基数树索引
	ART
)

var DefaultSetup = Setup{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256 MB
	SyncWrites:   false,
	IndexType:    BTree,
}

var DefaultIteratorSetup = IteratorSetup{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchSetup = WriteBatchSetup{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
