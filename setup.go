package bitcask_go

import "bitcask-go/index"

type Setup struct {
	DirPath      string // 数据库数据目录
	DataFileSize int64
	SyncWrites   bool            // 是否要每次写入后同步
	IndexType    index.IndexType // 索引类型
}

type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota + 1

	// ART Adpative Radix Tree
	ART
)
