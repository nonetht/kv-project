package bitcask_go

import "bitcask-go/index"

type Setup struct {
	DirPath      string // 数据库数据目录
	DataFileSize int64
	SyncWrites   bool
	IndexType    index.IndexType
}

type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota + 1

	// ART Adpative Radix Tree
	ART
)
