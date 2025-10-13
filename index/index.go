package index

import "bitcask-go/data"

// Indexer 抽象索引接口，后续如果想要接入其他数据结构，直接实现这个接口即可
type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
}
