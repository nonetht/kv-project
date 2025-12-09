package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
)

// Iterator 迭代器
type Iterator struct {
	indexIter index.Iterator // 索引迭代器
	db        *DB
	options   IteratorSetup
}

// NewIterator 初始化迭代器
func (db *DB) NewIterator(opts IteratorSetup) *Iterator {
	db.index.Iterator(opts.Reverse).Rewind()
}

// Rewind 重新回到迭代器起点
func (it *Iterator) Rewind() {
	//TODO implement me
	panic("implement me")
}

func (it *Iterator) Seek(key []byte) {
	//TODO implement me
	panic("implement me")
}

func (it *Iterator) Next() {
	//TODO implement me
	panic("implement me")
}

func (it *Iterator) Valid() bool {
	//TODO implement me
	panic("implement me")
}

func (it *Iterator) Key() []byte {
	//TODO implement me
	panic("implement me")
}

func (it *Iterator) Value() *data.LogRecordPos {
	//TODO implement me
	panic("implement me")
}

func (it *Iterator) Close() {
	//TODO implement me
	panic("implement me")
}
