package bitcask_go

import (
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
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		db:        db,
		indexIter: indexIter,
		options:   opts,
	}
}

/*
虽然大部分的方法，都可以调用索引迭代器来实现，但是部分函数不行。
*/

// Rewind 重新回到迭代器起点
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
}

func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
}

func (it *Iterator) Next() {
	it.indexIter.Next()
}

func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// Key 当前遍历位置的 Key 数据
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value 当前遍历位置的 Value 数据
// 注意，这个有所不同，之前的获取value实际上是 logRecordPos。我们是要获取存储的数据，不是这个而是存储的 Value
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.mu.Lock()
	defer it.db.mu.RUnlock()

	value, err := it.db.getValueFromPos(logRecordPos)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (it *Iterator) Close() {
	it.indexIter.Close()
}
