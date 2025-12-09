package bitcask_go

import (
	"bitcask-go/index"
	"bytes"
)

// Iterator 迭代器
type Iterator struct {
	indexIter index.Iterator // 索引迭代器
	db        *DB
	setup     IteratorSetup
}

// NewIterator 初始化迭代器
func (db *DB) NewIterator(opts IteratorSetup) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		db:        db,
		indexIter: indexIter,
		setup:     opts,
	}
}

/*
虽然大部分的方法，都可以调用索引迭代器来实现，但是部分函数不行。
*/

// Rewind 重新回到迭代器起点
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// Next 跳转到下一个 key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
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

	return it.db.getValueFromPos(logRecordPos)
}

func (it *Iterator) Close() {
	it.indexIter.Close()
}

// 关于 IteratorSetup.Prefix 相关的内容
// TODO: 为什么要做这一步呢？为什么要检查 prefix 内容？筛选，将全部前缀为 prefix 的过滤出来。
func (it *Iterator) skipToNext() {
	// prefix 长度为0，不需要做任何处理。
	prefixLen := len(it.setup.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		// 如果符合条件，我们就可以将其
		if prefixLen <= len(key) && bytes.Compare(it.setup.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
