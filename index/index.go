package index

import (
	"bitcask-go/data"
	"bytes"

	"github.com/google/btree"
)

/* 有了Indexer接口，数据库代码只跟这份“合同”打交道，知道有Indexer类型的成员，调用myIndexer.Put()函数即可
一份“行为合同”，规定了一个“合格的索引”必须具备哪些能力，不管你的内部如何实现的
比如初始化的时候，如果想要以BTree()进行实现，那么`db.index = NewBTree()`即可
*/

// Indexer 抽象索引接口，后续如果想要接入其他数据结构，直接实现这个接口即可
type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool // 有能力“存放”一个索引
	Get(key []byte) *data.LogRecordPos           // 有能力“获取”一个索引
	Delete(key []byte) bool                      // 有能力“删除”一个索引
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// Less 接收者为*Item类型的函数，实现了Item接口
func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
