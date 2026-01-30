package index

import (
	"bitcask-go/data"
	"bytes"

	"github.com/google/btree"
)

// Indexer 索引接口，应该就是存储在内存部分的，用于快速检索数据的“本子”
type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool // 写入操作
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
	Iterator(reverse bool) Iterator // 索引迭代器
	Size() int
}

type IndexType = int8

const (
	Btree IndexType = iota + 1
	ART
)

func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return NewART()
	default:
		panic("unknown index type")
	}
}

// Item 对 key 和 logRecordPos 的封装
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool { // Go语言中，方法接收者决定了哪个类型实现了该方法
	// bi.(*Item): 被称为“类型断言”，相当于：编译器知道bi中存储的有*Item类型的变量，让它(bi 接口)取出来
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// Iterator 通用索引迭代器的接口
type Iterator interface {
	// Rewind 重新回到迭代器的起点，即第一个数据
	Rewind()

	// Seek 根据传入的 key 查找到第一个大于等于的 key，根据这个 key 开始遍历
	Seek(key []byte)

	// Next 跳转到下一个 key
	Next()

	// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
	Valid() bool

	// Key 当前遍历位置的 Key 数据
	Key() []byte

	// Value 当前遍历位置的 Value 数据
	Value() *data.LogRecordPos

	// Close 关闭迭代器，释放相应资源
	Close()
}
