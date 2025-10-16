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
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// Less 只有实现了该方法，才算是实现了 Item 接口
// bi 是一个接口，但是为什么...可以这样使用呢？
// 我们可以将接口想象为一个容器，该容器存储两样东西：1. 指向它存储的“具体值”的指针。 2. 指向它存储的“具体值”的类型的指针。
// 函数接收者 *Item 决定了到底是哪个类型实现了该接口. 接收者为 (t *T) - 只有指针类型实现了该方法；接收者为 (t T) 则值类型和指针类型都实现了该方法
func (ai *Item) Less(bi btree.Item) bool { // Go语言中，方法接收者决定了哪个类型实现了该方法
	// bi.(*Item): 被称为“类型断言”，相当于：编译器知道bi中存储的有*Item类型的变量，让它(bi 接口)取出来
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
