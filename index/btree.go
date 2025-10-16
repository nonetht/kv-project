package index

import (
	"bitcask-go/data"
	"sync"

	"github.com/google/btree"
)

type BTree struct {
	btree *btree.BTree  // 非线程安全
	mu    *sync.RWMutex // 读写锁
}

func NewBTree() *BTree {
	return &BTree{
		btree: btree.New(32),
		mu:    &sync.RWMutex{},
	}
}

// 接下来我们要让BTree能够实现Indexer接口

// Put 将键和logRecord位置信息存储到内存之中
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key, pos}
	bt.mu.Lock()
	defer bt.mu.Unlock() // 这里我们使用 defer

	// ReplaceOrInsert 函数期待的是一个实现了 btree.Item 接口的变量
	bt.btree.ReplaceOrInsert(it) // 实际上装入的是 &Item类型的变量
}

// Get
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key}

	bt.mu.RLock()
	defer bt.mu.RUnlock()

	btreeItem := bt.btree.Get(it)
	if btreeItem == nil {
		return nil
	}

	// 内部都是 *Item类型的变量
	return btreeItem.(*Item).pos
}

// Delete
func (bt *BTree) Delete(key []byte) bool {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	it := &Item{key: key}
	oldItem := bt.btree.Delete(it)
	if oldItem == nil {
		return false
	}
	return true
}
