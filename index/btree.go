package index

import (
	"bitcask-go/data"
	"sync"

	"github.com/google/btree"
)

// BTree 索引，主要是封装了 google 的 btree kv
// 我们的BTree就实现了Indexer接口，这样未来如果是更换为其他数据结构的话，比如哈希表，会更灵活
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex // 提供了读写互斥锁 -- Read-Write Mutex
}

// NewBTree 初始化 BTree 索引结构
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: &sync.RWMutex{}, // Or `new(sync.RWMutex)`
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := Item{key: key, pos: pos}
	bt.lock.Lock() // 类似于上锁，上锁后只有这一个线程可以调用，其他线程无法调用，从而避免了竞态条件
	bt.tree.ReplaceOrInsert(&it)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	bt.lock.RLock() // 获取读锁
	btreeItem := bt.tree.Get(it)
	bt.lock.RUnlock() // 释放读锁

	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock() // 获取写锁
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock() // 释放写锁
	if oldItem == nil {
		return false
	}
	return true
}
