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

// Put 将键和logRecord位置信息存储到内存之中
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key, pos}
	bt.mu.Lock()
	defer bt.mu.Unlock() // 这里我们使用 defer

	// ReplaceOrInsert 函数期待的是一个**实现了 btree.Item 接口**的变量
	bt.btree.ReplaceOrInsert(it) // 实际上装入的是 &Item类型的变量
	return true                  // 肯定就是always返回true
}

// Get 通过key获取对应的索引信息
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}

	bt.mu.RLock()
	defer bt.mu.RUnlock()

	btreeItem := bt.btree.Get(it)
	if btreeItem == nil {
		return nil
	}

	// 内部都是 *Item类型的变量
	return btreeItem.(*Item).pos
}

// Delete 删除指定的key对应的索引记录
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

// BTree 索引迭代器
type btreeIterator struct {
	currIndex int     // 当前遍历的下标位置
	reverse   bool    // 是否是一个反向遍历
	values    []*Item // key+位置索引信息
}

func (b *btreeIterator) Rewind() {
	//TODO implement me
	panic("implement me")
}

func (b *btreeIterator) Seek(key []byte) {
	//TODO implement me
	panic("implement me")
}

func (b *btreeIterator) Next() {
	//TODO implement me
	panic("implement me")
}

func (b *btreeIterator) Valid() bool {
	//TODO implement me
	panic("implement me")
}

func (b *btreeIterator) Key() []byte {
	//TODO implement me
	panic("implement me")
}

func (b *btreeIterator) Value() *data.LogRecordPos {
	//TODO implement me
	panic("implement me")
}

func (b *btreeIterator) Close() {
	//TODO implement me
	panic("implement me")
}
