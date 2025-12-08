package index

import (
	"bitcask-go/data"
	"bytes"
	"sort"
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

// 新建 BTreeIterator，b树迭代器
func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int // 初始值为0
	values := make([]*Item, tree.Len())

	// 将所有的数据存放到数组中
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item) // ?
		idx++
		return true // 返回 false 会终止遍历，但是我也没有执行遍历...
	}

	if reverse {
		// 从大到小
		tree.Descend(saveValues)
	} else {
		// 从小到大
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (b *btreeIterator) Rewind() {
	b.currIndex = 0
}

func (b *btreeIterator) Seek(key []byte) {
	if b.reverse {
		b.currIndex = sort.Search(len(b.values), func(i int) bool {
			return bytes.Compare(b.values[i].key, key) <= 0
		})
	} else {
		b.currIndex = sort.Search(len(b.values), func(i int) bool {
			return bytes.Compare(b.values[i].key, key) >= 0
		})
	}
}

func (b *btreeIterator) Next() {
	b.currIndex++
}

// Valid 判断当前指针是否超过了数组长度
func (b *btreeIterator) Valid() bool {
	return b.currIndex < len(b.values)
}

func (b *btreeIterator) Key() []byte {
	return b.values[b.currIndex].key
}

func (b *btreeIterator) Value() *data.LogRecordPos {
	return b.values[b.currIndex].pos
}

func (b *btreeIterator) Close() {
	b.values = nil
}
