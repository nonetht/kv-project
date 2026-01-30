package index

import (
	"bitcask-go/data"
	"bytes"
	"sort"
	"sync"

	goart "github.com/plar/go-adaptive-radix-tree"
)

// AdaptiveRadixTree 自适应基数树
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

// NewART 初始化自适应基数树
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: &sync.RWMutex{},
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
	art.lock.Lock()
	art.tree.Insert(key, pos)
	art.lock.Unlock()
	return true
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos) // 强制转换为我们的类型
}

func (art *AdaptiveRadixTree) Delete(key []byte) bool {
	art.lock.Lock()
	defer art.lock.Unlock()
	_, deleted := art.tree.Delete(key)
	return deleted
}

func (art AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	defer art.lock.RUnlock()
	size := art.tree.Size()
	return size
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	//
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newARTIterator(art.tree, reverse)
}

// ART 的索引迭代器
type artIterator struct {
	currIndex int
	reverse   bool
	values    []*Item
}

// 和之前的 bTreeIterator 类似，都要先将全部的键值对存储到 values 之中
func newARTIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}

	values := make([]*Item, tree.Size())
	saveValue := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		// 将 item 存储到数组 values 之中
		values[idx] = item
		// 根据 reverse 来递增/递减 其中的 idx
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	// ForEach 遍历全部节点，并对每一个节点调用 savaValues 函数
	tree.ForEach(saveValue)

	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (art *artIterator) Rewind() {
	art.currIndex = 0
}

func (art *artIterator) Seek(key []byte) {
	if art.reverse {
		art.currIndex = sort.Search(len(art.values), func(i int) bool {
			return bytes.Compare(art.values[i].key, key) <= 0
		})
	} else {
		art.currIndex = sort.Search(len(art.values), func(i int) bool {
			return bytes.Compare(art.values[i].key, key) >= 0
		})
	}
}

func (art *artIterator) Next() {
	art.currIndex++
}

func (art *artIterator) Valid() bool {
	return art.currIndex >= 0 && art.currIndex < len(art.values)
}

func (art *artIterator) Key() []byte {
	return art.values[art.currIndex].key
}

func (art *artIterator) Value() *data.LogRecordPos {
	return art.values[art.currIndex].pos
}

func (art *artIterator) Close() {
	art.values = nil
}
