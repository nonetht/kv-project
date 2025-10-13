package index

import (
	"sync"

	"github.com/google/btree"
)

// BTree 索引，主要是封装了 google 的 btree kv
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}
