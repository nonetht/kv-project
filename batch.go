package bitcask_go

import (
	"bitcask-go/data"
	"sync"
)

type WriteBatch struct {
	mu            *sync.Mutex
	db            *DB
	setup         WriteBatchSetup
	pendingWrites map[string]*data.LogRecord // 暂存用户写入的数据
}

// NewWriteBatch 创建一个新的 WriteBatch，用于根据设置，暂存用户写入数据
func (db *DB) NewWriteBatch(setup WriteBatchSetup) *WriteBatch {
	return &WriteBatch{
		mu:    new(sync.Mutex),
		db:    db,
		setup: setup,
		// 感觉写的有问题，我这样是对的吗？
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 批量写数据，不会写磁盘也不会更新内存，只是构建 LogRecord，随后暂存到 map 之中
func (w *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	w.pendingWrites[string(key)] = logRecord
	return nil
}

// Delete 删除对应的数据，并且考虑两个case：1. 数据不存在；2. 数据存在于 pendingWrites 之中
func (w *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// 对应case2，待删除数据在 pendingWrites 之中
	if w.pendingWrites[string(key)] != nil {
		delete(w.pendingWrites, string(key)) // 从 pendingWrites 之中，将 string(key) 删除掉
	}

	// 对应case1，待删除数据不存在；但是不能放在 case2 前面，万一其中的待删除没有在索引之中找到，但是可以在 pendingWrites 中找到呢？
	if pos := w.db.index.Get(key); pos == nil {
		return nil
	}

	// 应该是将待删除的写入到 pendingWrites 之中
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}

	w.pendingWrites[string(key)] = logRecord
	return nil
}
