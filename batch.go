package bitcask_go

import (
	"bitcask-go/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
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
		mu:            new(sync.Mutex),
		db:            db,
		setup:         setup,
		pendingWrites: make(map[string]*data.LogRecord), // roseduan 明显写的有问题，你写一个类型，编译器都过不去！
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

// Commit 提交事务，将暂存的数据写到数据文件，并更新内存索引
func (w *WriteBatch) Commit() error {
	// 加锁保证事务提交串行化
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.pendingWrites) == 0 {
		return nil
	}
	if uint(len(w.pendingWrites)) > w.setup.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	// 加锁保证事务提交串行化
	//w.mu.Lock()
	//defer w.mu.Unlock()

	// 获取当前最新的事务序列号
	seqNumber := atomic.AddUint64(&w.db.seqNumber, 1)

	// 开始向其中写入数据。
	for _, logRecord := range w.pendingWrites {
		w.db.appendLogRecord(&data.LogRecord{
			Key:   addSeqToKey(logRecord.Key, seqNumber),
			Value: logRecord, Value,
			Type: logRecord.Type,
		})
	}
}

// 我的想法是将 seqNumber 添加到 key里面，就是key+seq编码
func addSeqToKey(key []byte, seqNumber uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64) // 创建变长数组
	n := binary.PutUvarint(seq[:], seqNumber)  // 我们将其中 seqNumber 放入到数组 seq 之中；n 应该是返回的长度

	// 创建一个 切片，长度为 n + len(key)，因为一方面 len(key) -> 要存储 key 的长度；另一方面，是n -> 存储 seqNumber 的内容
	encKey := make([]byte, n+len(key))
	// 前 n 对应 seqNumber
	copy(encKey[:n], seq[:n])
	// 剩余的，则是对应的 key
	copy(encKey[n:], key)

	return encKey
}
