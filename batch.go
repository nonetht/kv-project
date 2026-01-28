package bitcask_go

import (
	"bitcask-go/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

var txnFinKey = []byte("txn-fin")

const nonTransactionSeqNo uint64 = 0

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
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 批量写数据，不会写磁盘也不会更新内存，只是构建 LogRecord，随后暂存到 map 之中
func (w *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 加锁保证线程安全
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

// Delete 删除对应的数据，但是我的逻辑和作者写的稍有不同，应该按照 @roseduan 的为主
func (w *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// TODO: deal with corner cases；这是 @roseduan 的实现，但是我认为有一种情况没有考虑到。
	if pos := w.db.index.Get(key); pos == nil {
		if w.pendingWrites[string(key)] != nil {
			delete(w.pendingWrites, string(key))
		}
		return nil
	}

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

	w.db.mu.Lock()
	defer w.db.mu.Unlock()

	// 获取当前最新的事务序列号
	seqNumber := atomic.AddUint64(&w.db.seqNumber, 1)

	positions := make(map[string]*data.LogRecordPos)
	// 遍历 pendingWrite，将其中的 logRecord 全部写入
	for _, logRecord := range w.pendingWrites {
		pos, err := w.db.appendLogRecord(&data.LogRecord{
			Key:   addSeqToKey(logRecord.Key, seqNumber),
			Value: logRecord.Value,
			Type:  logRecord.Type,
		})
		if err != nil {
			return err
		}
		positions[string(logRecord.Key)] = pos
	}

	// 写一条标识事务完成的数据
	finishedRecord := &data.LogRecord{
		Key:  addSeqToKey(txnFinKey, seqNumber),
		Type: data.LogRecordTxnFinished,
	}

	// 然后将 “事务完成” 的logRecord 添加进去。
	if _, err := w.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	// 根据配置决定是否持久化
	if w.setup.SyncWrites && w.db.activeFile != nil {
		if err := w.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新索引：遍历 positions，将 pos 全部写入到索引
	for _, record := range w.pendingWrites {
		pos := positions[string(record.Key)]
		if record.Type == data.LogRecordNormal {
			w.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			w.db.index.Delete(record.Key)
		}
	}

	// pendingWrite 清空
	w.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

// 我的想法是将 seqNumber 添加到 key里面，就是key+seq编码
func addSeqToKey(key []byte, seqNumber uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64) // 创建变长数组
	n := binary.PutUvarint(seq[:], seqNumber)  // 我们将其中 seqNumber 放入到数组 seq 之中；n 应该是返回的长度

	// 创建一个 切片，长度为 n + len(key)，因为一方面 len(key) -> 要存储 key 的长度；另一方面，是 n -> 存储 seqNumber 的内容
	encKey := make([]byte, n+len(key))
	// 前 n 对应 seqNumber
	copy(encKey[:n], seq[:n])
	// 剩余的，则是对应的 key
	copy(encKey[n:], key)

	return encKey
}

// 解析 LogRecord 之中的 key，获取实际 key 和事务序列号。
func parseLogRecordKey(key []byte) (realKey []byte, seqNumber uint64) {
	seqNumber, n := binary.Uvarint(key) // 其中变量 n 为新的变量，因此短声明可以使用。
	realKey = key[n:]                   // 前 n 部分的是 seqNumber 部分；n 之后的就是 key
	return
}
