package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"sync"
)

// DB 存储面向用户的操作接口
type DB struct {
	setup        Setup
	mu           *sync.Mutex
	activeFile   *data.DataFile            // 当前活跃文件
	inactiveFile map[uint32]*data.DataFile // 不活跃数据文件，也就是不活跃的数据文件。
	index        index.Indexer             // 内存索引
}

// Put 用户写入 Key/Value 数据，key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	logRecord := data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 添加 LogRecord 之后，会返回 logRecordPos 的地址和 err
	logRecordPos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.index.Put(key, logRecordPos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	// 1. 根据key去内存索引中查找数据，如果没有找到，说明对应Key不存在，报错
	// 2. 找到了，取出对应位置信息
	// 3. 根据文件 id，去找到对应数据文件
	// 3.1 如果是活跃文件，直接使用活跃文件
	// 3.2 否则，从旧的数据文件中寻找
	// 3.3 没有找到，报错

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 如果没有找到，即 pos == nil，说明 key 不存在
	pos := db.index.Get(key)
	if pos == nil {
		return nil, ErrKeyNotFound
	}

	// 问题是拿到了 pos 之后，我们应该怎样获取对应的数据文件呢？
	// pos 的确有 fileId 字段，但是如何使用呢？怎样根据文件 id 找到数据文件呢？
	var dataFile *data.DataFile
	if db.activeFile.FileId == pos.Fid {
		// 从 activeFile 之中寻找
		dataFile = db.activeFile
	} else if db.inactiveFile[pos.Fid] != nil {
		// 反之，从 inactiveFile 这个map之中寻找
		dataFile = db.inactiveFile[pos.Fid]
	} else {
		// 没有找到，则报错
		return nil, ErrDataFileNotFound
	}

	// 现在我们获取了对应的 dataFile 之后呢？

}

// 追加写数据到活跃文件中
func (db *DB) appendLogRecord(logRecord data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断当前活跃文件是否存在，因为数据库没有写入的时候是没有文件生成
	// 如果为空，则初始化活跃文件
	if db.activeFile == nil {
		if err := db.initActiveFile(); err != nil {
			return nil, err
		}
	}

	// 首先将 LogRecord 进行编码为字节数组类型
	encodedRecord, size := data.EncodeLogRecord(&logRecord)
	// **判断**，超出预值的话：
	// 1. 将现有的数据文件转换为旧的数据文件，即 activeFile -> inactiveFile
	// 2. 打开一个新的数据文件，
	if db.activeFile.WriteOff+size > db.setup.DataFileSize {
		// 将当前活跃文件持久化，持久化到磁盘之中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 持久化后，将当前活跃文件转换为旧数据文件中
		db.inactiveFile[db.activeFile.FileId] = db.activeFile

		// 随后打开新的数据文件
		if err := db.initActiveFile(); err != nil {
			return nil, err
		}
	}

	// 如果 logRecord 的 Type 是待删除类型呢？
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encodedRecord); err != nil {
		return nil, err
	}

	// 判断一下，就是是否需要对数据进行一次安全的持久化。简单来说，相当于用户所拥有的一个可选项目
	if db.setup.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// 随后构造一个内存索引信息，并返回
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
	}
	return pos, nil

}

// 初始化当前活跃文件
// 在访问此方法前务必持有**互斥锁**
func (db *DB) initActiveFile() error {
	// 初始数据字段
	var initialFileId uint32 = 0
	// 不为空，则递增 + 1
	// 我还是不太理解，就是之前不是判断了吗，这里的活跃数据文件不一定是空吗？
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}

	// 打开新的数据文件
	// 目录传递是通过用户传递某个配置项，随后进行传递，因此需要一个类似"配置项"的结构体
	dataFile, err := data.OpenDataFile(db.setup.DirPath, initialFileId)
	if err != nil {
		return err
	}

	db.activeFile = dataFile
	return nil
}

//func (db *DB) appendLogRecordPos(logRecordPos *data.LogRecordPos) (*data.LogRecordPos, error) {
//
//}
