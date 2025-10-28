package bitcask_go

import (
	"bitcask-go/data"
	"errors"
	"io"
	"sync"
)

// DB 存储面向用户的操作接口
type DB struct {
	setup        Setup
	mu           *sync.Mutex
	activeFile   *data.DataFile            // 当前活跃文件
	inactiveFile map[uint32]*data.DataFile // 不活跃数据文件，也就是不活跃的数据文件。
}

// Put 用户写入的方法
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

	// 我还要检测 logRecordPos 是否有效
	if logRecordPos == nil {
		return errors.New("logRecord is nil")
	}

	// 将 logRecordPos添加到内存之中，但是具体添加到哪里呢？
	// 既然是添加到内存之中，就应该在 db 结构体中新增一个字段，用 map 来存储

}

// 返回写入数据的索引信息，随后内存会存放该索引信息数据
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
	encodedRecord, size := data.EncodeLogRecord(logRecord)
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

func (db *DB) appendLogRecordPos(logRecordPos *data.LogRecordPos) (*data.LogRecordPos, error) {

}
