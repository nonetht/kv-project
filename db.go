package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"sync"
)

// DB bitcask 存储引擎实例
// 为什么活跃文件和不活跃文件的差异如此之大呢？
type DB struct {
	setup        SetUp                     // 数据库配置
	mu           *sync.RWMutex             // 读写锁
	activeFile   *data.DataFile            // 当前活跃数据文件
	inactiveFile map[uint32]*data.DataFile // 不活跃数据文件，也就是不活跃的数据文件。
	index        index.Indexer             // 索引信息
}

func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造LogRecord结构体
	// 构建函数体的话，不是map形式为什么写成键值对这种呢？-> 估计是为了更直观吧，让结构体字段部分更加直观。
	log_record := data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入到当前活跃文件中
	pos, err := db.appendLogRecord(&log_record)
	if err != nil {
		return err
	}

	// 拿到索引信息之后，需要更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// 将一条logRecord添加到...随后返回索引的地址信息
// 应该就是将LogRecord这条数据添加进去，随后在记录信息后，还要返回一个索引信息，便于日后查找对应信息
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断当前活跃数据文件是否存在，如果数据没有写入的话，就没有文件生成
	// 如果为空，则初始化数据文件
	if db.activeFile == nil {
		if err := db.activeFileInit(); err != nil {
			return nil, err
		}
	}

	// 将logRecord进行编码，传入的是结构体，但是写入的话应该写入[]byte(字节数组)
	encodedLogRecord, size := data.EncodeLogRecord(logRecord)
	// 如果写入的数据 + 活跃文件的大小 > 数据活跃文件写入的预值
	// 对数据文件状态进行转换：将当前新的数据文件，转换为旧的数据文件，然后打开一个新的数据文件
	if db.activeFile.WriteOff+size > db.setup.DataSize {
		// 当前文件持久化到磁盘
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 持久化后，将当前活跃文件转换为旧数据文件
		// 先将其放入到旧的数据文件当中，也就是放入到map中
		db.inactiveFile[db.activeFile.FileId] = db.activeFile

		// 打开新的数据文件
		if err := db.activeFileInit(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff // ?这是做什么的
	if err := db.activeFile.Write(encodedLogRecord); err != nil {
		return nil, err
	}

	// 根据用户配置决定是否持久化
	if db.setup.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// 构造内存索引信息
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil
}

// 活跃文件的初始化
// 感觉更像是当前活跃文件初始化...
// 在访问此方法前必须持有互斥锁
func (db *DB) activeFileInit() error {
	var initialFileId uint32 = 0
	// 保证每个文件Id都是递增的
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.setup.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}
