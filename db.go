package bitcask_go

import (
	"bitcask-go/data"
	"sync"
)

// DB bitcask 存储引擎实例
// 为什么活跃文件和不活跃文件的差异如此之大呢？
type DB struct {
	setup        SetUp                     // 数据库配置
	mu           *sync.RWMutex             // 读写锁
	activeFile   *data.DataFile            // 当前活跃数据文件
	inactiveFile map[uint32]*data.DataFile // 不活跃数据文件，也就是不活跃的数据文件。
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

}

// 将一条logRecord添加到...随后返回索引的地址信息
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
