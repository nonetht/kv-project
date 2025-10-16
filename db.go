package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask 存储引擎实例
// 为什么活跃文件和不活跃文件的差异如此之大呢？
type DB struct {
	setup        SetUp                     // 数据库配置
	mu           *sync.RWMutex             // 读写锁
	fileIds      []int                     // 文件 id，只能在加载索引的时候使用，不能在其他地方更新和使用
	activeFile   *data.DataFile            // 当前活跃数据文件
	inactiveFile map[uint32]*data.DataFile // 不活跃数据文件，也就是不活跃的数据文件。
	index        index.Indexer             // 索引信息
}

// Open 打开 bitcask 存储引擎实例
func Open(setup SetUp) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(setup); err != nil {
		return nil, err
	}

	// 判断数据目录是否存在，如果不存在，则创建这个目录
	if _, err := os.Stat(setup.DirPath); os.IsNotExist(err) {
		if err := os.Mkdir(setup.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化 DB 实例结构体
	db := &DB{
		setup:        setup,
		mu:           new(sync.RWMutex),
		activeFile:   nil,
		inactiveFile: make(map[uint32]*data.DataFile),
		index:        index.NewIndexer(setup.IndexType),
	}

	// 加载数据文件
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	// 从数据文件中加载索引
	if err := db.loadIndexFromDataFile(); err != nil {
		return nil, err
	}

	return db, nil
}

// Put 写入 Key/Value数据，同时Key不为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造LogRecord结构体
	// 构建函数体的话，不是map形式为什么写成键值对这种呢？-> 估计是为了更直观吧，让结构体字段部分更加直观。
	logRecord := data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入到当前活跃文件中
	pos, err := db.appendLogRecord(&logRecord)
	if err != nil {
		return err
	}

	// 拿到索引信息之后，需要更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// Get 读取LogRecord，即存储的数据文件
// 但是，在拿到LogRecord首先要获取对应的索引，即LogRecordPos，随后有了索引才可以拿到对应的数据文件，最后通过偏移量读取数据文件中我们所需要的数据
func (db *DB) Get(key []byte) ([]byte, error) {

	// 需要加锁
	db.mu.RLock()
	defer db.mu.RUnlock()

	// 判断Key是否有效
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存数据结构题中取出 key 对应的索引信息
	logRecordPos := db.index.Get(key)
	// 没有找到，说明 key 不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// 如果有对应位置信息，根据文件 id 找到对应数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.inactiveFile[logRecordPos.Fid]
	}

	// 数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotExist
	}

	// 根据数据偏移量来读取数据
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// ??
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
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

// 从磁盘中加载数据文件
func (db *DB) loadDataFile() error {
	dirEntries, err := os.ReadDir(db.setup.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	// 遍历目录之中所有的文件，以.data文件为后缀便是我们的目标文件
	for _, entry := range dirEntries {
		// Name() 函数是什么？
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			// 000001.data -> 000001
			splitName := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitName[0])
			// 文件目录可能损坏
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			// 为什么不写成 fileIdes.append(fieId) 呢？
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件 id 进行排序，选择升序排序
	sort.Ints(fileIds)
	// 排序后，进行赋值操作
	db.fileIds = fileIds

	// 遍历每个文件id，并打开对应的数据文件
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.setup.DirPath, uint32(fid))
		if err != nil {
			return err
		}

		// 我不理解为什么要做这么一步...
		// 遍历到最后的文件，即最新的文件。这便是当前活跃文件，其他的加入到旧数据文件之中
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.inactiveFile[uint32(fid)] = dataFile
		}
	}

	return nil
}

// 从数据文件加载索引
// 遍历文件中所有记录，并更新到内存索引之中
func (db *DB) loadIndexFromDataFile() error {
	// 如果拿到的是一个空的数据库的话
	if len(db.fileIds) == 0 {
		return nil
	}

	// 遍历所有文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile

		// 为活跃文件，则从活跃文件中寻找
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
			// 反之，则从旧文件之中查找
		} else {
			dataFile = db.inactiveFile[fileId]
		}

		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			// 正常情况下读到最后一个文件，随即正常返回
			if err != nil && err == io.EOF {
				break
			} else if err != io.EOF {
				return err
			}
			// 构建内存索引，并保存
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}
			if logRecord.Type == data.LogRecordDeleted {
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, logRecordPos)
			}

			// 递增offset，下一次从新的位置读取
			offset += size
		}

		// 如果是当前活跃文件， 更新文件WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	return nil
}

func checkOptions(setup SetUp) error {
	if setup.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	// 数据大小预值无效
	if setup.DataSize <= 0 {
		return errors.New("database data size must be positive")
	}
	return nil
}
