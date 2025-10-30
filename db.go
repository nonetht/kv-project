package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"errors"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB 存储面向用户的操作接口
type DB struct {
	setup        Setup
	mu           *sync.RWMutex
	activeFile   *data.DataFile            // 当前活跃文件
	inactiveFile map[uint32]*data.DataFile // 不活跃数据文件，也就是不活跃的数据文件。
	index        index.Indexer             // 内存索引
}

// Open 打开 bitcask 存储引擎的方法
func Open(setup Setup) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(setup); err != nil {
		return nil, err
	}

	// 随后应该根据 setup 中的 DirPath 字段，打开对应的部分。
	// 但是在这之前，还应校验，如果目录不存在，则创建一个新的目录
	if _, err := os.Stat(setup.DirPath); os.IsNotExist(err) {
		if err := os.Mkdir(setup.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

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

	// 读取数据时，还应该加锁
	db.mu.Lock()
	defer db.mu.Unlock()

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
	// 根据偏移量读取对应的数据
	logRecord, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	// 针对 LogRecord 还应进行一个类型判断
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
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
	// 是的，我也不是很理解...
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

// 从磁盘中加载数据文件
func (db *DB) loadDataFile() error {
	dirEntries, err := os.ReadDir(db.setup.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// 遍历当前目录所有文件，并找到以 .data 后缀结尾的文件
	for _, entry := range dirEntries {
		// 如果是以 .data 结尾的话，需要对文件名进行分割
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			// e.g. 当前文件为 001.data 文件，我们需要根据 "." 来分割，获取文件名 001 作为文件id
			// 最后 Split函数之后返回的是 ["001", "data"]
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0]) // 是不是将 string -> int 类型？
			if err != nil {
				return ErrDataDirectoryCorrupted
			}

			// 将文件 id 添加到我们的 fileId数组之中
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件 id 进行排序，从小到大进行依次加载
	sort.Ints(fileIds)

	// 遍历每个文件id，并打开对应的数据文件
	// 如果有相同名称，但是不同后缀的文件，该怎么办呢？
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.setup.DirPath, uint32(fid))
		if err != nil {
			return err
		}

		// 遍历到最后的文件，也就是我们的活跃文件
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			// 否则，添加到旧的数据文件之中
			db.inactiveFile[uint32(fid)] = dataFile
		}
	}

	return nil
}

// 对用户传入的配置项进行校验
func checkOptions(setup Setup) error {
	// 传入目录为空，直接返回一个错误
	if setup.DirPath == "" {
		return errors.New("dirPath is empty")
	}

	if setup.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	return nil
}
