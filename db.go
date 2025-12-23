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

// DB 存储面向用户的操作接口
type DB struct {
	setup        Setup
	mu           *sync.RWMutex
	fileIds      []int
	activeFile   *data.DataFile            // 当前活跃文件
	inactiveFile map[uint32]*data.DataFile // 不活跃数据文件，也就是不活跃的数据文件。
	index        index.Indexer             // 内存索引
	seqNumber    uint64                    // 事务序列号，全局递增
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

	// 随后开始准备构建索引
	/*
		务必从小到大来遍历文件的 id，根据 id 找到对应数据文件，可以写一个 for循环。
		并且定义 offset 变量，表示读取到当前文件哪个位置。直接调用 ReadLogRecord 方法，拿到 LogRecord 即可
		随后根据当前文件遍历的 id，以及offset，构建出内存索引信息 LogRecordPos，并将其存储到内存索引之中
	*/

	// 1. 从数据文件中，加载索引
	if err := db.loadIndexFromDatafile(); err != nil {
		return nil, err
	}
	return db, nil
}

// Put 用户写入 Key/Value 数据到活跃文件之中，key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	logRecord := data.LogRecord{
		Key:   addSeqToKey(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 添加 LogRecord 之后，会返回 logRecordPos 的地址和 err
	logRecordPos, err := db.appendLogRecordWithLock(&logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if ok := db.index.Put(key, logRecordPos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) Delete(key []byte) error {
	// 判断key 有效性
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 去内存索引查找一下，看key是否存在，如果不存在的话，直接返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 随后构造对应logRecord信息，然后写入到数据文件之中。
	logRecord := &data.LogRecord{
		Key:  addSeqToKey(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}

	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 随后将其从内存索引之中删除
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// Get 通过 key 然后在内存索引之中找到 pos，随后根据 pos 中字段找到对应的 dataFile，然后根据偏移量读取数据取得 LogRecord。
// 最后 LogRecord 之中的 Value 字段就是 Value 的值
func (db *DB) Get(key []byte) ([]byte, error) {
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

	return db.getValueFromPos(pos)
}

func (db *DB) getValueFromPos(pos *data.LogRecordPos) ([]byte, error) {
	// 问题是拿到了 pos 之后，我们应该怎样获取对应的数据文件呢？
	// pos 的确有 fileId 字段，但是如何使用呢？怎样根据文件 id 找到数据文件呢？
	var dataFile *data.DataFile
	if db.activeFile.FileId == pos.Fid {
		// 从 activeFile 之中寻找
		dataFile = db.activeFile
		// 反之，从 inactiveFile 这个map之中寻找
	} else if db.inactiveFile[pos.Fid] != nil {
		dataFile = db.inactiveFile[pos.Fid]
	} else {
		// 没有找到，则报错
		return nil, ErrDataFileNotFound
	}

	// 现在我们获取了对应的 dataFile 之后呢？
	// 根据偏移量读取对应的数据
	logRecord, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	// 针对 LogRecord 还应进行一个类型判断
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

// ListKeys 获取数据库之中所有的 key
func (db *DB) ListKeys() [][]byte {
	//iter := db.NewIterator(DefaultIteratorSetup)
	//
	//var keys [][]byte
	//for iter.Rewind(); iter.Valid(); iter.Next() {
	//	keys = append(keys, iter.Key())
	//}
	//
	//return keys, nil

	iter := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())

	var idx int
	for iter.Rewind(); iter.Valid(); iter.Next() {
		keys[idx] = iter.Key()
	}
	return keys
}

// Fold 获取所有的数据，执行用户指定操作。函数返回 false 时终止遍历
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iter := db.index.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		val, err := db.getValueFromPos(iter.Value())
		if err != nil {
			return err
		}
		if !fn(iter.Key(), val) {
			break
		}
	}
	return nil
}

// Close 关闭数据库，清理并释放相关资源
func (db *DB) Close() error {
	if db == nil {
		return nil
	}

	// 还是要有加锁、解锁的内容
	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// 不活跃文件如何关闭呢？inactiveFile 的类型是 map[uint32]*data.DataFile。即 uint32 - *data.DataFile
	// 应该就是遍历 inactiveFile 这个映射，然后选择 value 部分（*data.DataFile 类型），然后执行 Close() 函数
	// TODO: 对于val对应类型，也就是 *data.DataFile，其本质为地址，为什么可以执行 Close 函数？
	for _, file := range db.inactiveFile {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Sync() error {
	// 避免竞态条件，加锁解锁
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// 只对 activeFile 进行操作
	if err := db.activeFile.Sync(); err != nil {
		return err
	}
	return nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// 追加写数据到活跃文件中，为了避免竞态条件，应该加锁。
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// 判断当前活跃文件是否存在，因为数据库没有写入的时候是没有文件生成
	// 如果为空，则初始化活跃文件
	if db.activeFile == nil {
		if err := db.initActiveFile(); err != nil {
			return nil, err
		}
	}

	// 首先将 LogRecord 进行编码为字节数组类型，
	// TODO: 为什么要编码为直接数组类型？
	encodedRecord, size := data.EncodeLogRecord(logRecord)
	// **判断**，超出预值的话：
	// 1. 将现有的数据文件转换为旧的数据文件，即 activeFile -> inactiveFile
	// 2. 打开一个新的数据文件，
	if db.activeFile.WriteOff+size > db.setup.DataFileSize {
		// 将当前活跃文件持久化，持久化到磁盘之中
		// TODO: 比较好奇，所谓的持久化是个什么样子。
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

// 初始化当前活跃文件，在访问此方法前务必持有**互斥锁**
// TODO: 我个人理解，是创建一个新的文件，但是为什么是读取某个文件路径呢？由于 OpenDataFile 没有实现，我们无从得知
func (db *DB) initActiveFile() error {
	// 初始数据字段
	var initialFileId uint32 = 0
	// 不为空，则在递增 + 1
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

// 从磁盘中加载数据文件，就是填充 db 结构体 中的 activeFiles, inactiveFiles。
func (db *DB) loadDataFile() error {
	// 得到的有文件夹和文件，就是只选择带有 .data 后缀的文件。
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
			fileId, err := strconv.Atoi(splitNames[0]) // 是不是将 string -> int 类型？是的
			if err != nil {
				return ErrDataDirectoryCorrupted
			}

			// 将文件 id 添加到我们的 fileId 数组之中
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件 id 进行排序，从小到大进行依次加载
	sort.Ints(fileIds)
	db.fileIds = fileIds // 将数组传递给 db 结构体之中的 fileIds 字段

	// 遍历每个文件id，并打开对应的数据文件。
	//TODO: 但是如果存在这么一种情况的话呢？就是一个文件为001.data,还有一个叫001.txt。就是相同名称，但是不同后缀的文件。
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

// TODO: kv存储项目代码真是一坨，我看着都难受...感觉实在是太臃肿了！
func (db *DB) loadIndexFromDatafile() error {
	// dataFile -> logRecord -> logRecordPos -> Put 到 Indexer 之中
	if len(db.fileIds) == 0 {
		return nil
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool // 定义 ok 变量，对结果进行检测
		// 如果为删除类型的话，则执行删除操作。即从 db.index 将其删除
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("failed to update index at startup")
		}
	}

	transactionRecords := make(map[uint64]*data.TransactionRecord)

	// 通过 db 中的 activeFile，inactiveFile获取的对应的 dataFile，而我们则是使用的 OpenDataFile
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.inactiveFile[fileId]
		}

		var offset int64 = 0
		for {
			// 考虑到 offset，我们还要获取 logRecord的大小
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			// 不能正常返回，但是如果到最后一个文件就是正常情况，其他情况则进行返回
			if err != nil {
				// 读取到了最后一个文件，正常情况
				if err == io.EOF {
					break
				}
				return err
			}

			// 先创建一个 logRecordPos
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset, // 起始值就是 0
			}

			// TODO: 为什么要解析 key 拿到事务序列号？
			key, seq := parseLogRecordKey(logRecord.Key)

			if seq == nonTransactionSeqNo {
				// 非事务操作，直接更新内存索引
				updateIndex(key, data.LogRecordDeleted, logRecordPos)
			} else {
				// 事务完成，对应的 seq 的数据可以更新到内存索引中
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seq)
				} else {
					logRecord.Key = realKey
					transactionRecords[seq] = append(transactionRecords[seq], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// 对 offset 进行递增操作
			offset += size
		}

		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
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

	// 数据文件大小如果 <= 0，也会报错
	if setup.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	return nil
}
