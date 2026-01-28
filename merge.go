package bitcask_go

import (
	"bitcask-go/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const mergeDirName = "-merge"

// Merge 清理无效数据，并生成 Hint 文件
func (db *DB) Merge() error {
	// 数据库为空，直接返回
	if db.activeFile == nil {
		return nil
	}

	// 加锁保证线程安全。
	db.mu.Lock()

	// 如果 merge 正在进行，返回错误
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsProgress
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 关闭当前活跃文件，打开一个新的活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}

	db.inactiveFile[db.activeFile.FileId] = db.activeFile
	if err := db.initActiveFile(); err != nil {
		db.mu.Unlock()
		return err
	}

	nonMergeFileId := db.activeFile.FileId

	// 取出所有需要 merge 的文件，并存放到 mergeFiles 切片之中
	var mergeFiles []*data.DataFile
	for _, file := range db.inactiveFile {
		mergeFiles = append(mergeFiles, file)
	}

	// TODO：为什么是这里执行解锁呢？
	db.mu.Unlock()

	// 等待 merge 的文件根据 FileId 从小到大排序，依次 merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergePath := db.getMergePath()
	// 如果之前存在目录，说明发生过 merge，将其删除掉。
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	// 新建一个 merge path 目录，
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	// 在该目录下，传入配置项，打开一个临时 bitcask 实例
	mergeSetup := Setup{
		DirPath:    mergePath,
		SyncWrites: false,
	}

	mergeDB, err := Open(mergeSetup)
	if err != nil {
		return err
	}

	// 打开 hint 文件存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	// 遍历处理每个数据文件，然后获取其中存储的 logRecord，比较 logRecordPos 以及
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 就因为改动了 Put 方法，导致后续都要执行 parseLogRecord 方法
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)
			// 通过两个字段：Fid，offset 来同内存中的索引位置进行比较，如果有效则重写。
			if logRecordPos != nil && logRecordPos.Fid == dataFile.FileId && logRecordPos.Offset == offset {
				// 清除事务标记，因为这些都是有效的数据
				logRecord.Key = addSeqToKey(realKey, nonTransactionSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}

				// 将位置索引写入到 Hint 文件之中
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			offset += size
		}
	}

	// 对当前文件（新的实例mergeDB以及hintFile）持久化，保证写入到磁盘之中。
	if err := mergeDB.Sync(); err != nil {
		return err
	}
	if err := hintFile.Sync(); err != nil {
		return err
	}

	// 新增一个标识 merge 完成的文件 -- mergeFinFile
	mergeFinFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}

	// ⚠️其中的 Value 部分，实际上是 nonMergeFileId，本质上是最新的 FileId
	mergeFinRec := &data.LogRecord{
		Key:   []byte("merge already finished"),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}

	// 将 mergeFinRec 进行编码，编入后续写入到文件 -- mergeFinFile
	encRecord, _ := data.EncodeLogRecord(mergeFinRec)
	if err := mergeFinFile.Write(encRecord); err != nil {
		return err
	}

	// 写入之后，刷写到磁盘上，保证持久性
	if err := mergeFinFile.Sync(); err != nil {
		return err
	}

	return nil
}

// 比如说我们当前目录是：/tmp/bitcask
// 那么我们在其父目录之上，增加一个新的目录：/tmp/bitcask-merge
func (db *DB) getMergePath() string {
	// Dir 表示拿到父目录
	// Clean 表示清楚末尾的斜杠
	Dir := path.Dir(path.Clean(db.setup.DirPath))
	// 拿到文件夹的名称
	base := path.Base(db.setup.DirPath)

	return path.Join(Dir, base+mergeDirName)
}

func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	// mergePath 路径不存在的话，直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}

	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 寻找查看 merge 是否完成的文件
	var isMergeFinished bool
	var mergeFileNames []string
	for _, dirEntry := range dirEntries {
		if dirEntry.Name() == mergeDirName {
			isMergeFinished = true
		}
		mergeFileNames = append(mergeFileNames, dirEntry.Name())
	}

	// 没有 merge 完成的表示文件，则直接返回
	if !isMergeFinished {
		return nil
	}

	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	// 删除旧数据文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.setup.DirPath, fileId)
		if _, err := os.Stat(fileName); os.IsNotExist(err) {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// 将新的数据文件移动到目录中；其实移动方式，就是将文件夹名称进行了更改罢了。
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.setup.DirPath, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}

	rec, _, err := mergeFinFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}

	nonMergeFileId, err := strconv.Atoi(string(rec.Value))
	if err != nil {
		return 0, err
	}

	return uint32(nonMergeFileId), nil
}

// 从 Hint 文件中加载索引
func (db *DB) loadIndexFromHintFile() error {
	hintFileName := filepath.Join(db.setup.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	// 打开 hint 索引文件
	hintFile, err := data.OpenHintFile(hintFileName)
	if err != nil {
		return err
	}

	// 读取文件中的索引
	var offset int64 = 0
	for {
		rec, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// 解码拿到实际位置索引信息
		pos := data.DecodeLogRecordPos(rec.Value)
		db.index.Put(rec.Key, pos)
		offset += size
	}
	return nil
}
