package bitcask_go

import (
	"bitcask-go/data"
	"io"
	"os"
	"path"
	"sort"
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
	// e.g.比如说当前我们有三个文件：file0, file1, file2。此时我们应该关闭当前活跃文件 file2，随后打开一个新的文件 file3.
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}

	// 将当前活跃文件转换为旧的文件
	db.inactiveFile[db.activeFile.FileId] = db.activeFile
	// 打开新的活跃文件
	if err := db.initActiveFile(); err != nil {
		db.mu.Unlock()
		return err
	}

	// 取出所有需要 merge 的文件
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
	// TODO: 我就很奇怪，为什么不能是在 merge 结束的时候将其删除掉呢？
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

	// 遍历处理每个数据文件
	// 说实在的，写得我感觉恶心，太臃肿了这部分。
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
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)
			// 和内存中的索引位置进行比较，如果有效则重写。比较两个：Fid, offset
			if logRecordPos != nil && logRecordPos.Fid == dataFile.FileId && logRecordPos.Offset == offset {
				// 清除事务标记
				logRecord.Key = addSeqToKey(realKey, nonTransactionSeqNo) //
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

	// 新增一个标识 merge 完成的文件。
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
