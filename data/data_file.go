package data

import "bitcask-go/fio"

const DataFileNameSuffix = ".data"

// DataFile 使用结构体来定义数据文件
type DataFile struct {
	FileId   uint32 // 当前文件id
	WriteOff int64  // 当前文件写入到了哪个位置
	// 之前定义的文件操作的抽象接口，我们需要调用该接口实现数据读写的操作
	IoManager fio.IoManager
}

// OpenDataFile 打开数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

// ReadLogRecord 读取对应的 LogRecord 为什么还要需要 offset？
// 除了返回一条 LogRecord，还要返回 LogRecord 大小
// TODO: 说实话，我感觉我什么不能单独创建一个函数来求取 LogRecord 的大小呢？
// 确实，其实我也考虑过，就是用单独的函数来做，是不是更好呢？
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}
