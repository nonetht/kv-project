package data

import "bitcask-go/fio"

type DataFile struct {
	FileId   uint32 // 当前文件id
	WriteOff int64  // 当前文件写入到了哪个位置
	// 之前定义的文件操作的抽象接口，我们需要调用该接口实现数据读写的操作
	IoManager fio.IoManager
}

// OpenDataFile 打开数据文件，数据库其中时的方法
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, error) {
	return nil, nil
}
