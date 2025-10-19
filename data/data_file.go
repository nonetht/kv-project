package data

import (
	"bitcask-go/fio"
	"fmt"
	"path/filepath"
)

// DataFileNameSuffix 为后缀定义一个常量
const DataFileNameSuffix = ".data"

// DataFile 数据文件的结构体
type DataFile struct {
	FileId    uint32        // 文件id
	WriteOff  int64         // 文件写到了哪个位置
	IoManager fio.IOManager // io 读写管理，需要调该接口，实现对数据的读写操作
}

// OpenDataFile 打开新的数据文件，以及对应文件id；随后添加数据文件后缀 .data 从而构造出完整的数据文件名称
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
	// 初始化 IOManager
	manager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: manager,
	}, nil
}

// ReadLogRecord 读取LogRecord，很重要的方法。根据偏移offset，来读取指定位置的LogRecord信息
// Ps. 为什么LogRecord来要记录 key，value 的长度呢？
// 先根据 offset 读取 header 部分，随后在拿到key，value长度之后，根据key，value的长度来读取存储的具体key，value信息
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}

// Sync 貌似是数据持久化方法，就是将数据持久化
func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Write(buf []byte) error {
	nBytes, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(nBytes)
	return nil
}

// Close 之前没有显示这部分的代码，应该是新增的
//func (df *DataFile) Close() error {
//	return nil
//}
