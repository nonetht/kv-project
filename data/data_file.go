package data

import "bitcask-go/fio"

// DataFile 数据文件的结构体
type DataFile struct {
	FileId    uint32        // 文件id
	WriteOff  int64         // 文件写到了哪个位置
	IoManager fio.IOManager // io 读写管理，需要调该接口，实现对数据的读写操作
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {

}
