package fio

import "os"

// FileIO 标准系统文件 IO
type FileIO struct {
	fd *os.File
}

// NewFileIOManager 初始化标准文件
func NewFileIOManager(filePath string) (*FileIO, error) {
	fd, err := os.OpenFile(
		filePath,
		os.O_CREATE|os.O_RDWR|os.O_APPEND, // 描述的似乎是，如果没有该文件存在，就创建一个
		DataFilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

