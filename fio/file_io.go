package fio

import "os"

// FileIO 标准系统文件 IO
// 实现了 IOManager 这个接口
type FileIO struct {
	fd *os.File
}

// NewFileIOManager 初始化标准文件
func NewFileIOManager(filePath string) (*FileIO, error) {
	fd, err := os.OpenFile(
		filePath,
		os.O_CREATE|os.O_RDWR|os.O_APPEND, // 描述的似乎是，如果没有该文件存在，就创建一个
		DataFilePerm)
	// 如果err不是nil，就代表出了问题
	if err != nil {
		return nil, err
	}
	// 返回文件读写的地址
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

// Size 先获取FileInfo，随后获取对应的文件大小。
// FileInfo 应该存储了该文件的大小
func (fio *FileIO) Size() (int64, error) {
	// Stat returns the [FileInfo] structure describing file.
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil // Size: length in bytes for regular files
}
