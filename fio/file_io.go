package fio

import "os"

const DataFilePerm = 0644

// FileIO 结构体实现了 io_manager 接口，IoManager接口中所有方法最终都是对磁盘文件进行操作
// Go语言中，该“文件”就是 *os.File 类型变量，称之为 File Descriptor / File Handle
type FileIO struct {
	fd *os.File // 文件句柄或文件描述符
}

// NewFileIOManager 初始化标准文件
func NewFileIOManager(filePath string) (*FileIO, error) {
	fd, err := os.OpenFile(
		filePath,
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		DataFilePerm,
	)
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
