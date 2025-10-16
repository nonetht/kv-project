package fio

// IoManager 将标准文件操作比如read，write，close进行简单封装。
// 当前仅支持标准的系统文件IO，如果后面有其他IO类型，都可以进行接入
type IoManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Sync() error
	Close() error
}
