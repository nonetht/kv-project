package data

import (
	"bitcask-go/fio"
	"fmt"
	"io"
	"path/filepath"
)

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
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
	// 初始化 IOManager 管理器接口
	ioManager, err := fio.NewIoManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

// ReadLogRecord
// TODO: 读取对应的 LogRecord 为什么还要需要 offset？
// 除了返回一条 LogRecord，还要返回 LogRecord 大小
// TODO: 说实话，我感觉我什么不能单独创建一个函数来求取 LogRecord 的大小呢？
// 确实，其实我也考虑过，就是用单独的函数来做，是不是更好呢？
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	headerBuf, err := df.readNBytes(maxLogRecordHeaderSize, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := decodeLogRecordHeader(headerBuf)
	if header == nil {
		// 标明读取到了文件的末尾
		return nil, 0, io.EOF
	}
	// 同样表示，也是读取到文件末尾，返回 EOF
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 取出对应的 key 和 value 的长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var logrecordSize =

	return nil, 0, nil
}

// 读取 n 个字节。
// TODO: 但是其中返回值为什么是切片和err呢？我观察到 Read 函数是返回的数量，返回的 b 只是一个空切片啊！
func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)                   // 创建长度为 n，类型为 byte 的切片
	_, err = df.IoManager.Read(b, offset) // 通过 df.IoManager 执行 Read 函数: 就是在offset偏移量之后，读取 len(b) 的字节数目
	return
}
