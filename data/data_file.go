package data

import (
	"bitcask-go/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid CRC value, log may be corrupted")
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
	headerBuf, err := df.readNBytes(maxLogRecordSize, offset)
	if err != nil {
		return nil, 0, err
	}

	head, headSize := DecodeLogRecordHeader(headerBuf)
	if head == nil {
		// 如果读取到的 header 为空的话，则说明已经读取完毕，应该返回一个 EOF
		return nil, 0, io.EOF
	}

	if head.crc == 0 && head.keySize == 0 && head.valueSize == 0 {
		// 如果校验值和key，value大小也为0，说明到了末尾，执行返回
		return nil, 0, io.EOF
	}

	// 取出对应的 key，value长度
	keySize, valueSize := int64(head.keySize), int64(head.valueSize)
	var recordSize = headSize + keySize + valueSize

	logRecord := &LogRecord{Type: head.readType}

	// 读取一个实际的key，value
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headSize) // offset 也要进行更新
		if err != nil {
			return nil, 0, err
		}

		// 输出 key 和 value
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// 校验数据 crc 是否正确，校验数据有效性
	crc := getLogrecordCRC(logRecord, headerBuf[crc32.Size:headSize])
	if crc != head.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, recordSize, nil
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
func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

// 指定读取多少字节，调用IOManager读取对应数据，并返回一个字节数组
func (df *DataFile) readNBytes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := df.IoManager.Read(b, offset)
	if err != nil {
		return nil, err
	}

	return b, nil
}
