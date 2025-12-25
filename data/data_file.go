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
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

const (
	FileNameSuffix = ".data"
	HintFileName   = "hint-index"
	MergeFinished = "merge-finished"
)

// DataFile 使用结构体来定义数据文件
type DataFile struct {
	FileId   uint32 // 当前文件id
	WriteOff int64  // 当前文件写入到了哪个位置
	// 之前定义的文件操作的抽象接口，我们需要调用该接口实现数据读写的操作
	IoManager fio.IoManager
}

// OpenDataFile 打开数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	// fileName = filePath + fileName + Suffix
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+FileNameSuffix)
	// 初始化 IOManager 管理器接口；同时也是下面这行代码实现了，即便不存在的名称也可以被创建。
	return newDataFile(fileName, fileId)
}

func newDataFile(fileName string, fileId uint32) (*DataFile, error) {
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

// OpenHintFile 打开 Hint 索引文件。
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0)
}

// 标识 merge 完成的文件
func

func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	return nil
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	// 随后对 header 数组进行解码
	header, headerSize := decodeLogRecordHeader(headerBuf) // 将 header 信息从切片之中提取出来
	if header == nil {
		// 标明读取到了文件的末尾，读取完毕返回 EOF
		return nil, 0, io.EOF
	}
	// 同样表示，也是读取到文件末尾，返回 EOF
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 取出对应的 key 和 value 的长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var logRecordSize = headerSize + keySize + valueSize

	logRecord := &LogRecord{Type: header.recordType}
	// 随后根据 key，value 的长度，读取其中的key, value的信息
	// 之所以使用 || 是因为会存在一些 key 非空，而 value 为空的情况。如果是采用 && 就会漏掉部分记录
	if keySize > 0 || valueSize > 0 {
		// 读取 key 和 value 长度的字节。（为什么呢，为什么不可以分开阅读呢？） -> 就是系统调用昂贵，减少；磁盘IO，多次读取会降低效率
		// 读取的偏移是从 offset + headerSize 开始
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// 校验数据的有效性，crc：循环冗余校验
	// crc 校验值计算公式：CRC = Hash( HeaderBody + Key + Value )
	// TODO: 读取到的是，最大的 header 信息？这里也是比较难的地方
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, logRecordSize, nil
}

// 从数据文件的指定偏移量offset处，读取n个字节的数据，并将其作为字节切片返回。
// TODO: 难点，就在于理解其中 os.ReadAt 函数！
func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)                   // 创建长度为 n，类型为 byte 的切片
	_, err = df.IoManager.Read(b, offset) // 通过 df.IoManager 执行 Read 函数: 就是在offset偏移量之后，读取 len(b) 的字节数目
	if err != nil {
		return nil, err
	}
	return
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}
