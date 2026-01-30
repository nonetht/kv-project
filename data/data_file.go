package data

import (
	"bitcask-go/fio"
	"errors"
	"fmt"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

const (
	FileNameSuffix = ".data"
	HintFileName   = "hint-index"
	MergeFinished  = "merge-finished"
	SeqNoFileName  = "seq-no"
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
	fileName := GetDataFileName(dirPath, fileId)
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

// OpenSeqNoFile 存储事务序列号的文件
func OpenSeqNoFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, SeqNoFileName)
	return newDataFile(fileName, 0)
}

func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinished)
	return newDataFile(fileName, 0)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+FileNameSuffix) // ?
	return fileName
}

func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encodedRec, _ := EncodeLogRecord(record)
	return df.Write(encodedRec)
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

	var heaSize int64 = maxLogRecordHeaderSize
	// 处理其中的 corner case，就是我们的 maxHeaderSize + offset < fileSize。如果条件为真，那么将 heaSize 定为
	if heaSize+offset > fileSize {
		heaSize = fileSize - offset
	}

	buf, err := df.readNBytes(heaSize, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := decodeLogRecordHeader(buf)
	if header == nil {
		return nil, 0, io.EOF
	}

	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 在读取到 header 之后，我们转向获取对应的 keySize，valueSize
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recSize = headerSize + keySize + valueSize

	logRecord := &LogRecord{
		Type: header.recordType,
	}

	kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
	if err != nil {
		return nil, 0, err
	}

	key, value := kvBuf[:keySize], kvBuf[keySize:]
	logRecord.Key = key
	logRecord.Value = value

	crc := getLogRecordCRC(logRecord, buf[4:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recSize, nil
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
