package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// crc type keySize valueSize Sum
// 4  +  1  +  5    +   5   =  15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord 向磁盘中写入的数据信息
// 之所以叫日志，是因为数据文件中数据是追加写入的，类似日志的格式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType // 新增或修改还是删除
}

func NewLogRecord(key []byte, value []byte) *LogRecord {
	return &LogRecord{
		Key:   key,
		Value: value,
		Type:  LogRecordNormal,
	}
}

// LogRecordPos 向内存中写入的索引信息
// 说明了文件名称以及位置
type LogRecordPos struct {
	Fid    uint32
	Offset int64
}

// LogRecord 头部信息
type logRecordHeader struct {
	crc        uint32        // crc 校验值
	recordType LogRecordType // 标识 LogRecord 类型
	keySize    uint32        // key 大小
	valueSize  uint32        // value 大小
}

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

// 根据字节数组中 Header 信息进行解码，从而拿到 header 头部信息，并且返回 header 长度
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	return nil, 0
}

// 我们需要同时获取其 logRecord 之中的key，value信息，以及 header 信息。
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}
