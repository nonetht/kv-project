package data

import (
	"encoding/binary"
	"hash/crc32"
)

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

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组以及其长度
// +--------------+-----------+---------------+---------------+--------+--------+
// |                     LogRecordHeader部分                   |   LogRecord内容 |
// +--------------+-----------+---------------+---------------+--------+--------+
// | crc 校验值   | type 类型  | key size      | value size     | key   | value |
// +--------------+-----------+---------------+---------------+--------+--------+
// | 4字节        | 1字节      | 变长(最大5)    | 变长(最大5)     | 变长   | 变长    |
// +--------------+-----------+---------------+---------------+--------+--------+
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// 初始化一个 header 部分的字节数组，随后将 header 的字段部分转换为 []byte （字节数组）
	header := make([]byte, maxLogRecordHeaderSize)

	// 第五个字节存储 Type
	header[4] = logRecord.Type
	var index = 5
	// 下标为5字节之后，存储到是key，value的长度信息
	// binary 包，存储变长顺序；同时，binary.PutVarint 方法会返回写入了多少个字节，因此需要对index进行递增
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))   // header[index:] -> 表示从下标 index 开始
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value))) // header[index:] -> 表示从下标 index 开始

	// 最后编码的长度，就是如此：header 长度，Key 长度以及 Value 长度
	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encodedBytes := make([]byte, size)

	// 于是我们又获得了一个更大的数组（bigger one），接下来是将 header 部分拷贝进来
	// copy -> func copy(dst, src []Type) int
	copy(encodedBytes[:index], header[:index]) // 将 header 拷贝到 encodedBytes

	// 将 key 和 value 拷贝到字节数组之中，这里注意长度大小的问题。
	copy(encodedBytes[index:], logRecord.Key)
	copy(encodedBytes[index+len(logRecord.Key):], logRecord.Value)

	// crc 校验，Go之中自带该方法，可以直接调用
	crc := crc32.ChecksumIEEE(encodedBytes[4:]) // 最终返回的是 uint32 类型，如何将其转换为 []byte 类型呢？
	// 小端序，大端序....(这里使用BigEndian可以吗？)
	// PutUint32 涉及到移位运算...
	binary.LittleEndian.PutUint32(header[:4], crc)

	return encodedBytes, int64(size)
}

// 根据字节数组中 Header 信息进行解码，从而拿到 header 头部信息，并且返回 header 长度
// 是不是其中的 buf 存储的 header 字节数组呢？
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) < 5 {
		return nil, 0
	}

	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5
	// 取出 keySize，
	// TODO: 但是我还是不理解，就是 size 都是变长的，为什么可以直接读取出 keySize 呢？
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize) // 给结构体之中的 keySize 字段赋值
	index += n

	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n
	return header, int64(index)
}

// 我们需要同时获取其 logRecord 之中的key，value信息，以及 header 信息。
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}
