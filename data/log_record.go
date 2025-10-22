package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota // iota是什么？
	LogRecordDeleted
)

// crc type keySize valueSize
// 4 + 1 + 5 + 5(binary.MaxVarintLen32)
const maxLogRecordHeadSize = 15

// LogRecordPos 数据内存索引，主要是描述数据在磁盘上的位置
// 定义了目录中每一条索引的格式。告诉我们一个一个Key对应的数据存在哪个文件的哪个位置
type LogRecordPos struct {
	Fid    uint32 // 文件id，表示将文件存储到了哪个文件之中
	Offset int64  // 偏移，表示将数据存储到了数据文件中的哪个位置
}

// LogRecord 写入到数据文件的记录格式
// 由于数据文件以类似日志形式被追加写入，因此称为日志
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType // 新增/修改，还是删除？墓碑值？
}

// LogRecordHeader 定义 LogRecord 中的 Header 的结构信息
type LogRecordHeader struct {
	crc        uint32        // crc 校验值
	recordType LogRecordType // 表示 LogRecord 的类型，查看其是否是待删除类型（是否是墓碑值）
	keySize    uint32
	valueSize  uint32
}

// EncodeLogRecord 对 LogRecord 编码，返回字节数组以及长度
// +--------------+-----------+---------------+---------------+--------+--------+
// |                     LogRecordHeader部分                   |   LogRecord内容 |
// +--------------+-----------+---------------+---------------+--------+--------+
// | crc 校验值   | type 类型  | key size      | value size     | key   | value |
// +--------------+-----------+---------------+---------------+--------+--------+
// | 4字节        | 1字节      | 变长(最大5)    | 变长(最大5)     | 变长   | 变长    |
// +--------------+-----------+---------------+---------------+--------+--------+
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// 初始化一个 header 部分的字节数组
	header := make([]byte, maxLogRecordHeadSize)

	// 第五个字节存储 Type
	// 我之前写成了 header[5] = ...
	header[4] = logRecord.Type
	var index = 5
	// 5 字节之后，存储的是 key 和 value的长度
	// 使用变长类型，节省空间
	// binary.PutVarint 方法会返回写入的字节的数量，因此用 index 来递增就很合适
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	//for _, val := range header {
	//	fmt.Println("val: ", val)
	//}

	// 整条logRecord的编码后长度 = header的长度 + Key的长度 + Value的长度
	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encodedBytes := make([]byte, size)

	// 将 header 部分拷贝到长度为size的数组中
	copy(encodedBytes[:index], header[:index])
	// 将 key，value数据分别直接拷贝到字节数组中
	copy(encodedBytes[index:], logRecord.Key)
	copy(encodedBytes[index+len(logRecord.Key):], logRecord.Value)

	// 对整个 LogRecord 进行 crc 校验
	crc := crc32.ChecksumIEEE(encodedBytes[4:])
	// 小端序，为什么呢？
	binary.LittleEndian.PutUint32(encodedBytes[:4], crc) // 之前写成了 encodedBytes[4:]

	return encodedBytes, int64(size)
}

// DecodeLogRecordHeader 对字节数组的 header 信息进行解码，从而得到一个 LogRecordHeader，以及其对应的长度
func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	// hard code style, not good
	// 如果连 crc 4个字节都没有占到，说明有问题
	// TODO: 但是我对此有问题，就是为什么是4而不是5呢？看 EncodeLogRecord 函数之中，index一开始设置变为5，为什么不是5呢？
	if len(buf) <= 4 {
		return nil, 0
	}

	// 先读取部分属性信息
	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	// 从下边为5的位置拿取
	var index = 5

	// 取出实际的 key size
	// TODO: 这是怎么取得的呢？我不理解。
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	// TODO: 同上，我不理解怎样获取的
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n // index 代表实际的 header 的长度

	return header, int64(index)
}

func getLogRecordCRC(lr *LogRecord, head []byte) uint32 {
	if lr == nil {
		return 0
	}

	// head 部分，但是不是最终的crc的值，还要加上后面 Key，Value 两字段才行
	crc := crc32.ChecksumIEEE(head[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
