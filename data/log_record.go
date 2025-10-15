package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota // iota是什么？
	LogRecordDeleted
)

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
	Type  LogRecordType // 新增/修改，还是删除？
}

// EncodeLogRecord 对 LogRecord 编码，返回字节数组以及长度
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}
