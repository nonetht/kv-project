package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

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

// EncodeLogRecord 对 LogRecod 进行编码，返回字节数组
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}
