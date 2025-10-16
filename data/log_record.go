package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelted
)

// LogRecord 向磁盘中写入的数据信息
type LogRecord struct {
	key   []byte
	value []byte
	Type  LogRecordType // 新增或修改还是删除
}

// LogRecordPos 向内存中写入的索引信息
// 说明了文件名称以及位置
type LogRecordPos struct {
	Fid    uint32
	offset int64
}
