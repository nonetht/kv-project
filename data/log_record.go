package data

// LogRecord 向磁盘中写入的数据信息
type LogRecord struct {
	key   []byte
	value []byte
}

// LogRecordPos 向内存中写入的索引信息
// 说明了文件名称以及位置
type LogRecordPos struct {
	fileName string
	offset   int64
}
