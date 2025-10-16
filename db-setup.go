package bitcask_go

// 就是类似数据的配置，用户需要指定对应的文件路径以配置数据库
type SetUp struct {
	DirPath    string      // 数据库数据目录
	DataSize   int64       // 数据写入的预值
	IndexType  IndexerType // 索引类型
	SyncWrites bool        // 决定每次写入数据是否持久化
}

type IndexerType = int8

const (
	// BTree 索引
	// 此外，就是iota是一个数值为0的常量
	BTree IndexerType = iota + 1
)
