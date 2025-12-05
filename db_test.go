package bitcask_go

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 测试完成之后，销毁 DB 数据目录
// 为 destoryDB添加了活跃文件空指针保护。空目录打开时，activeFile为空不再出发panic。
func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.activeFile.Close() // TODO: 实现了 Close 方法后，用 Close 方法来替代。
		}
		err := os.RemoveAll(db.setup.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestOpen(t *testing.T) {
	setup := DefaultSetup
	dir, _ := os.MkdirTemp("", "bitcask-go")
	setup.DirPath = dir
	db, err := Open(setup)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	setup := DefaultSetup
	dir, _ := os.MkdirTemp("", "bitcask-go-put")
	setup.DirPath = dir
	setup.DataFileSize = 64 * 1024 * 1024
	db, err := Open(setup)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常Put一条数据
}
