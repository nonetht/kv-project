package bitcask_go

import (
	"bitcask-go/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 测试完成之后，销毁 DB 数据目录
// 为 destoryDB添加了活跃文件空指针保护。空目录打开时，activeFile为空不再出发panic。
func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.activeFile.Close() // TODO: 实现了 db.Close() 方法后，用 db.Close 方法来替代。
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
	// 根据数据库配置 setup，Open一个数据库
	db, err := Open(setup)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常Put一条数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	normalVal, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, normalVal)

	// 2. 重复 Put key 相同的数据
	// TODO: 如果是重复写入的话，是不是也不会对之前写入的键值对造成影响呢？
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	duplicateVal, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, duplicateVal)

	// 3. key 为空
	err = db.Put([]byte(""), utils.RandomValue(24)) // key 为空的话，肯定会报错的
	assert.NotNil(t, err)
	//t.Log(err) // key is empty

	// 4. value 为空
}
