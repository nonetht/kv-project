package bitcask_go

import (
	"bitcask-go/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 测试完成之后，销毁 DB 数据目录
// 为 destoryDB 添加了活跃文件空指针保护。空目录打开时，activeFile为空不再出发panic。
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

/*
我之前接触过，根据测试驱动的方式来学习Go语言，我在想是否可以将这些测试更精简，将部分代码抽象为函数。此外就是，
将测试之中的case分为不同部分来执行，这样可以让整个测试更条理论！而不是从头到尾简单的代码执行。
*/

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
	t.Log(dir)
	setup.DirPath = dir
	setup.DataFileSize = 64 * 1024 * 1024
	// 根据数据库配置 setup，Open一个数据库
	db, err := Open(setup)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	/*
		其测试流程大致相同，不同地方也就是 Put 的数据略有不同。相同地方在于：
		1. 对 Put 后的返回值进行检查，看反复的 err 是否为空
		2. 同时，对 db 执行 Get 操作，涉及到的 key 相同
		3. 最后对返回值进行校验
		因此，可以将这些重复部分抽象为一个函数 ———— checkPutResult
	*/

	// 1.正常Put一条数据
	t.Run("Put one kv pair", func(t *testing.T) {
		err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
		checkPutResult(t, err, db)
	})

	// 2. 重复 Put key 相同的数据
	// TODO: 如果是重复写入的话，是不是也不会对之前写入的键值对造成影响呢？
	t.Run("Put 2 same key", func(t *testing.T) {
		err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
		checkPutResult(t, err, db)
	})

	// 3. key 为空
	t.Run("Put empty key", func(t *testing.T) {
		err = db.Put([]byte(""), utils.RandomValue(24)) // key 为空的话，肯定会报错的
		assert.NotNil(t, err)
		//t.Log(err) // key is empty
	})

	// 4. value 为空的情况。value为空的话，是可以正常添加的。
	t.Run("Put empty value", func(t *testing.T) {
		err = db.Put(utils.GetTestKey(1), []byte(""))
		checkPutResult(t, err, db)
	})

	// 5. 写到数据文件进行了转换
	// TODO: 我说好奇的是，为什么会有这种数字 1000000 如何得到的呢？
	t.Run("Put ...", func(t *testing.T) {
		for i := 0; i < 1000000; i++ {
			err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
			assert.Nil(t, err)
		}
		assert.Equal(t, 2, len(db.inactiveFile))
	})

	// 6. 重启后，Put 数据
	t.Run("After reset, Put data", func(t *testing.T) {
		err = db.activeFile.Close()
		assert.Nil(t, err)

		// 重启数据库
		db2, err := Open(setup)
		assert.Nil(t, err)
		assert.NotNil(t, db2)
		err = db2.Put(utils.GetTestKey(55), utils.RandomValue(128))
		assert.Nil(t, err)
	})
}

func TestDB_Get(t *testing.T) {

}

func TestDB_Delete(t *testing.T) {

}

func checkPutResult(t *testing.T, err error, db *DB) {
	t.Helper() // 告诉测试框架，该方法是一个辅助函数。测试失败的话，报告行号指向调用函数地方而非测试辅助函数内部。
	assert.Nil(t, err)
	normalVal, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, normalVal)
}
