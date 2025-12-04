package data

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDataFile(t *testing.T) {
	// 这样可以创建一个临时目录用于测试，避免污染系统的/tmp目录
	// 将 os.TempDir() 修改为 t.TempDir() 可以自动创建一个专属于本次测试的临时目录
	tempDir := t.TempDir()
	fmt.Println("tempDir:", tempDir)

	dataFile1, err := OpenDataFile(tempDir, 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	dataFile2, err := OpenDataFile(tempDir, 111)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)

	dataFile3, err := OpenDataFile(tempDir, 111)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile3)
}

func TestDataFile_Write(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 111)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("aaa")) // 写入一条数据
	assert.Nil(t, err)

	err = dataFile.Write([]byte("bbb")) // 写入一条数据
	assert.Nil(t, err)

	err = dataFile.Write([]byte("ccc")) // 写入一条数据
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 123)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("aaa")) // 写入一条数据
	assert.Nil(t, err)

	err = dataFile.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 502)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("aaa")) // 写入一条数据
	assert.Nil(t, err)

	err = dataFile.Sync()
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	// 使用专门的方法 t.TempDir()。会为每一次测试运行创建一个全新的、独立的、随机的临时目录
	tmpDir := t.TempDir()
	dataFile, err := OpenDataFile(tmpDir, 222) // os.TempDir()路径在: /var/folders/hg/dvfl8ymd03l80wctphqtg_g80000gn/T/
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	// 只有一条 LogRecord
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask kv go"),
	}

	// 将 rec1 编码，随后写入到 dataFile 之中
	res1, size1 := EncodeLogRecord(rec1)
	err = dataFile.Write(res1)
	assert.Nil(t, err)

	// readSize1 就是 LogRecord 的总长度
	readRec1, readSize1, err := dataFile.ReadLogRecord(0) // 随后我们从起始位置（offset=0）开始读取数据
	assert.Nil(t, err)
	assert.Equal(t, rec1, readRec1)
	assert.Equal(t, size1, readSize1)

	// 多条 LogRecord 的从不同的位置读取

	// 被删除的数据在数据文件的末尾
}
