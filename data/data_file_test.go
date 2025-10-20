package data

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDataFile(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	dataFile2, err := OpenDataFile(os.TempDir(), 111)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)

	dataFile3, err := OpenDataFile(os.TempDir(), 111)
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
