package fio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 在测试之后，我们要将测试之中产生的临时文件清除掉
func destoryFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestFileIOManager(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path) // ?

	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("bitcask kv"))
	assert.Equal(t, 10, n)

	n, err = fio.Write([]byte("storage"))
	assert.Equal(t, 7, n)
}

func TestFileIO_Read(t *testing.T) {

	// 注意需要对file进行更改，不要一直使用a.data文件
	path := filepath.Join("/tmp", "b.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	b1 := make([]byte, 5) // 字节切片
	n, err := fio.Read(b1, 0)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b1)

	b2 := make([]byte, 5)
	n, err = fio.Read(b2, 5)
	// t.Log(string(b2), err)

	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-b"), b2)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("/tmp", "c.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("/tmp", "d.data")
	fio, err := NewFileIOManager(path)
	defer destoryFile(path)

	err = fio.Close()
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}
