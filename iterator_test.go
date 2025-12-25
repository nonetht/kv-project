package bitcask_go

import (
	"bitcask-go/utils"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 时间大概是：55:00 ~ 59:00 左右
// TODO: 对迭代器创建函数，进行单元测试
func TestDB_NewIterator(t *testing.T) {
	db, cleanup := newDB(t, DefaultSetup)
	defer cleanup()

	entries := map[string][]byte{
		"iter-key-3": []byte("iter-value-3"),
		"iter-key-1": []byte("iter-value-1"),
		"iter-key-2": []byte("iter-value-2"),
	}
	for k, v := range entries {
		require.NoError(t, db.Put([]byte(k), v))
	}

	iter := db.NewIterator(DefaultIteratorSetup)
	defer iter.Close()

	var keys []string
	for iter.Rewind(); iter.Valid(); iter.Next() {
		k := string(iter.Key())
		val, err := iter.Value()
		require.NoError(t, err)
		assert.Equal(t, entries[k], val)
		keys = append(keys, k)
	}

	assert.Equal(t, []string{"iter-key-1", "iter-key-2", "iter-key-3"}, keys)
}

// TODO: 对迭代器，其中一个值进行单元测试。
func TestDB_Iterator_Single_Val(t *testing.T) {
	db, cleanup := newDB(t, DefaultSetup)
	defer cleanup()

	key := utils.GetTestKey(1)
	value := []byte("single-value")
	require.NoError(t, db.Put(key, value))

	iter := db.NewIterator(DefaultIteratorSetup)
	defer iter.Close()

	iter.Rewind()
	require.True(t, iter.Valid())
	assert.Equal(t, key, iter.Key())

	val, err := iter.Value()
	require.NoError(t, err)
	assert.Equal(t, value, val)

	iter.Next()
	assert.False(t, iter.Valid())
}

func TestDB_Iterator_Multi_Val(t *testing.T) {
	db, cleanup := newDB(t, DefaultSetup)
	defer cleanup()

	entries := []struct {
		key string
		val string
	}{
		{"prefix-2", "value-2"},
		{"other-1", "value-other"},
		{"prefix-1", "value-1"},
		{"prefix-0", "value-0"},
	}
	valueByKey := make(map[string][]byte)
	for _, e := range entries {
		valueByKey[e.key] = []byte(e.val)
		require.NoError(t, db.Put([]byte(e.key), valueByKey[e.key]))
	}

	// 反向迭代
	reverseIter := db.NewIterator(IteratorSetup{Reverse: true})
	defer reverseIter.Close()

	var reverseKeys []string
	for reverseIter.Rewind(); reverseIter.Valid(); reverseIter.Next() {
		k := string(reverseIter.Key())
		val, err := reverseIter.Value()
		require.NoError(t, err)
		assert.Equal(t, valueByKey[k], val)
		reverseKeys = append(reverseKeys, k)
	}
	assert.Equal(t, []string{"prefix-2", "prefix-1", "prefix-0", "other-1"}, reverseKeys)

	// 指定 prefix
	prefixIter := db.NewIterator(IteratorSetup{Prefix: []byte("prefix")})
	defer prefixIter.Close()

	var prefixKeys []string
	for prefixIter.Rewind(); prefixIter.Valid(); prefixIter.Next() {
		k := string(prefixIter.Key())
		val, err := prefixIter.Value()
		require.NoError(t, err)
		assert.Equal(t, valueByKey[k], val)
		prefixKeys = append(prefixKeys, k)
	}
	assert.Equal(t, []string{"prefix-0", "prefix-1", "prefix-2"}, prefixKeys)

	// 前缀不存在时，应直接结束遍历
	emptyIter := db.NewIterator(IteratorSetup{Prefix: []byte("missing")})
	defer emptyIter.Close()
	emptyIter.Rewind()
	assert.False(t, emptyIter.Valid())
}
