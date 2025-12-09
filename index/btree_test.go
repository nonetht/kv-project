package index

import (
	"bitcask-go/data"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestBTree_Put 其中参数类型 *testing.T 是什么意思呢?
func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{1, 100})
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{1, 2})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	bt.Put(nil, &data.LogRecordPos{1, 100})

	// 测试获取key=nil对应值的情况
	pos1 := bt.Get(nil) // pos1 类型是 *data.LogRecordPos
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	// 测试获取key="a"对应值的情况
	bt.Put([]byte("a"), &data.LogRecordPos{2, 2})

	pos2 := bt.Get([]byte("a")) // []byte类型总感觉怪...
	assert.Equal(t, uint32(2), pos2.Fid)
	assert.Equal(t, int64(2), pos2.Offset)

	// 连续两次Put函数添加，会改变key对应的value，测试value是否如期改变
	bt.Put([]byte("a"), &data.LogRecordPos{1, 3})
	pos3 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos3.Fid)
	assert.Equal(t, int64(3), pos3.Offset)
	t.Log(pos3)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()

	bt.Put(nil, &data.LogRecordPos{1, 100})
	res1 := bt.Delete(nil)
	assert.True(t, res1)

	bt.Put([]byte("a"), &data.LogRecordPos{2, 111})
	res2 := bt.Delete([]byte("a"))
	assert.True(t, res2)
}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBTree()

	// 如何测试我们的迭代器呢？对于迭代器的功能，我们应该怎样测试呢？还有就是像 roseduan 这种测试的方法实在是不太好。
	// TODO: 因为万一前面的 case 没有通过测试，那么后面的测试也是无法通过。我的想法是，将所有的 case 拆分为单独的子测试出来。
	// 1. btree 为空的情况下
	iter1 := bt1.Iterator(false)
	assert.False(t, iter1.Valid())

	// 2. Put 进去一个值的时候，测试其 (Key，Value)
	bt1.Put([]byte("code"), &data.LogRecordPos{Fid: 1, Offset: 100})
	iter2 := bt1.Iterator(false)
	assert.True(t, iter2.Valid())
	assert.Equal(t, []byte("code"), iter2.Key())
	assert.Equal(t, &data.LogRecordPos{Fid: 1, Offset: 100}, iter2.Key())
}
