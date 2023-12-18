package index

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

// 以下是测试文件，使用到了github.com/stretchr/testify的库
//其中assert函数是一个断言，如果出了问题会在断言处报错的

func TestBTree_Put(t *testing.T) {
	bt := NewTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)

}

func TestBTree_Get(t *testing.T) {
	bt := NewTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.True(t, res3)

	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 22, Offset: 33})
	assert.True(t, res3)

	res4 := bt.Delete([]byte("aaa"))
	assert.True(t, res4)

}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewTree()
	//1、BTree为空的情况
	iter1 := bt1.Iterator(false)
	//因为没有存放数据，所以这里应该是false
	assert.Equal(t, iter1.Valid(), false)

	//2、BTree有数据的情况
	bt1.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	//t.Log(iter2)
	assert.Equal(t, true, iter2.Valid())
	//t.Log(iter2.Key())
	//t.Log(iter2.Value())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	//t.Log(iter2.Valid())
	assert.Equal(t, false, iter2.Valid())

	//3、有多条数据
	bt1.Put([]byte("bbde"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("ddde"), &data.LogRecordPos{Fid: 1, Offset: 10}) //为何不会进行覆盖
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}

	//测试seek
	iter5 := bt1.Iterator(false)

	for iter5.Seek([]byte("cc")); iter5.Valid(); iter5.Next() {
		//t.Log(string(iter5.Key()))
	}

	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("cc")); iter6.Valid(); iter6.Next() {
		t.Log(string(iter6.Key()))
	}

}
