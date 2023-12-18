package redis

import (
	bitcask "bitcask-go"
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestRedisDataStructure_Get(t *testing.T) {
	opts := bitcask.DefaultOptioins
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(2), time.Second*5, utils.RandomValue(100))
	assert.Nil(t, err)

	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	val2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	_, err = rds.Get(utils.GetTestKey(33))
	assert.Equal(t, err, bitcask.ErrKeyNotFound)
}

func TestRedisDataStructure_Del_Type(t *testing.T) {
	opts := bitcask.DefaultOptioins
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-del-type")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(2), time.Second*5, utils.RandomValue(100))
	assert.Nil(t, err)

	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = rds.Del(utils.GetTestKey(33))
	assert.Equal(t, err, bitcask.ErrKeyNotFound)

	typ, err := rds.Type(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.Equal(t, typ, String)
}

func TestRedisDataStructure_HGet(t *testing.T) {
	opts := bitcask.DefaultOptioins
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-hget")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.Equal(t, ok1, true)

	v1 := utils.RandomValue(100)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	assert.Nil(t, err)
	assert.Equal(t, ok2, false)

	v2 := utils.RandomValue(100)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	assert.Equal(t, ok3, true)
	assert.Nil(t, err)

	val1, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.Equal(t, v1, val1)

	val2, err := rds.HGet(utils.GetTestKey(1), []byte("field2"))
	assert.Nil(t, err)
	assert.Equal(t, v2, val2)

	_, err = rds.HGet(utils.GetTestKey(1), []byte("field-not-exist"))
	assert.Equal(t, err, bitcask.ErrKeyNotFound)

}

func TestRedisDataStructure_HDel(t *testing.T) {
	opts := bitcask.DefaultOptioins
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-hget")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	del1, err := rds.HDel(utils.GetTestKey(200), nil)
	assert.False(t, del1)
	assert.Nil(t, err)
	//t.Log(del1, err)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.Equal(t, ok1, true)

	v1 := utils.RandomValue(100)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	assert.Nil(t, err)
	assert.Equal(t, ok2, false)

	v2 := utils.RandomValue(100)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	assert.Equal(t, ok3, true)
	assert.Nil(t, err)

	del2, err := rds.HDel(utils.GetTestKey(1), []byte("field1"))
	assert.True(t, del2)
	assert.Nil(t, err)
	//t.Log(del2, err)

	val2, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, val2)
	assert.Equal(t, err, bitcask.ErrKeyNotFound)
	//t.Log(val2, err)

	val3, err := rds.HGet(utils.GetTestKey(1), []byte("field2"))
	assert.Nil(t, err)
	assert.NotNil(t, val3)
	//t.Log(string(val3), err)
}

func TestRedisDataStructure_SisMember(t *testing.T) {
	opts := bitcask.DefaultOptioins
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-SisMember")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-1")) //设置重复的member会跳过
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SisMember(utils.GetTestKey(1), []byte("val-1"))
	//t.Log(ok, err)
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SRem(utils.GetTestKey(1), []byte("val-1"))
	//t.Log(ok, err)
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SisMember(utils.GetTestKey(1), []byte("val-1"))
	//t.Log(ok, err)
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = rds.SisMember(utils.GetTestKey(1), []byte("val-2"))
	//t.Log(ok, err)
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SisMember(utils.GetTestKey(2), []byte("val-1"))
	//t.Log(ok, err)
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestRedisDataStructure_SRem(t *testing.T) {
	opts := bitcask.DefaultOptioins
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-SRem")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-1")) //设置重复的member会跳过
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SRem(utils.GetTestKey(2), []byte("val-1")) //删除一个没有的key
	//t.Log(ok, err)
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = rds.SRem(utils.GetTestKey(1), []byte("val-1"))
	//t.Log(ok, err)
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SRem(utils.GetTestKey(1), []byte("val-2"))
	//t.Log(ok, err)
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SRem(utils.GetTestKey(1), []byte("val-not-exist")) //删除一个没有的member
	//t.Log(ok, err)
	assert.Nil(t, err)
	assert.False(t, ok)

}

func TestRedisDataStructure_List(t *testing.T) {
	opts := bitcask.DefaultOptioins
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-List")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	res, err := rds.LPush(utils.GetTestKey(1), []byte("val-1"))
	t.Log(res, err)
	res, err = rds.LPush(utils.GetTestKey(1), []byte("val-1"))
	t.Log(res, err)
	res, err = rds.LPush(utils.GetTestKey(1), []byte("val-2"))
	t.Log(res, err)

	val1, err := rds.LPop(utils.GetTestKey(1))
	t.Log(string(val1), err)
	val2, err := rds.RPop(utils.GetTestKey(1))
	t.Log(string(val2), err)
}

func TestRedisDataStructure_ZScore(t *testing.T) {
	opts := bitcask.DefaultOptioins
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-ZScore")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := rds.ZAdd(utils.GetTestKey(1), 113, []byte("val-1"))
	t.Log(ok, err)
	ok, err = rds.ZAdd(utils.GetTestKey(1), 333, []byte("val-1"))
	t.Log(ok, err)
	ok, err = rds.ZAdd(utils.GetTestKey(1), 98, []byte("val-2"))
	t.Log(ok, err)

	val, err := rds.ZScore(utils.GetTestKey(1), []byte("val-1"))
	t.Log(val, err)
	val, err = rds.ZScore(utils.GetTestKey(1), []byte("val-2"))
	t.Log(val, err)
}
