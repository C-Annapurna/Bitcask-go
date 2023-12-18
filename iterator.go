package bitcask_go

import (
	"bitcask-go/index"
	"bytes"
)

// 面向用户的迭代器
type Iterator struct {
	indexIter index.Iterator //索引迭代器
	db        *DB
	options   IteratorOptions
}

// 初始化一个属于db的迭代器
func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		indexIter: indexIter,
		db:        db,
		options:   opts,
	}
}

// 重新回到迭代器的起点，即第一个数据
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// 根据传入的key查找第一个大于(或小于)等于的目标key，根据这个key开始遍历
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// 跳转到下一个key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// 是否有效，即是否已经遍历完所有的key，用于退出遍历
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// 当前遍历位置的key数据
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// 当前遍历位置的Value数据
func (it *Iterator) Value() ([]byte, error) {
	//根据迭代器得到数据的位置
	logRecordPos := it.indexIter.Value()
	it.db.mu.Lock()
	defer it.db.mu.Unlock()
	return it.db.getValueByPosition(logRecordPos)
}

// 关闭迭代器，释放相应数据
func (it *Iterator) Close() {
	it.indexIter.Close()
}

func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
