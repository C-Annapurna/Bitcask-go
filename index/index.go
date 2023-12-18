package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
)

// 针对内存数据表进行操作的部分
// 定义抽象的索引接口,后续要想接入其他的数据结构，则直接实现这个接口即可       我们在btree.go中使用了google提供的btree，实现了Indexer接口中的所有函数
type Indexer interface {
	//向索引中存储key对应的数据的位置
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos

	//根据key取出对应的索引位置信息
	Get(key []byte) *data.LogRecordPos

	//根据key删除对应的索引位置信息
	Delete(key []byte) (*data.LogRecordPos, bool) //对delete和put进行修改

	//返回索引中的数据量
	Size() int

	//返回一个索引迭代器
	Iterator(reverse bool) Iterator

	//关闭索引
	Close() error
}

type Item struct { //这个就是内存数据结构   其中key是键值    pos就是对应的value  我们会记录文件的号码和偏移
	key []byte
	pos *data.LogRecordPos
}

// 这是google中 Item接口中定义的方法
func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

type IndexType = int8

const (
	//目前暂时只实现了btree实现的index接口
	Btree IndexType = iota + 1

	//后续还可能实现基于其他数据结构的index接口
	ART //自适应基数树索引

	BPTree //新增的b+树类型
)

// 根据类型初始化索引   在进行初始化索引的时候，会选择适当的数据结构进行初始化
func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewTree() //返回btree.go中的BTree索引结构
	case ART:
		//return nil
		return NewART()
	case BPTree:
		return nil
		//return NewBPlusTree(dirPath, sync)
	default:
		panic("unsupported index type")
	}
}

// 通用的索引迭代器接口
type Iterator interface {
	//重新回到迭代器的起点，即第一个数据
	Rewind()

	//根据传入的key查找第一个大于(或小于)等于的目标key，根据这个key开始遍历
	Seek(key []byte)

	//跳转到下一个key
	Next()

	//是否有效，即是否已经遍历完所有的key，用于退出遍历
	Valid() bool

	//当前遍历位置的key数据
	Key() []byte

	//当前遍历位置的Value数据
	Value() *data.LogRecordPos

	//关闭迭代器，释放相应数据
	Close()
}
