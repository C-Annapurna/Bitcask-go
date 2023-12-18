package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
	"sort"
	"sync"
)

//使用b树实现了indexer抽象接口中的所有方法，当然也可以使用其他的数据结构来实现（哈希表，跳表，基数树）

// 这个btree结构主要就是封装google的btree库   不用我们重复实现一个btree的结构了
type BTree struct {
	tree *btree.BTree  //点进定义中可以看到注释说这个结构在多个goroutine中并发写是不安全的(并发读是安全的)，所以我们需要对他加锁进行保护
	lock *sync.RWMutex //这个锁就是保护并发读的情况下对tree进行保护的
}

// 初始化BTree索引结构
func NewTree() *BTree {
	return &BTree{
		tree: btree.New(32), //这里32表示叶子结点的数量
		lock: new(sync.RWMutex),
	}
}

// 下面的函数主要其实根本上还是调用的google实现的btree的一些函数来实现的
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{
		key: key,
	}
	btreeItem := bt.tree.Get(it) //根据it得到btreeItem，注意这里的btreeItem是谷歌那个btree
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos //要返回的话，需要转化成Item(我们自定义的)
}

func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock() //当初忘记释放锁了，在后面写测试文件的时候发生报错：fatal error: all goroutines are asleep - deadlock!
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

// 这个索引迭代器的方法也是供BTree结构使用的
func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	return NewBTreeIterator(bt.tree, reverse)
}

func (bt *BTree) Close() error {
	return nil
}

// BTree索引迭代器   对应index.go中的Iterator接口
type btreeIterator struct {
	currIndex int     //当前遍历的下标位置
	reverse   bool    //是否是反向遍历
	values    []*Item //Item包括： key + 位置索引信息
}

// 这里是新建一个BTree索引迭代器的实例
func NewBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	//将所有的数据都存放在数组中
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}

	if reverse { //表示数据是从大到小的顺序进行存放的
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}
	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// 重新回到迭代器的起点，即第一个数据
func (bti *btreeIterator) Rewind() {
	bti.currIndex = 0
}

// 根据传入的key查找第一个大于(或小于)等于的目标key，根据这个key开始遍历
func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		//使用sort.Search会在[0,len(bti.values)]的范围内查找能够使匿名函数为true的索引i
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0 //使用bytes.Compare函数比较bti.values[i].key是否<=key。满足的话就返回true
		})
	} else {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}

}

// 跳转到下一个key
func (bti *btreeIterator) Next() {
	bti.currIndex += 1
}

// 是否有效，即是否已经遍历完所有的key，用于退出遍历
func (bti *btreeIterator) Valid() bool {
	return bti.currIndex < len(bti.values)
}

// 当前遍历位置的key数据
func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.currIndex].key
}

// 当前遍历位置的Value数据
func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currIndex].pos
}

// 关闭迭代器，释放相应数据   主要是将临时的数组清理掉
func (bti *btreeIterator) Close() {
	bti.values = nil
}
