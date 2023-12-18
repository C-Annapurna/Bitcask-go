package index

import (
	"bitcask-go/data"
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

// 自适应基数树索引
// 封装的库https://github.com/plar/go-adaptive-radix-tree
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

// 初始化一个自适应基数树索引
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

// 向索引中存储key对应的数据的位置
func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	oldValue, _ := art.tree.Insert(key, pos)
	art.lock.Unlock()
	if oldValue == nil { //表示当前put的数据没有旧的值
		return nil
	}
	return oldValue.(*data.LogRecordPos)
}

// 根据key取出对应的索引位置信息
func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos) //需要进行类型转化
}

// 根据key删除对应的索引位置信息
func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	oldValue, deleted := art.tree.Delete(key)
	art.lock.Unlock()
	if oldValue == nil {
		return nil, false
	}
	return oldValue.(*data.LogRecordPos), deleted
}

// 返回索引中的数据量
func (art *AdaptiveRadixTree) Size() int {
	art.lock.Lock()
	size := art.tree.Size()
	art.lock.Unlock()
	return size
}

// 返回一个索引迭代器
func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newARTIterator(art.tree, reverse)
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

// ART索引迭代器   对应index.go中的Iterator接口
type artIterator struct {
	currIndex int     //当前遍历的下标位置
	reverse   bool    //是否是反向遍历
	values    []*Item //key+位置索引信息
}

// 这里是新建一个ART索引迭代器的实例
func newARTIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveValues)

	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// 重新回到迭代器的起点，即第一个数据
func (ai *artIterator) Rewind() {
	ai.currIndex = 0
}

// 根据传入的key查找第一个大于(或小于)等于的目标key，根据这个key开始遍历
func (ai *artIterator) Seek(key []byte) {
	if ai.reverse {
		//使用sort.Search会在[0,len(bti.values)]的范围内查找能够使匿名函数为true的索引i
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) <= 0 //使用bytes.Compare函数比较bti.values[i].key是否<=key。满足的话就返回true
		})
	} else {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) >= 0
		})
	}

}

// 跳转到下一个key
func (ai *artIterator) Next() {
	ai.currIndex += 1
}

// 是否有效，即是否已经遍历完所有的key，用于退出遍历
func (ai *artIterator) Valid() bool {
	return ai.currIndex < len(ai.values)
}

// 当前遍历位置的key数据
func (ai *artIterator) Key() []byte {
	return ai.values[ai.currIndex].key
}

// 当前遍历位置的Value数据
func (ai *artIterator) Value() *data.LogRecordPos {
	return ai.values[ai.currIndex].pos
}

// 关闭迭代器，释放相应数据   主要是将临时的数组清理掉
func (ai *artIterator) Close() {
	ai.values = nil
}
