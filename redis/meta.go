package redis

import (
	"bitcask-go/utils"
	"encoding/binary"
	"math"
)

const (
	maxMetadataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32 //这里表示hash数据结构最大的长度是多少。根据下mainmetadata进行计算的
	extraListMetaSize = binary.MaxVarintLen64 * 2                           //这里表示List数据结构专用的内容，所需占用的最大空间

	initialListMark = math.MaxUint64 / 2 //针对list数据结构使用的常量，初始化是head和tail指向的位置
)

// 由于hash、set数据结构涉及到一些特殊的结构设计，所以专门开一个文件
type metadata struct {
	dataType byte   //表示数据类型
	expire   int64  //过期时间
	version  int64  //版本号，方便删除
	size     uint32 //数据量
	head     uint64 //list数据结构专用
	tail     uint64 //list数据结构专用
}

// 以下是关于元数据编解码的逻辑
func (md *metadata) encode() []byte {
	var size = maxMetadataSize
	if md.dataType == List { //List多两个字段，所以最大占用空间会多一点
		size += extraListMetaSize
	}
	buf := make([]byte, size)
	buf[0] = md.dataType
	var index = 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}
	return buf[:index]
}

func decodeMetadata(buf []byte) *metadata {
	dataType := buf[0]
	var index = 1
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	index += n

	var head uint64 = 0
	var tail uint64 = 0
	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, n = binary.Uvarint(buf[index:])
	}
	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}

type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

// hash的key是由key+version+field共同组成的，写这样一个编码的方法进行处理
func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+len(hk.field)+8) //hash结构下的key：key+field+version
	//key
	var index = 0
	copy(buf[index:index+len(hk.key)], hk.key)
	index += len(hk.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8

	//field
	copy(buf[index:], hk.field)

	return buf
}

type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

func (sk *setInternalKey) encode() []byte { //关于将set数据部分的编码实现
	buf := make([]byte, len(sk.key)+len(sk.member)+8+4) //这里再加4是因为set这边还需要加上member size字段
	//key
	var index = 0
	copy(buf[index:index+len(sk.key)], sk.key)
	index += len(sk.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(sk.version))
	index += 8

	//member
	copy(buf[index:index+len(sk.member)], sk.member)
	index += len(sk.member)

	//member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(sk.member)))

	return buf
}

type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

// 下面这一部分对应于list结构数据部分的编码过程
func (lk *listInternalKey) encode() []byte {
	buf := make([]byte, len(lk.key)+8+8) //由于version 64位;index也是64位.所以这两个都使用8个字节就能表示了
	//key
	var index = 0
	copy(buf[index:index+len(lk.key)], lk.key)
	index += len(lk.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lk.version))
	index += 8

	//index
	binary.LittleEndian.PutUint64(buf[index:], lk.index)

	return buf

}

type zsetInternalKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64
}

func (zk *zsetInternalKey) encodeWithMember() []byte {
	buf := make([]byte, len(zk.key)+8+8+len(zk.member))

	//key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	//member
	copy(buf[index:], zk.member)

	return buf
}

func (zk *zsetInternalKey) encodeWithScore() []byte {
	buf := make([]byte, len(zk.key)+8+8+len(zk.member))

	//key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	//score   这是float64的数字,我们需要定义一个方法将float64的数据转化为字节数组(binary中没有函数可以做到这一点)
	scoreBuf := utils.Float64ToBytes(zk.score)
	copy(buf[index:index+len(scoreBuf)], scoreBuf)
	index += len(scoreBuf)

	//member
	copy(buf[index:index+len(zk.member)], zk.member)
	index += len(zk.member)

	//最后面还要加上member的长度
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(zk.member)))

	return buf
}
