package data

import (
	"encoding/binary"
	"hash/crc32"
)

//相对于data_file.go文件，这一段更多倾向于对内存中的索引结构的管理

type LogRecordType = byte

const (
	LogRecordNormal  LogRecordType = iota //表示文件是正常的，没被删除
	LogRecordDeleted                      //表示文件已被删除
	LogRecordTxnFinished
)

const (
	maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5
)

// 写入到数据文件的记录   包含键值对，已经墓碑值
// 之所以叫日志，是因为数据文件中的数据是追加写入的，类似于日志的格式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType //这是一个墓碑值，表示当前文件是否被删除
}

// LogRecord的头部信息
type LogRecordHeader struct {
	crc        uint32        //crc校验值
	recordType LogRecordType //表示logrecord的类型
	keySize    uint32        //key的长度
	valueSize  uint32        //value的长度
}

type LogRecordPos struct { //这个是存放在内存索引结构上的，用于指示文件位于磁盘上的哪个位置
	Fid    uint32 //文件id，表示数据存储到哪个文件当中
	Offset int64  //表示该数据存放在了文件的哪个位置
	Size   uint32 //表示数据在磁盘上的大小
}

// 暂存事务相关信息    要注意大小写，控制对外是否隐藏
type TransactionReocrd struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// 对LogRecord进行编码，返回字符数组及长度
// 需要将传入的logrecord添加上header信息，转化为字节数组返回。    后续会将header+kv一起放置在活跃文件中
// 编码之后的结构：
//
//		+-----------+---------------+---------------+---------------+-----------+---------------+
//		|crc 校验值	|	type类型		|	keysize		|	valuesize	|	key		|	value		|
//		+-----------+---------------+---------------+---------------+-----------+---------------+
//	      4字节		1字节					变长，最大为5字节			变长			变长
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	//初始化一个header信息
	header := make([]byte, maxLogRecordHeaderSize)
	//crc校验值是最后进行存储的，先解决后面几个字段

	//从第五个字节开始存储
	header[4] = logRecord.Type
	var index = 5
	//5字节之后，存储的是key和value的长度信息
	//使用变长类型，节省空间
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key))) //使用了binary的库函数PutVarint：将整数进行变长编码并写入字节切片中
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))
	//此时header已经写完了，此时可能header总长度并没有达到maxLogRecordHeaderSize

	var size = index + len(logRecord.Key) + len(logRecord.Value) //这里就表示了整个编码的长度
	encBytes := make([]byte, size)
	//这个encBytes将存放除了crc以外的数据
	copy(encBytes[:index], header[:index])
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	//这样就将除了crc所有的内容都保存在encBytes中了，接下来就是进行crc校验码的生成
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc) //主流的平台(arm,x86)一般都是支持小端序，所以我们使用LittleEndian

	return encBytes, int64(size)
}

// 对位置信息进行编码
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	index += binary.PutVarint(buf[index:], int64(pos.Size))
	return buf[:index]
}

// 对位置信息进行解码    返回的结果就包括1、数据文件的id；2、对应record在数据文件上的偏移
func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fileId, n := binary.Varint(buf[index:])
	index += n
	offset, n := binary.Varint(buf[index:])
	index += n
	size, _ := binary.Varint(buf[index:])

	return &LogRecordPos{
		Fid:    uint32(fileId),
		Offset: offset,
		Size:   uint32(size),
	}
}

//注意上方的encode部分和下面的decode部分，对于keysize和valuesize转化成字节流时，是作为两个元素转换进去的
//所以下面使用Varint进行解码的时候，可以分别解码出keysize和valuesize

// 从buf字节数组中解码中header的信息(仅包含header信息)   也就是从header的字节流之中提取出提取出有效的信息(header包括的信息：crc   type    keysize    valuesize)
func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 { //这里表示传入的buf连crc四个字节的要求都没有达到
		return nil, 0
	}

	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]), //这是将长度为4字节的字节切片转换为小端序的无符号32位整数
		recordType: buf[4],                              //记录记录的类型
	}

	var index = 5
	//取出实际的key size
	keysize, n := binary.Varint(buf[index:]) //由于在binary.PutVarint时，keysize和valuesize是分别放入的，所以这一次varint只会得到keysize的内容
	header.keySize = uint32(keysize)
	index += n

	valuesize, n := binary.Varint(buf[index:]) //这一次也就只会得到valuesize的内容
	header.valueSize = uint32(valuesize)
	index += n

	return header, int64(index) //将header信息返回，并且返回当前header的大小
}

// 根据输入的logrecord和header，得到crc值    这里的crc的计算是需要输出header部分以及key、value的
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])               //直接使用函数先计算得到kaysize和valuesize对应的crc32校验和
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)   // Update returns the result of adding the bytes in p to the crc.
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value) //Update就是根据新传入的字节更新crc的值

	return crc
}
