package bitcask_go

// 用户在初始化数据库的时候的一些配置文件
type Option struct {
	DirPath string //数据路数据目录

	DataFileSize int64 //这一项指定了活跃文件最多能够存放多少个字节的数据

	SyncWrites bool //一个布尔值，是否每一次写入文件，都进行一次持久化操作

	BytesPerSync uint //累计写到多少字节后进行持久化

	IndexType IndexerType //指定内存索引的实现方式(btree or art)

	MMapAtStartup bool //配置项，是否在启动的时候使用mmap加载数据

	DataFileMergeRatio float32 //数据文件合并阈值
}

// 索引迭代器配置项
type IteratorOptions struct {
	Prefix  []byte //遍历前缀为指定值的key，默认为空
	Reverse bool   //是否进行反向遍历   默认为false
}

type WriteBatchOptions struct {
	//一个批次当中最大的数据量
	MaxBatchNum uint

	//提交时是否sync持久化
	SyncWrites bool
}

type IndexerType = int8

const (
	//BTree索引
	BTree IndexerType = iota + 1

	//自适应基数树索引
	ART

	//BPlusTree   B+树索引，将索引存储在磁盘上
	BPLusTree
)

var DefaultOptioins = Option{
	DirPath:            "G:\\GO_Project\\kv_project\\tmp\\bitcask",
	DataFileSize:       256 * 1024 * 1024,
	SyncWrites:         false,
	BytesPerSync:       0,
	IndexType:          BTree,
	MMapAtStartup:      true,
	DataFileMergeRatio: 0.5, //这里默认设置无效数据站总数据一半，我们就进行merge处理
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
