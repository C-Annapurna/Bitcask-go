package fio

//这里涉及的就是对磁盘上的数据进行处理

const DataFilePerm = 0644

type FileIOType = byte

const (
	//标准文件IO
	StanderdFIO FileIOType = iota

	//mmap内存文件映射
	MemoryMap
)

// 抽象的io管理接口，可以接入不同的io类型，目前支持标准文件io
// 我们在file_io.go文件中根据go提供的接口，实现了下面的四个方法，并通过了测试
// 后续可能还会实现其他的IO类型，比如MMap
type IOManager interface {
	//需要实现的方法：基本的数据读写   关闭数据文件的接口

	//从文件给定位置读取对应的数据
	Read([]byte, int64) (int, error)

	//写入字节数组到文件中
	Write([]byte) (int, error)

	//将内存缓冲区的文件数据持久化到磁盘中
	Sync() error

	//关闭文件
	Close() error

	//获取到文件大小
	Size() (int64, error)
}

// 初始化IOManager，目前只支持标准FileIO
// 后续如果实现了新的IO方法，可以在下面增加一个判断来选择不同的io类型
func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StanderdFIO:
		return NewFileOPManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("unsupported io type")
	}

}
