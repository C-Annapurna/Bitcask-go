package fio

import "os"

// 使用标准系统文件IO来实现IOManager接口
type FileIO struct {
	//使用go语言提供的一些文件接口进行封装
	fd *os.File //系统文件描述符     这里指向的文件是位于磁盘上的，而我们实现的存储数据库就是将文件存储在磁盘上的
}

func NewFileOPManager(fileName string) (*FileIO, error) {
	fil, err := os.OpenFile( //如果该文件不存在则会创建对应的文件
		fileName,
		os.O_CREATE|os.O_APPEND, //os.O_APPEND表示我们的文件是只支持追加写入的
		DataFilePerm,
	)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fil}, nil
}

// 从文件给定位置读取对应的数据
func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

// 写入字节数组到文件中
func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

// 将内存缓冲区的文件数据持久化到磁盘中
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

// 关闭文件
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
