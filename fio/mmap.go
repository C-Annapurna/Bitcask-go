package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

// MMap IO  内存文件映射
type MMap struct {
	readerAt *mmap.ReaderAt //go语言官方的mmap包只能实现读取数据
}

// 初始化MMap IO
func NewMMapIOManager(fileName string) (*MMap, error) {
	_, err := os.OpenFile(fileName, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil
}

// 从文件给定位置读取对应的数据
func (mmap *MMap) Read(b []byte, offset int64) (int, error) {
	return mmap.readerAt.ReadAt(b, offset)
}

// 写入字节数组到文件中
func (mmap *MMap) Write([]byte) (int, error) {
	panic("not implemented")
}

// 将内存缓冲区的文件数据持久化到磁盘中
func (mmap *MMap) Sync() error {
	panic("not implemented")
}

// 关闭文件
func (mmap *MMap) Close() error {
	return mmap.readerAt.Close()
}

// 获取到文件大小
func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}
