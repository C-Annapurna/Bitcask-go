package utils

import (
	"golang.org/x/sys/windows"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"unsafe"
)

// 获取一个目录的大小 用于计算是否达到了满足merge的阈值
func DirSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// 获取磁盘剩余可用空间大小
func AvailableDiskSize() (uint64, error) {
	wd, err := os.Getwd() //获得当前工作目录
	var freeBytesAvailableToCaller, totalNumberOfBytes, totalNumberOfFreeBytes uint64

	err = windows.GetDiskFreeSpaceEx(
		windows.StringToUTF16Ptr(wd),
		(*uint64)(unsafe.Pointer(&freeBytesAvailableToCaller)),
		(*uint64)(unsafe.Pointer(&totalNumberOfBytes)),
		(*uint64)(unsafe.Pointer(&totalNumberOfFreeBytes)),
	)

	if err != nil {
		return 0, err
	}

	return uint64(freeBytesAvailableToCaller), nil
}

// 拷贝数据目录
func CopyDir(src, dest string, exclude []string) error {
	//传入的参数：源路径，目标路径    排除的路径(文件锁的路径)
	//如果目标文件夹不存在的话，直接创建对应的目录
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		//目标路径不存在，直接创建
		if err := os.MkdirAll(dest, os.ModePerm); err != nil {
			return err
		}
	}

	//filepath.Walk用于递归遍历指定目录下的所有文件和子目录
	//其中第二个参数是一个匿名函数，表示对每一个文件或者目录的进行的处理
	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		fileName := strings.Replace(path, src, "", 1)
		if fileName == "" {
			return nil
		}
		for _, e := range exclude {
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}
		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dest, fileName), info.Mode())
		}

		data, err := os.ReadFile(filepath.Join(src, fileName))
		if err != nil {
			return nil
		}
		return os.WriteFile(filepath.Join(dest, fileName), data, info.Mode())

	})
}
