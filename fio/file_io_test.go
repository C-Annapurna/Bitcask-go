package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destroyFile(name string) { //新建的文件记得要清理掉   同时注意，在进行destroy这个函数之前，请确保打开的name文件已被close，不然无法关闭文件
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileOPManager(t *testing.T) {
	path := filepath.Join("G:\\GO_Project\\kv_project\\tmp", "a.data")
	fio, err := NewFileOPManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("G:\\GO_Project\\kv_project\\tmp", "a.data")
	fio, err := NewFileOPManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("bitcask kv"))
	//t.Log(n, err)
	assert.Equal(t, 10, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("storage"))
	//t.Log(n, err)
	assert.Equal(t, 7, n)
	assert.Nil(t, err)

	err = fio.Close()
	assert.Nil(t, err)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("G:\\GO_Project\\kv_project\\tmp", "a.data")
	fio, err := NewFileOPManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	b := make([]byte, 5)     //创造一个长度为5的字节数组
	n, err := fio.Read(b, 0) //从0的位置读取了b个字节(5)    此时数据存放在b数组中
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b)

	b2 := make([]byte, 5)
	n, err = fio.Read(b2, 5)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-b"), b2)

	err = fio.Close()
	assert.Nil(t, err)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("G:\\GO_Project\\kv_project\\tmp", "a.data")
	fio, err := NewFileOPManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)

	err = fio.Close()
	assert.Nil(t, err)

}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("G:\\GO_Project\\kv_project\\tmp", "a.data")
	fio, err := NewFileOPManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close() //试试重复close
	assert.Nil(t, err)
}
