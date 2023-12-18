package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

// 获取测试时使用的key
func GetTestKey(i int) []byte {
	result := []byte(fmt.Sprintf("bitcask-go-key-%09d", i))
	return result //举例，假如这个i为8714548051793787397，这里就相当于将bitcask-go-key-8714548051793787397使用ascii码转化。
}

// 生成随机的value，用于测试   输入参数n由于表示随机的value的大小
func RandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	return []byte("bitcask-go-key" + string(b))
}
