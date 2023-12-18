package benchmark

import (
	bitcask "bitcask-go"
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
	"time"
)

// 基准测试，测试吞吐量；响应时间；并发量

var db *bitcask.DB

func init() {
	//初始化用于基准测试的存储引擎
	options := bitcask.DefaultOptioins
	dir, _ := os.MkdirTemp("", "bitcask-go-bench")
	options.DirPath = dir

	var err error
	db, err = bitcask.OpenDB(options)
	if err != nil {
		panic(err)
	}
}

func Benchmark_Put(b *testing.B) { //区别于此前单元测试，这里使用的是一个*testing.B的结构体
	b.ResetTimer()   //重置计时器
	b.ReportAllocs() //报道每次操作的内存分配情况

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	//测试get数据的性能，那就得先往数据库里放好数据吧
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) { //区别于此前单元测试，这里使用的是一个*testing.B的结构体
	b.ResetTimer()   //重置计时器
	b.ReportAllocs() //报道每次操作的内存分配情况

	rand.Seed(time.Now().UnixNano())

	//运行整个bench_test.go文件时
	for i := 0; i < b.N; i++ {
		key := rand.Int()
		//err := db.Put(utils.GetTestKey(key), utils.RandomValue(1024))
		err := db.Delete(utils.GetTestKey(key))
		if err == bitcask.ErrKeyNotFound {
			continue
		} else {
			assert.Nil(b, err)
		}
	}
}
