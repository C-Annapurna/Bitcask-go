package main

import (
	bitcask "bitcask-go"
	bitcask_redis "bitcask-go/redis"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

type BitcaskServer struct {
	dbs    map[int]*bitcask_redis.RedisDataStructure //可以使用多个db向同一个服务端发起请求
	server *redcon.Server
	mu     sync.Mutex
}

const addr = "127.0.0.1:6380"

func main() {
	//打开redis数据结构服务
	redisDataStructure, err := bitcask_redis.NewRedisDataStructure(bitcask.DefaultOptioins)
	if err != nil {
		panic(err)
	}

	//初始化一个BitcaskServer
	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*bitcask_redis.RedisDataStructure),
	}
	bitcaskServer.dbs[0] = redisDataStructure

	//初始化一个服务器 redis server端
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.close)

	bitcaskServer.listen()

}

func (svr *BitcaskServer) listen() {
	log.Println("bitcask server running,ready to accept connectioins.")
	_ = svr.server.ListenAndServe()
}

func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	cli := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.db = svr.dbs[0]
	cli.server = svr
	conn.SetContext(cli)
	return true
}

func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		db.Close()
	}
	_ = svr.server.Close()
}

//func main() {
//	conn, err := net.Dial("tcp", "localhost:6379")
//	if err != nil {
//		panic(err)
//	}
//
//	//向redis发送一个命令
//	cmd := "set k-name bitcask-kv\r\n"
//	conn.Write([]byte(cmd))
//
//	//解析redis响应
//	reader := bufio.NewReader(conn)
//	res, err := reader.ReadString('\n')
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println(res)
//}
