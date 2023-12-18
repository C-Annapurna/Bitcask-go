package main

import (
	bitcask "bitcask-go"
	bitcask_redis "bitcask-go/redis"
	"bitcask-go/utils"
	"fmt"
	"github.com/tidwall/redcon"
	"strings"
)

func newWrongNumberOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}

type cmdHandler func(cli *BitcaskClient, args [][]byte) (interface{}, error)

var supportCommands = map[string]cmdHandler{
	"set":   set,
	"get":   get,
	"hset":  hset,
	"sadd":  sadd,
	"lpush": lpush,
	"zadd":  zadd,
}

type BitcaskClient struct {
	server *BitcaskServer
	db     *bitcask_redis.RedisDataStructure
}

// 处理传递的各种命令，对命令进行解析并返回对应的结果
func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))
	cmdFunc, ok := supportCommands[command]
	if !ok {
		conn.WriteError("Err unsupported command:'" + command + "'")
		return
	}

	client, _ := conn.Context().(*BitcaskClient)
	switch command {
	case "quit":
		_ = conn.Close()
	case "ping":
		conn.WriteString("PONG")
	default:
		res, err := cmdFunc(client, cmd.Args[1:])
		if err != nil {
			if err == bitcask.ErrKeyNotFound {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		conn.WriteAny(res)

	}
}

// set命令
func set(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("set")
	}

	//比如set a 100
	key, value := args[0], args[1]
	if err := cli.db.Set(key, 0, value); err != nil {
		return nil, err
	}

	return redcon.SimpleString("OK"), nil
}

// get命令
func get(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("get")
	}

	//调用存储引擎
	value, err := cli.db.Get(args[0])
	if err != nil {
		return nil, err
	}
	return value, nil
}

func hset(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("hset")
	}

	var ok = 0
	key, field, value := args[0], args[1], args[2]
	res, err := cli.db.HSet(key, field, value)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func sadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("sadd")
	}

	var ok = 0
	key, member := args[0], args[1]
	res, err := cli.db.SAdd(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func lpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("lpush")
	}
	key, value := args[0], args[1]
	res, err := cli.db.LPush(key, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(res), nil
}

func zadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("zadd")
	}

	var ok = 0
	//zadd传递三个参数，key  score  member
	key, score, member := args[0], args[1], args[2]
	res, err := cli.db.ZAdd(key, utils.FloatFromBytes(score), member)

	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

//还有很多其他的方法吗，可以进行补全。主要就是做一下转换，这样就能支持redis的任何命令
