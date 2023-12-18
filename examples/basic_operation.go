package main

import (
	bitcask "bitcask-go"
	"fmt"
)

func main() {
	opts := bitcask.DefaultOptioins
	db, err := bitcask.OpneDB(opts)
	if err != nil {
		panic(err)
	}
	err = db.Put([]byte("name"), []byte("bitcask_go_project"))
	if err != nil {
		panic(err)
	}
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(val))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
