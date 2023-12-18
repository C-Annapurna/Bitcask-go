package main

import (
	bitcask "bitcask-go"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
)

var db *bitcask.DB

func init() {
	//初始化db实例
	var err error
	options := bitcask.DefaultOptioins
	dir, _ := os.MkdirTemp("", "bitcask-go-http")
	//options.DirPath = "G:\\GO_Project\\kv_project\\tmp\\http"
	options.DirPath = dir
	db, err = bitcask.OpenDB(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open db : %v", err))
	}
}

// 对于put方法的处理
func handlePut(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data map[string]string
	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer, err.Error(), http.StatusMethodNotAllowed)
		return
	}

	for key, value := range data {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to put value in db : %v", err)
			return
		}
	}
}

// 对于get方法的处理
func handleGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet { //对于get方法我们要求请求是get请求
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := request.URL.Query().Get("key") //我们从url中得到key
	value, err := db.Get([]byte(key))
	if err != nil && err != bitcask.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get value in db : %v", err)
		return
	}
	//运行到这一步表示已经拿到值了
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(string(value))
}

func handleDelete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodDelete {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := request.URL.Query().Get("key")
	err := db.Delete([]byte(key))
	if err != nil && err != bitcask.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to delete in db : %v", err)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("OK")
}

func handleListKeys(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	keys := db.ListKeys()
	writer.Header().Set("Content-Type", "application/json")
	var result []string
	for _, key := range keys {
		result = append(result, string(key))
	}
	_ = json.NewEncoder(writer).Encode(result)
}
func handleStat(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stat := db.Stat()
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(stat)
}

func main() {
	//注册处理方法
	http.HandleFunc("/bitcask/put", handlePut)
	http.HandleFunc("/bitcask/get", handleGet)
	http.HandleFunc("/bitcask/delete", handleDelete)
	http.HandleFunc("/bitcask/listkeys", handleListKeys)
	http.HandleFunc("/bitcask/stat", handleStat)

	//启动HTTP服务
	_ = http.ListenAndServe("localhost:5208", nil)
}
