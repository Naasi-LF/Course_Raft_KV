package kv

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"sync"
)

var fileMutex sync.Mutex

func (kv *KVServer) persistData() {
	fileMutex.Lock()
	defer fileMutex.Unlock()

	kv.mu.Lock()
	data, err := json.MarshalIndent(kv.data, "", "    ")
	kv.mu.Unlock()

	if err != nil {
		log.Printf("Server %d: Failed to marshal data: %v", kv.me, err)
		return
	}

	err = ioutil.WriteFile("data_kv.json", data, 0644)
	if err != nil {
		log.Printf("Server %d: Failed to write data to file: %v", kv.me, err)
	} else {
		log.Printf("Server %d: Data persisted to data_kv.json", kv.me)
	}
}

func (kv *KVServer) SaveData() {
	go kv.persistData() // 调用现有的 persistData 方法
}

func (kv *KVServer) loadData() {
	fileMutex.Lock()
	defer fileMutex.Unlock()

	data, err := ioutil.ReadFile("data_kv.json")
	if err != nil {
		log.Printf("Server %d: Failed to read data_kv.json: %v", kv.me, err)
		return // 文件可能首次不存在，直接返回
	}

	var loadedData map[string]KVEntry
	err = json.Unmarshal(data, &loadedData)
	if err != nil {
		log.Printf("Server %d: Failed to parse data_kv.json: %v", kv.me, err)
		return
	}

	kv.mu.Lock()
	kv.data = loadedData // 更新内存中的数据
	kv.mu.Unlock()
	log.Printf("Server %d: Data loaded from data_kv.json", kv.me)
}
