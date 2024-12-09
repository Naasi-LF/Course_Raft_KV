package kv

import (
	"course/labrpc"
	"encoding/json"
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

type KVClient struct {
	servers  []*labrpc.ClientEnd
	clientID int64
	seqNum   int64
	leaderID int
}

func MakeKVClient(servers []*labrpc.ClientEnd) *KVClient {
	ck := &KVClient{
		servers:  servers,
		clientID: rand.Int63(),
		leaderID: 0,
	}
	return ck
}

func (ck *KVClient) Get(key string) string {
	args := &GetArgs{
		Key: key,
	}

	log.Printf("Client %d: Starting Get request for key=%s", ck.clientID, key)

	for retries := 0; retries < 5; retries++ {
		server := ck.servers[ck.leaderID]
		log.Printf("Client %d: Sending Get request for key=%s to server %d (attempt %d)", ck.clientID, key, ck.leaderID, retries+1)

		var reply GetReply
		ok := server.Call("KVServer.Get", args, &reply)

		if ok {
			if reply.Err == "Key not found" {
				log.Printf("Client %d: Get key=%s not found on server %d", ck.clientID, key, ck.leaderID)
				return "" // 返回空值表示 key 不存在
			} else if reply.Err == "" {
				// 将 KVEntry 转换为 JSON 字符串返回
				value, _ := json.Marshal(reply.Value)
				log.Printf("Client %d: Get key=%s succeeded on server %d", ck.clientID, key, ck.leaderID)
				return string(value)
			} else if reply.Err == ErrWrongLeader {
				log.Printf("Client %d: Wrong leader on server %d, switching to server %d", ck.clientID, ck.leaderID, (ck.leaderID+1)%len(ck.servers))
				ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			}
		} else {
			log.Printf("Client %d: Get key=%s failed on server %d, retrying...", ck.clientID, key, ck.leaderID)
		}

		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("Client %d: Get key=%s failed after retries", ck.clientID, key)
	return ""
}

func (ck *KVClient) Put(key string, value KVEntry) {
	args := &PutArgs{
		Key:      key,
		Value:    value,
		ClientID: ck.clientID,
		SeqNum:   int(atomic.AddInt64(&ck.seqNum, 1)),
	}

	for retries := 0; retries < 5; retries++ {
		server := ck.servers[ck.leaderID]
		var reply PutReply
		ok := server.Call("KVServer.Put", args, &reply)
		if ok && reply.Err == "" {
			log.Printf("Client %d: Put key=%s value=%+v succeeded on leader %d", ck.clientID, key, value, ck.leaderID)
			return
		} else if ok && reply.Err == ErrWrongLeader {
			log.Printf("Client %d: Wrong leader, switching from %d to %d", ck.clientID, ck.leaderID, (ck.leaderID+1)%len(ck.servers))
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		}

		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("Client %d: Put key=%s value=%+v failed after retries", ck.clientID, key, value)
}

// 新增 GetAllKeys 方法
func (ck *KVClient) GetAllKeys() []string {
	args := &GetAllKeysArgs{}

	log.Printf("Client %d: Starting GetAllKeys request", ck.clientID)

	for retries := 0; retries < 5; retries++ {
		server := ck.servers[ck.leaderID]
		var reply GetAllKeysReply
		ok := server.Call("KVServer.GetAllKeys", args, &reply)

		if ok {
			if reply.Err == "" {
				log.Printf("Client %d: GetAllKeys succeeded on server %d", ck.clientID, ck.leaderID)
				return reply.Keys // 返回 string 类型的键列表
			} else if reply.Err == ErrWrongLeader {
				log.Printf("Client %d: Wrong leader on server %d, switching to server %d", ck.clientID, ck.leaderID, (ck.leaderID+1)%len(ck.servers))
				ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			}
		} else {
			log.Printf("Client %d: GetAllKeys failed on server %d, retrying...", ck.clientID, ck.leaderID)
		}

		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("Client %d: GetAllKeys failed after retries", ck.clientID)
	return []string{}
}
