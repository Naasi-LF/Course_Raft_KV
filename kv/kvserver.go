package kv

import (
	"course/labrpc"
	"course/raft"
	"encoding/gob"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type KVServer struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	data      map[string]KVEntry
	notifyCh  map[int]chan Op
	clientSeq map[int64]int
	dead      int32

	peers []*labrpc.ClientEnd
}

func StartKVServer(peers []*labrpc.ClientEnd, me int, persister *raft.Persister) *KVServer {
	gob.Register(Op{})
	gob.Register(KVEntry{})

	kv := &KVServer{
		me:        me,
		applyCh:   make(chan raft.ApplyMsg),
		data:      make(map[string]KVEntry),
		notifyCh:  make(map[int]chan Op),
		clientSeq: make(map[int64]int),
		peers:     peers,
	}
	kv.rf = raft.Make(peers, me, persister, kv.applyCh)
	kv.loadData()
	go kv.applyLoop()
	return kv
}

func (kv *KVServer) GetRaft() *raft.Raft {
	return kv.rf
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	log.Printf("Server %d: Killed", kv.me)
}

func (kv *KVServer) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

func (kv *KVServer) applyLoop() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			command := msg.Command.(Op)
			kv.mu.Lock()

			if command.Type == OpPut {
				kv.data[command.Key] = command.Value // 修正为 KVEntry 类型
				kv.clientSeq[command.ClientID] = command.SeqNum
				kv.SaveData()
			}

			if ch, ok := kv.notifyCh[msg.CommandIndex]; ok {
				ch <- command
				delete(kv.notifyCh, msg.CommandIndex)
			}

			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, exists := kv.data[args.Key]
	if exists {
		reply.Value = value
		reply.Err = ""
	} else {
		reply.Err = ErrNoKey
	}
}

func (kv *KVServer) Put(args *PutArgs, reply *PutReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Type:     OpPut,
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		SeqNum:   args.SeqNum,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan Op, 1)
	kv.notifyCh[index] = ch
	kv.mu.Unlock()

	select {
	case <-ch:
		reply.Err = ""
	case <-time.After(1 * time.Second):
		reply.Err = ErrTimeout
	}

	kv.mu.Lock()
	delete(kv.notifyCh, index)
	kv.mu.Unlock()
}
func (kv *KVServer) GetAllKeys(args *GetAllKeysArgs, reply *GetAllKeysReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var keys []string
	for key := range kv.data { // 确保 key 是 string 类型
		keys = append(keys, key)
	}

	reply.Keys = keys
	reply.Err = ""
}
func (kv *KVServer) GetPeers() []*labrpc.ClientEnd {
	return kv.peers
}

func (kv *KVServer) GetMe() int {
	return kv.me
}
