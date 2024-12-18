package main

import (
	"course/kv"
	"course/labrpc"
	"course/raft"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

var client *kv.KVClient

func main() {
	// 创建一个网络
	network := labrpc.MakeNetwork()

	// 节点数量
	nServers := 3

	// 存储服务器的 ClientEnd
	servers := make([]*labrpc.ClientEnd, nServers)

	// 存储 KVServer 实例
	kvServers := make([]*kv.KVServer, nServers)

	// 创建所有 ClientEnd
	for i := 0; i < nServers; i++ {
		serverName := "server" + strconv.Itoa(i)
		clientEnd := network.MakeEnd("ClientEnd" + serverName)
		servers[i] = clientEnd
	}

	// 创建所有 KVServer 实例
	for i := 0; i < nServers; i++ {
		serverName := "server" + strconv.Itoa(i)

		// 创建服务器
		server := labrpc.MakeServer()

		// 创建持久化实例
		persister := raft.MakePersister()

		// 创建 KVServer 实例
		kvs := kv.StartKVServer(servers, i, persister)

		// 将 KVServer 注册为服务
		kvService := labrpc.MakeService(kvs)
		server.AddService(kvService)

		// 创建并注册 Raft 实例
		raftService := labrpc.MakeService(kvs.GetRaft())
		server.AddService(raftService)

		// 将服务器添加到网络
		network.AddServer(serverName, server)

		// 连接网络
		network.Connect("ClientEnd"+serverName, serverName)
		network.Enable("ClientEnd"+serverName, true)

		// 保存 KVServer 实例
		kvServers[i] = kvs
	}

	// 创建客户端
	clientEnds := make([]*labrpc.ClientEnd, nServers)
	for i := 0; i < nServers; i++ {
		serverName := "server" + strconv.Itoa(i)
		clientEndName := "Client" + serverName

		// 创建一个 ClientEnd
		clientEnd := network.MakeEnd(clientEndName)
		network.Connect(clientEndName, serverName)
		network.Enable(clientEndName, true)

		clientEnds[i] = clientEnd
	}

	// 创建 KVClient
	client = kv.MakeKVClient(clientEnds)

	// 启动 HTTP 服务
	go startHTTPServer()
	// 启动模拟故障的 Goroutine
	// go simulateFaults(kvServers)
	// 捕获中断信号
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	log.Println("Server is running. Press Ctrl+C to stop.")

	// 阻塞等待中断信号
	<-signalChan

	// 清理工作
	log.Println("Shutting down servers...")
	for _, kvs := range kvServers {
		kvs.Kill()
	}

	// 清理网络
	network.Cleanup()

	log.Println("Server stopped.")
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 设置允许的来源，* 表示允许所有来源
		w.Header().Set("Access-Control-Allow-Origin", "*")
		// 设置允许的请求方法
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		// 设置允许的请求头
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		// 处理预检请求（OPTIONS）
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// 启动 HTTP 服务
func startHTTPServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/put", handlePut)
	mux.HandleFunc("/get", handleGet)
	mux.HandleFunc("/search", handleSearch)
	mux.HandleFunc("/list_all", handleListAll)

	// 使用跨域中间件
	log.Println("HTTP server is running on :8080")
	log.Fatal(http.ListenAndServe(":8080", cors(mux)))
}

// 处理 /put 请求
func handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error": "Invalid request method"}`, http.StatusMethodNotAllowed)
		return
	}

	var request struct {
		Key   string     `json:"key"`
		Value kv.KVEntry `json:"value"`
	}

	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, `{"error": "Failed to parse request body"}`, http.StatusBadRequest)
		return
	}

	client.Put(request.Key, request.Value)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"message": fmt.Sprintf("Put operation successful for key: %s", request.Key),
	})
}

// 处理 /get 请求
func handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error": "Invalid request method"}`, http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, `{"error": "Key is required"}`, http.StatusBadRequest)
		return
	}

	jsonValue := client.Get(key)
	if jsonValue == "" {
		http.Error(w, `{"error": "Key not found"}`, http.StatusNotFound)
		return
	}

	// 将 JSON 字符串解析为结构化对象
	var record map[string]interface{}
	err := json.Unmarshal([]byte(jsonValue), &record)
	if err != nil {
		http.Error(w, `{"error": "Failed to parse record"}`, http.StatusInternalServerError)
		return
	}

	// 添加 key 字段到记录中
	record["id"] = key

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(record)
}

// 处理 /search 请求
func handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error": "Invalid request method"}`, http.StatusMethodNotAllowed)
		return
	}

	// 获取查询条件
	query := r.URL.Query()
	name := query.Get("name")                  // 查询姓名
	class := query.Get("class")                // 查询班级
	major := query.Get("major")                // 查询专业
	grand := query.Get("grand")                // 查询年级
	courseCount := query.Get("course_count")   // 查询课程数
	totalCredits := query.Get("total_credits") // 查询总学分

	if name == "" && class == "" && major == "" && grand == "" && courseCount == "" && totalCredits == "" {
		http.Error(w, `{"error": "At least one condition is required"}`, http.StatusBadRequest)
		return
	}

	// 从客户端获取所有键
	keys := client.GetAllKeys()

	// 结果列表
	var results []map[string]interface{}

	// 遍历每个键
	for _, key := range keys {
		jsonValue := client.Get(key)
		if jsonValue == "" {
			continue
		}

		// 解析 JSON 数据
		var record map[string]interface{}
		err := json.Unmarshal([]byte(jsonValue), &record)
		if err != nil {
			continue
		}

		// 检查条件
		matches := true
		if name != "" && record["name"] != name {
			matches = false
		}
		if class != "" && record["class"] != class {
			matches = false
		}
		if major != "" && record["major"] != major {
			matches = false
		}
		if grand != "" && strconv.Itoa(int(record["grand"].(float64))) != grand {
			matches = false
		}
		if courseCount != "" {
			queryCourseCount, err := strconv.Atoi(courseCount)
			if err != nil || int(record["course_count"].(float64)) != queryCourseCount {
				matches = false
			}
		}
		if totalCredits != "" {
			queryCredits, err := strconv.ParseFloat(totalCredits, 64)
			if err != nil || record["total_credits"] != queryCredits {
				matches = false
			}
		}

		// 如果条件匹配，加入结果
		if matches {
			record["id"] = key // 添加键作为 `id` 字段
			results = append(results, record)
		}
	}

	// 设置响应头为 JSON 格式
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// 处理 /list_all 请求
func handleListAll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error": "Invalid request method"}`, http.StatusMethodNotAllowed)
		return
	}

	// 从客户端获取所有键
	keys := client.GetAllKeys()

	// 存储所有学生信息的列表
	var results []map[string]interface{}

	// 遍历每个键
	for _, key := range keys {
		jsonValue := client.Get(key)
		if jsonValue == "" {
			continue
		}

		// 将 JSON 数据解析为结构化对象
		var record map[string]interface{}
		err := json.Unmarshal([]byte(jsonValue), &record)
		if err != nil {
			continue
		}

		// 添加键到结果中
		record["id"] = key
		results = append(results, record)
	}

	// 设置响应头为 JSON 格式
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// 模拟故障
func simulateFaults(kvServers []*kv.KVServer) {
	rand.Seed(time.Now().UnixNano()) // 初始化随机种子

	for {
		time.Sleep(10 * time.Second) // 每隔 10 秒模拟一次故障

		// 随机选择一个服务器
		serverIndex := rand.Intn(len(kvServers))
		log.Printf("[Fault] Simulating failure on server %d", serverIndex)

		// 模拟杀死服务器
		kvServers[serverIndex].Kill()

		// 启动一个 Goroutine 持续打印状态
		done := make(chan bool)
		go func(index int, done chan bool) {
			for {
				select {
				case <-done:
					return
				default:
					log.Printf("\033[31m💀💀💀💀[Fault] Server %d is down💀💀💀💀\033[0m", index)

					time.Sleep(100 * time.Millisecond)
				}
			}
		}(serverIndex, done)

		// 休眠 10 秒后恢复服务器
		// time.Sleep(5 * time.Second)

		log.Printf("🩺🩺🩺[Fault] Recovering server %d🩺🩺🩺", serverIndex)
		persister := raft.MakePersister()
		newServer := kv.StartKVServer(kvServers[serverIndex].GetPeers(), kvServers[serverIndex].GetMe(), persister)
		kvServers[serverIndex] = newServer

		// 通知打印 Goroutine 停止
		done <- true
		close(done)

		log.Printf("👩‍⚕️👩‍⚕️👩‍⚕️[Fault] Server %d recovered👩‍⚕️👩‍⚕️👩‍⚕️", serverIndex)
	}
}
