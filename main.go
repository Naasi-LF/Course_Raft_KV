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
	go simulateFaults(kvServers)
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

// 启动 HTTP 服务
func startHTTPServer() {
	http.HandleFunc("/put", handlePut)
	http.HandleFunc("/get", handleGet)
	http.HandleFunc("/get_field", handleGetField) // 新增：单字段查询接口
	http.HandleFunc("/search", handleSearch)      // 新增：条件查询接口

	log.Println("HTTP server is running on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// 处理 /put 请求
func handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// 定义一个结构体匹配请求体的 JSON 格式
	var request struct {
		Key   string     `json:"key"`
		Value kv.KVEntry `json:"value"` // 修改 Value 类型为 KVEntry
	}

	// 解析请求体
	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, "Failed to parse request body", http.StatusBadRequest)
		return
	}

	// 调用 client.Put，传入 KVEntry 类型的 Value
	client.Put(request.Key, request.Value)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Put operation successful for key: %s", request.Key)
}

// 处理 /get 请求
func handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	value := client.Get(key)
	if value == "" {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Value for key '%s': %s", key, value)
}

// 处理 /get_field 请求
func handleGetField(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// 获取查询参数
	key := r.URL.Query().Get("key")
	field := r.URL.Query().Get("field")
	if key == "" || field == "" {
		http.Error(w, "Key and field are required", http.StatusBadRequest)
		return
	}

	// 获取完整记录
	jsonValue := client.Get(key)
	if jsonValue == "" {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	// 解析 JSON 数据
	var record kv.KVEntry
	err := json.Unmarshal([]byte(jsonValue), &record)
	if err != nil {
		http.Error(w, "Failed to parse record", http.StatusInternalServerError)
		return
	}

	// 使用反射或者直接匹配字段
	var result interface{}
	switch field {
	case "grand":
		result = record.Grand
	case "class":
		result = record.Class
	case "major":
		result = record.Major
	case "name":
		result = record.Name
	case "course_count":
		result = record.CourseCount
	case "total_credits":
		result = record.TotalCredits
	default:
		http.Error(w, "Invalid field", http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "%v", result)
}

// 处理 /search 请求
func handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// 获取查询条件
	query := r.URL.Query()
	major := query.Get("major")
	class := query.Get("class")
	grand := query.Get("grand")
	totalCredits := query.Get("total_credits") // total_credits 查询条件

	if major == "" && class == "" && grand == "" && totalCredits == "" {
		http.Error(w, "At least one condition is required", http.StatusBadRequest)
		return
	}

	// 从客户端获取所有键
	keys := client.GetAllKeys()

	// 结果列表
	results := []map[string]string{}

	// 遍历每个键
	for _, key := range keys {
		jsonValue := client.Get(key)
		if jsonValue == "" {
			continue
		}

		// 解析 JSON 数据
		var record kv.KVEntry
		err := json.Unmarshal([]byte(jsonValue), &record)
		if err != nil {
			continue
		}

		// 检查条件
		matches := true
		if major != "" && record.Major != major {
			matches = false
		}
		if class != "" && record.Class != class {
			matches = false
		}
		if grand != "" && strconv.Itoa(record.Grand) != grand {
			matches = false
		}
		if totalCredits != "" {
			// 将 total_credits 转换为浮点数进行比较
			queryCredits, err := strconv.ParseFloat(totalCredits, 64)
			if err != nil || record.TotalCredits != queryCredits {
				matches = false
			}
		}

		// 如果条件匹配，加入结果
		if matches {
			results = append(results, map[string]string{
				"id":   key,
				"name": record.Name,
			})
		}
	}

	// 返回结果
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

					time.Sleep(500 * time.Millisecond)
				}
			}
		}(serverIndex, done)

		// 休眠 10 秒后恢复服务器
		time.Sleep(5 * time.Second)

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
