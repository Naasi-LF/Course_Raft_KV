package kv

// 操作类型常量
const (
	OpGet = "Get"
	OpPut = "Put"
)

// 操作结构体，用于封装客户端请求
type Op struct {
	Type     string
	Key      string
	Value    KVEntry
	ClientID int64
	SeqNum   int
}

// Get 请求参数
type GetArgs struct {
	Key string
}

// Get 回复参数
type GetReply struct {
	Value KVEntry
	Err   string
}

// Put 请求参数
type PutArgs struct {
	Key      string
	Value    KVEntry
	ClientID int64
	SeqNum   int
}

// Put 回复参数
type PutReply struct {
	Err string
}

// 错误信息常量
const (
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

// KVEntry 定义存储的数据结构
type KVEntry struct {
	Grand        int     `json:"grand"`
	Class        string  `json:"class"`
	Major        string  `json:"major"`
	Name         string  `json:"name"`
	CourseCount  int     `json:"course_count"`
	TotalCredits float64 `json:"total_credits"`
}

// GetAllKeys 请求参数
type GetAllKeysArgs struct{}

// GetAllKeys 回复参数
type GetAllKeysReply struct {
	Keys []string
	Err  string
}
