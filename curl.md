以下是所有可能的 `curl` 示例，覆盖你的接口功能。

---

### **1. 添加数据（/put）**

**请求：**

```bash
curl -X POST -H "Content-Type: application/json" \
-d '{
    "key": "21030108",
    "value": {
        "grand": 2021,
        "class": "21计一",
        "major": "计算机科学与技术(嵌入式)",
        "name": "杜雨菲",
        "course_count": 14,
        "total_credits": 28.5
    }
}' \
http://localhost:8080/put
```

**再添加另一条数据：**

```bash
curl -X POST -H "Content-Type: application/json" \
-d '{
    "key": "21030109",
    "value": {
        "grand": 2021,
        "class": "21计二",
        "major": "通信工程",
        "name": "张三",
        "course_count": 15,
        "total_credits": 30.0
    }
}' \
http://localhost:8080/put
```

---

### **2. 获取完整数据（/get）**

**请求：**

```bash
curl -X GET "http://localhost:8080/get?key=21030108"
```

**获取另一条记录：**

```bash
curl -X GET "http://localhost:8080/get?key=21030109"
```

---

### **3. 获取单个字段值（/get_field）**

**请求：**

- 获取学号 `21030108` 的专业：
  ```bash
  curl -X GET "http://localhost:8080/get_field?key=21030108&field=major"
  ```

- 获取学号 `21030109` 的班级：
  ```bash
  curl -X GET "http://localhost:8080/get_field?key=21030109&field=class"
  ```

- 获取学号 `21030108` 的总学分：
  ```bash
  curl -X GET "http://localhost:8080/get_field?key=21030108&field=total_credits"
  ```

---

### **4. 条件查询（/search）**

**查询某个专业的所有人：**

- 查询专业为 `计算机科学与技术(嵌入式)` 的学生：
  ```bash
  curl -X GET "http://localhost:8080/search?major=计算机科学与技术(嵌入式)"
  ```

- 查询专业为 `通信工程` 的学生：
  ```bash
  curl -X GET "http://localhost:8080/search?major=通信工程"
  ```

**查询某个班级的所有人：**

- 查询班级为 `21计一` 的学生：
  ```bash
  curl -X GET "http://localhost:8080/search?class=21计一"
  ```

- 查询班级为 `21计二` 的学生：
  ```bash
  curl -X GET "http://localhost:8080/search?class=21计二"

  curl -X GET "https://cathy.s7.tunnelfrp.com/search?class=23软件一"
  ```

**查询某个年级的所有人：**

- 查询年级为 `2021` 的学生：
  ```bash
  curl -X GET "http://localhost:8080/search?grand=2023"
  ```

**组合查询：**

- 查询专业为 `计算机科学与技术(嵌入式)` 且班级为 `21计一` 的学生：
  ```bash
  curl -X GET "http://localhost:8080/search?major=计算机科学与技术(嵌入式)&class=21计一"
  ```

- 查询专业为 `通信工程` 且年级为 `2021` 的学生：
  ```bash
  curl -X GET "http://localhost:8080/search?major=通信工程(嵌入式)&grand=2021"
  ```

---


### **6. 测试不存在的键或字段**

- 查询不存在的学号：
  ```bash
  curl -X GET "http://localhost:8080/get?key=99999999"
  ```

- 查询不存在的字段：
  ```bash
  curl -X GET "http://localhost:8080/get_field?key=21030108&field=invalid_field"
  ```
