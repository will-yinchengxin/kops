# Kafka 运维工具 - 配置查看功能说明

## 新增功能：config 命令

新增了 `config` 命令，用于查看 Kafka 的配置信息，支持查看 Topic 配置和 Broker 配置。

## 使用方法

### 1. 查看指定 Topic 的完整配置

```bash
./kops -cmd config -type topic -t my-topic
```

**输出示例：**
```
📋 Configuration for Topic: my-topic

CONFIG KEY                  VALUE                DESCRIPTION
cleanup.policy              delete               清理策略 (delete/compact)
compression.type            producer             压缩类型
max.message.bytes           1048588 (1.0M)       最大消息大小
min.insync.replicas         2                    最小同步副本数
retention.ms                604800000 (168h)     消息保留时间
segment.bytes               1073741824 (1.1G)    日志段大小
segment.ms                  604800000 (168h)     日志段滚动时间
```

**特点：**
- 显示所有配置项
- 自动格式化时间（毫秒转小时）
- 自动格式化存储大小（字节转可读单位）
- 提供中文配置说明

---

### 2. 查看所有 Topic 的关键配置摘要

```bash
./kops -cmd config -type topic
```

**输出示例：**
```
📋 Topic Configurations Summary (5 topics)

TOPIC           RETENTION   CLEANUP   COMPRESSION   MIN.ISR   MAX.MSG.SIZE
my-topic-1      168h        delete    producer      2         1.0M
my-topic-2      336h        compact   gzip          1         5.0M
my-topic-3      168h        delete    snappy        2         1.0M
test-topic      24h         delete    producer      1         1.0M
user-events     720h        delete    lz4           2         2.0M
```

**特点：**
- 只显示关键配置（retention、cleanup、compression 等）
- 表格化展示，便于对比
- 支持 filter 参数过滤 topic

---

### 3. 查看指定 Broker 的配置

```bash
./kops -cmd config -type broker -broker-id 1
```

**输出示例：**
```
🖥️  Broker 1 Configuration (kafka-broker-1:9092)

CONFIG KEY                      VALUE              DESCRIPTION
auto.create.topics.enable       false              自动创建主题
log.retention.hours             168                日志保留小时数
log.segment.bytes               1073741824 (1.1G)  日志段大小
num.io.threads                  8                  IO线程数
num.network.threads             3                  网络线程数
num.replica.fetchers            1                  副本拉取线程数
replica.lag.time.max.ms         30000              副本最大滞后时间
socket.receive.buffer.bytes     102400 (102.4k)    接收缓冲区大小
socket.send.buffer.bytes        102400 (102.4k)    发送缓冲区大小
```

---

### 4. 查看所有 Broker 的配置

```bash
./kops -cmd config -type broker
```

输出所有 Broker 的关键配置信息，每个 Broker 单独显示一个表格。

---

## 导出功能

### 导出为 JSON 格式

```bash
# 导出单个 Topic 配置
./kops -cmd config -type topic -t my-topic -export json

# 导出所有 Topic 配置
./kops -cmd config -type topic -export json > topics_config.json

# 导出 Broker 配置
./kops -cmd config -type broker -export json > brokers_config.json
```

**JSON 输出示例：**
```json
{
  "topic": "my-topic",
  "config": {
    "cleanup.policy": "delete",
    "compression.type": "producer",
    "max.message.bytes": "1048588",
    "min.insync.replicas": "2",
    "retention.ms": "604800000"
  }
}
```

### 导出为 CSV 格式

```bash
# 导出单个 Topic 配置
./kops -cmd config -type topic -t my-topic -export csv > topic_config.csv

# 导出所有 Topic 配置
./kops -cmd config -type topic -export csv > all_topics_config.csv

# 导出 Broker 配置
./kops -cmd config -type broker -export csv > brokers_config.csv
```

**CSV 输出示例：**
```csv
Topic,ConfigKey,Value
my-topic,cleanup.policy,delete
my-topic,compression.type,producer
my-topic,max.message.bytes,1048588
my-topic,min.insync.replicas,2
my-topic,retention.ms,604800000
```

---

## 配置项说明

### Topic 关键配置项

| 配置项 | 说明 |
|--------|------|
| `retention.ms` | 消息保留时间（毫秒） |
| `retention.bytes` | 分区最大保留字节数 |
| `segment.ms` | 日志段滚动时间 |
| `segment.bytes` | 日志段大小 |
| `cleanup.policy` | 清理策略（delete/compact） |
| `compression.type` | 压缩类型（none/gzip/snappy/lz4/zstd） |
| `min.insync.replicas` | 最小同步副本数 |
| `max.message.bytes` | 最大消息大小 |
| `min.compaction.lag.ms` | 最小压缩延迟 |

### Broker 关键配置项

| 配置项 | 说明 |
|--------|------|
| `log.retention.hours` | 日志保留小时数 |
| `log.retention.bytes` | 日志最大保留字节数 |
| `log.segment.bytes` | 日志段大小 |
| `num.network.threads` | 网络线程数 |
| `num.io.threads` | IO 线程数 |
| `socket.send.buffer.bytes` | 发送缓冲区大小 |
| `socket.receive.buffer.bytes` | 接收缓冲区大小 |
| `num.replica.fetchers` | 副本拉取线程数 |
| `replica.lag.time.max.ms` | 副本最大滞后时间 |
| `auto.create.topics.enable` | 是否自动创建主题 |

---

## 实用场景

### 1. 审计所有 Topic 的保留策略

```bash
./kops -cmd config -type topic -export csv | grep retention
```

### 2. 检查哪些 Topic 使用了压缩

```bash
./kops -cmd config -type topic -export csv | grep compression.type
```

### 3. 找出配置了 compact 策略的 Topic

```bash
./kops -cmd config -type topic -export csv | grep "compact"
```

### 4. 导出完整配置用于备份

```bash
./kops -cmd config -type topic -export json > backup_$(date +%Y%m%d).json
```

### 5. 对比不同环境的配置

```bash
# 生产环境
./kops -b prod-kafka:9092 -cmd config -type topic -t my-topic -export json > prod_config.json

# 测试环境
./kops -b test-kafka:9092 -cmd config -type topic -t my-topic -export json > test_config.json

# 对比
diff prod_config.json test_config.json
```

---

## 参数说明

| 参数 | 简写 | 说明 | 默认值 |
|------|------|------|--------|
| `-type` | - | 配置类型：topic 或 broker | topic |
| `-topic` | `-t` | Topic 名称（查看特定 Topic 时使用） | - |
| `-broker-id` | - | Broker ID（查看特定 Broker 时使用） | -1（所有） |
| `-export` | - | 导出格式：json 或 csv | -（控制台） |
| `-filter` | - | 过滤 Topic 名称（支持部分匹配） | - |
| `-internal` | - | 是否显示内部 Topic（__开头的） | false |
| `-brokers` | `-b` | Kafka 集群地址 | localhost:9092 |

---

## 编译和运行

```bash
# 编译
go build -o kops kops_enhanced.go

# 查看帮助
./kops

# 基本用法
./kops -cmd config -type topic -t my-topic
```

---

## 新增代码变更总结

1. **新增全局变量：**
   - `configType`：配置类型（topic/broker）
   - `brokerID`：Broker ID

2. **新增数据结构：**
   - `TopicConfigInfo`：Topic 配置信息
   - `BrokerConfigInfo`：Broker 配置信息

3. **新增核心函数：**
   - `cmdShowConfig()`：配置命令入口
   - `cmdShowTopicConfig()`：查看单个 Topic 配置
   - `cmdShowAllTopicsConfig()`：查看所有 Topic 配置
   - `cmdShowBrokerConfig()`：查看 Broker 配置

4. **新增辅助函数：**
   - `printTopicConfig()`：打印 Topic 配置
   - `printAllTopicsConfig()`：打印所有 Topic 配置摘要
   - `printBrokerConfig()`：打印 Broker 配置
   - `getConfigDescription()`：获取配置项中文说明
   - `exportTopicConfigJSON()`：导出 Topic 配置为 JSON
   - `exportTopicConfigCSV()`：导出 Topic 配置为 CSV
   - `exportAllTopicsConfigJSON()`：导出所有 Topic 配置为 JSON
   - `exportAllTopicsConfigCSV()`：导出所有 Topic 配置为 CSV
   - `exportBrokerConfigJSON()`：导出 Broker 配置为 JSON
   - `exportBrokerConfigCSV()`：导出 Broker 配置为 CSV

5. **功能特性：**
   - ✅ 支持查看单个/所有 Topic 配置
   - ✅ 支持查看单个/所有 Broker 配置
   - ✅ 支持 JSON/CSV 导出
   - ✅ 自动格式化时间和存储大小
   - ✅ 提供配置项中文说明
   - ✅ 支持 filter 过滤
   - ✅ 表格化展示，美观易读
