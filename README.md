# 🚀 Kafka 全面运维工具套件

> 专业的 Kafka 集群运维、监控、诊断、数据管理一体化解决方案

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

---

## ✨ 核心特性

### 📊 全面监控
- ✅ 实时集群健康监控
- ✅ Consumer Lag 追踪与告警
- ✅ 分区副本状态检查
- ✅ 磁盘使用分析
- ✅ 吞吐量性能分析

### 🔧 Topic 管理
- ✅ 创建、删除、修改 Topic
- ✅ 动态调整分区数
- ✅ 配置批量管理
- ✅ Topic 间数据镜像
- ✅ 配置对比与审计

### 👥 Consumer Group 管理
- ✅ Group 状态查询
- ✅ Offset 重置
- ✅ Lag 监控与告警
- ✅ 成员信息查看
- ✅ Group 删除与清理

### 💾 数据操作
- ✅ 消息查询与搜索
- ✅ Topic 数据备份
- ✅ 数据恢复与迁移
- ✅ 消息生产与消费
- ✅ Topic 数据清空

### 📈 诊断与分析
- ✅ Under-replicated 分区检测
- ✅ Offline 分区查找
- ✅ 性能瓶颈分析
- ✅ 容量规划建议
- ✅ 配置异常检测

### 📄 报告生成
- ✅ HTML 集群报告
- ✅ JSON/CSV 数据导出
- ✅ 审计日志
- ✅ 容量分析报告
- ✅ 自定义报告模板

---


## 📖 命令速查表

### 基础信息
```bash
kops -cmd health              # 集群健康检查
kops -cmd inspect             # 全面巡检
kops -cmd brokers             # Broker 列表
kops -cmd topics              # Topic 列表
kops -cmd groups              # Consumer Group 列表
```

### Topic 管理
```bash
# 创建 topic
kops -cmd create-topic -t my-topic -partitions 12 -replicas 3

# 查看详情
kops -cmd details -t my-topic

# 增加分区
kops -cmd increase-partitions -t my-topic -partitions 24

# 修改配置
kops -cmd alter-topic -t my-topic -retention 336

# 删除 topic
kops -cmd delete-topic -t my-topic -force
```

### 消息操作
```bash
# 查看最新消息
kops -cmd peek -t my-topic -n 50

# 搜索消息
kops -cmd search -t my-topic -search-value "keyword" -n 100

# 统计消息数
kops -cmd count -t my-topic

# 发送消息
kops -cmd produce -t my-topic -d '{"data":"value"}' -key "key1"
```

### Consumer Group
```bash
# 查看 lag
kops -cmd lag -g my-group

# 查看详情
kops -cmd describe-group -g my-group

# 重置 offset
kops -cmd reset-offset -g my-group -t my-topic -from 0

# 删除 group
kops -cmd delete-group -g my-group
```

### 监控诊断
```bash
# 实时监控
kops -cmd monitor -interval 10

# 监控 lag
kops -cmd monitor-lag -g my-group -interval 5 -threshold 10000

# 检查副本状态
kops -cmd under-replicated

# 检查离线分区
kops -cmd offline-partitions

# 磁盘使用
kops -cmd disk-usage
```

### 配置管理
```bash
# 查看 topic 配置
kops -cmd config -type topic -t my-topic

# 查看所有 topic 配置
kops -cmd config -type topic

# 查看 broker 配置
kops -cmd config -type broker -broker-id 1

# 对比配置
kops -cmd compare-config -t topic1,topic2,topic3
```

### 数据操作
```bash
# 备份 topic
kops -cmd backup -t my-topic -output backup.json

# 镜像 topic
kops -cmd mirror -t source-topic -target-topic dest-topic

# 清空 topic
kops -cmd purge -t test-topic -force
```

### 报告生成
```bash
# 生成 HTML 报告
kops -cmd report -output cluster-report.html

# 生成审计报告
kops -cmd audit -output audit.json

# 导出为 CSV
kops -cmd topics -export csv > topics.csv
```

---

## 🎯 典型使用场景

### 场景 1：新业务上线

```bash
# 1. 创建 topic
kops -cmd create-topic \
  -t user-service.events.v1 \
  -partitions 12 \
  -replicas 3 \
  -retention 168 \
  -compression lz4 \
  -min-isr 2

# 2. 验证配置
kops -cmd config -type topic -t user-service.events.v1

# 3. 测试消息
kops -cmd produce \
  -t user-service.events.v1 \
  -d '{"event":"test","timestamp":"2024-01-06"}' \
  -key "test-1"

# 4. 验证消费
kops -cmd peek -t user-service.events.v1 -n 1
```

### 场景 2：故障排查

```bash
# 1. 快速健康检查
kops -cmd health

# 2. 检查问题分区
kops -cmd under-replicated
kops -cmd offline-partitions

# 3. 检查 consumer lag
kops -cmd groups | grep "⚠️"

# 4. 查看问题 group 详情
kops -cmd describe-group -g problematic-group

# 5. 生成诊断报告
kops -cmd inspect > diagnostic-report.txt
```

### 场景 3：容量扩展

```bash
# 1. 查看当前使用情况
kops -cmd disk-usage

# 2. 分析增长趋势
kops -cmd throughput -t high-traffic-topic

# 3. 增加分区
kops -cmd increase-partitions -t high-traffic-topic -partitions 24

# 4. 验证变更
kops -cmd details -t high-traffic-topic
```

### 场景 4：数据迁移

```bash
# 1. 备份源数据
kops -cmd backup -t source-topic -output source-backup.json

# 2. 创建目标 topic
kops -cmd create-topic -t dest-topic -partitions 12 -replicas 3

# 3. 镜像数据
kops -cmd mirror -t source-topic -target-topic dest-topic

# 4. 验证数据
kops -cmd count -t source-topic
kops -cmd count -t dest-topic
```

### 场景 5：定期维护

```bash
# 设置 cron job
# 每小时健康检查
0 * * * * /path/to/kops -cmd health >> /var/log/kafka/health.log

# 每天备份关键 topics
0 2 * * * /path/to/kafka-ops-toolkit.sh backup

# 每周生成报告
0 0 * * 0 /path/to/kops -cmd report -output /reports/weekly-$(date +\%Y\%W).html

# 每月配置审计
0 0 1 * * /path/to/kops -cmd audit -output /audit/audit-$(date +\%Y\%m).json
```

---