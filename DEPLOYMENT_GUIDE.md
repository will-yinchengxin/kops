# Kafka 运维工具 - 快速部署指南

## 📦 目录结构

```
kafka-ops/
├── kops                          # 主程序（编译后）
├── kops_full.go                  # 源代码
├── kafka-ops-toolkit.sh          # 自动化运维脚本
├── config/                       # 配置文件目录
│   ├── production.env           # 生产环境配置
│   ├── staging.env              # 测试环境配置
│   ├── topics.yaml              # Topic 批量创建配置
│   └── backup-topics.txt        # 需要备份的 topics
├── logs/                         # 日志目录
├── reports/                      # 报告目录
├── backups/                      # 备份目录
└── scripts/                      # 自定义脚本
    ├── daily-check.sh
    ├── weekly-report.sh
    └── monthly-audit.sh
```

## 🚀 快速开始

### 1. 编译工具

```bash
# 进入项目目录
cd kafka-ops

# 编译 kops
go build -o kops kops_full.go

# 验证编译
./kops -cmd health -b localhost:9092

# 或者不编译直接运行
go run kops_full.go -cmd health -b localhost:9092
```

### 2. 设置权限

```bash
# 给脚本添加执行权限
chmod +x kafka-ops-toolkit.sh
chmod +x scripts/*.sh

# 创建必要的目录
mkdir -p logs reports backups config scripts
```

### 3. 配置环境

创建 `config/production.env`:

```bash
# Kafka 集群配置
export KAFKA_BROKERS="kafka1.prod.com:9092,kafka2.prod.com:9092,kafka3.prod.com:9092"

# 默认配置
export DEFAULT_PARTITIONS=12
export DEFAULT_REPLICAS=3
export DEFAULT_RETENTION=168  # 7天
export DEFAULT_MIN_ISR=2

# 监控配置
export MONITOR_INTERVAL=60
export LAG_THRESHOLD=10000

# 备份配置
export BACKUP_ENABLED=true
export BACKUP_RETENTION_DAYS=30

# 告警配置
export ALERT_EMAIL="ops@example.com"
export ALERT_WEBHOOK="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"

# 自动清理配置
export AUTO_DELETE_TEST_TOPICS=false
export CLEANUP_OLD_LOGS=true
export LOG_RETENTION_DAYS=7
```

加载配置：

```bash
source config/production.env
./kops -cmd health -b "$KAFKA_BROKERS"
```

### 4. 配置 Cron 定时任务

```bash
# 编辑 crontab
crontab -e

# 添加以下任务
# 每小时执行健康检查
0 * * * * cd /path/to/kafka-ops && ./kafka-ops-toolkit.sh health >> logs/cron.log 2>&1

# 每天凌晨备份关键 topics
0 1 * * * cd /path/to/kafka-ops && ./kafka-ops-toolkit.sh backup >> logs/backup.log 2>&1

# 每 10 分钟监控 consumer lag
*/10 * * * * cd /path/to/kafka-ops && ./kafka-ops-toolkit.sh monitor-lag 5000 >> logs/lag-monitor.log 2>&1

# 每周日生成报告
0 2 * * 0 cd /path/to/kafka-ops && ./kafka-ops-toolkit.sh report >> logs/weekly-report.log 2>&1

# 每月 1 号进行配置审计
0 3 1 * * cd /path/to/kafka-ops && ./kafka-ops-toolkit.sh audit >> logs/monthly-audit.log 2>&1
```

## 📋 配置文件示例

### config/topics.yaml

用于批量创建 topics：

```yaml
# Topic 批量创建配置
topics:
  # 用户事件流
  - name: user.events.v1
    partitions: 12
    replicas: 3
    retention_hours: 168
    cleanup_policy: delete
    compression: lz4
    min_isr: 2
    description: "用户行为事件流"

  # 订单事件流
  - name: order.created.v1
    partitions: 24
    replicas: 3
    retention_hours: 720
    cleanup_policy: delete
    compression: lz4
    min_isr: 2
    description: "订单创建事件"

  # 支付事件流
  - name: payment.completed.v1
    partitions: 6
    replicas: 3
    retention_hours: 2160  # 90天
    cleanup_policy: delete
    compression: snappy
    min_isr: 2
    description: "支付完成事件"

  # 用户状态（Compacted）
  - name: user.state.v1
    partitions: 12
    replicas: 3
    cleanup_policy: compact
    compression: lz4
    min_isr: 2
    description: "用户状态存储"

  # 日志收集
  - name: application.logs.v1
    partitions: 48
    replicas: 2
    retention_hours: 24
    cleanup_policy: delete
    compression: gzip
    min_isr: 1
    description: "应用日志收集"
```

### config/backup-topics.txt

需要定期备份的 topics：

```
user.events.v1
order.created.v1
payment.completed.v1
user.state.v1
critical-business-events
audit-logs
```

### scripts/daily-check.sh

每日健康检查脚本：

```bash
#!/bin/bash
set -e

# 加载配置
source /path/to/kafka-ops/config/production.env

LOG_FILE="/path/to/kafka-ops/logs/daily-check-$(date +%Y%m%d).log"
KOPS="/path/to/kafka-ops/kops"

{
    echo "========================================="
    echo "Kafka Daily Health Check"
    echo "Date: $(date)"
    echo "========================================="
    echo ""

    # 1. 集群健康
    echo "1. Cluster Health:"
    $KOPS -cmd health -b "$KAFKA_BROKERS"
    echo ""

    # 2. Under-replicated 分区
    echo "2. Under-Replicated Partitions:"
    UNDER_REP=$($KOPS -cmd under-replicated -b "$KAFKA_BROKERS")
    echo "$UNDER_REP"
    
    if echo "$UNDER_REP" | grep -q "Found.*Under-Replicated"; then
        echo "⚠️  ALERT: Under-replicated partitions detected!"
        # 发送告警
        # send_alert "Under-replicated partitions found"
    fi
    echo ""

    # 3. Offline 分区
    echo "3. Offline Partitions:"
    OFFLINE=$($KOPS -cmd offline-partitions -b "$KAFKA_BROKERS")
    echo "$OFFLINE"
    
    if echo "$OFFLINE" | grep -q "Found.*Offline"; then
        echo "🚨 CRITICAL: Offline partitions detected!"
        # 发送紧急告警
        # send_critical_alert "Offline partitions found"
    fi
    echo ""

    # 4. Lag 监控
    echo "4. Top 10 Lagging Consumer Groups:"
    $KOPS -cmd groups -b "$KAFKA_BROKERS" | head -15
    echo ""

    # 5. 磁盘使用
    echo "5. Disk Usage (Top 10):"
    $KOPS -cmd disk-usage -b "$KAFKA_BROKERS" | head -15
    echo ""

    echo "========================================="
    echo "Daily check completed at $(date)"
    echo "========================================="

} | tee "$LOG_FILE"

# 如果有问题，发送汇总邮件
if grep -q "ALERT\|CRITICAL" "$LOG_FILE"; then
    # mail -s "Kafka Daily Check - Issues Found" "$ALERT_EMAIL" < "$LOG_FILE"
    echo "Issues found - alert sent"
fi
```

### scripts/weekly-report.sh

每周报告脚本：

```bash
#!/bin/bash
set -e

source /path/to/kafka-ops/config/production.env

REPORT_DIR="/path/to/kafka-ops/reports/weekly"
KOPS="/path/to/kafka-ops/kops"
WEEK=$(date +%Y-W%V)

mkdir -p "$REPORT_DIR"

{
    echo "╔══════════════════════════════════════════════════════════════════╗"
    echo "║         Kafka Weekly Report - $WEEK                              ║"
    echo "╚══════════════════════════════════════════════════════════════════╝"
    echo ""
    echo "Generated: $(date)"
    echo ""

    echo "═══ CLUSTER OVERVIEW ═══"
    $KOPS -cmd inspect -b "$KAFKA_BROKERS"
    echo ""

    echo "═══ TOPIC SUMMARY ═══"
    $KOPS -cmd topics -b "$KAFKA_BROKERS"
    echo ""

    echo "═══ CONSUMER GROUPS ═══"
    $KOPS -cmd groups -b "$KAFKA_BROKERS"
    echo ""

    echo "═══ DISK USAGE ANALYSIS ═══"
    $KOPS -cmd disk-usage -b "$KAFKA_BROKERS"
    echo ""

    echo "═══ CONFIGURATION AUDIT ═══"
    $KOPS -cmd config -type topic -b "$KAFKA_BROKERS" | head -50
    echo ""

    echo "═══ RECOMMENDATIONS ═══"
    echo "1. Review topics with high message count"
    echo "2. Check consumer groups with high lag"
    echo "3. Monitor disk usage trends"
    echo "4. Plan for capacity expansion if needed"
    echo ""

} > "$REPORT_DIR/report-$WEEK.txt"

# 生成 HTML 报告
$KOPS -cmd report -b "$KAFKA_BROKERS" -output "$REPORT_DIR/report-$WEEK.html"

# 生成 JSON 数据
$KOPS -cmd inspect -b "$KAFKA_BROKERS" -export json > "$REPORT_DIR/data-$WEEK.json"

echo "Weekly report generated in $REPORT_DIR"

# 发送报告
# mail -s "Kafka Weekly Report - $WEEK" -a "$REPORT_DIR/report-$WEEK.html" "$ALERT_EMAIL"
```

### scripts/monthly-audit.sh

每月审计脚本：

```bash
#!/bin/bash
set -e

source /path/to/kafka-ops/config/production.env

AUDIT_DIR="/path/to/kafka-ops/reports/audit"
KOPS="/path/to/kafka-ops/kops"
MONTH=$(date +%Y-%m)

mkdir -p "$AUDIT_DIR"

{
    echo "╔══════════════════════════════════════════════════════════════════╗"
    echo "║         Kafka Monthly Audit - $MONTH                             ║"
    echo "╚══════════════════════════════════════════════════════════════════╝"
    echo ""

    echo "═══ CONFIGURATION SNAPSHOT ═══"
    echo "Exporting all topic configurations..."
    $KOPS -cmd config -type topic -b "$KAFKA_BROKERS" -export json > "$AUDIT_DIR/topics-config-$MONTH.json"
    
    echo "Exporting broker configurations..."
    $KOPS -cmd config -type broker -b "$KAFKA_BROKERS" -export json > "$AUDIT_DIR/brokers-config-$MONTH.json"
    
    echo "═══ CAPACITY ANALYSIS ═══"
    $KOPS -cmd disk-usage -b "$KAFKA_BROKERS" -export csv > "$AUDIT_DIR/disk-usage-$MONTH.csv"
    
    echo "═══ CONSUMER GROUP ANALYSIS ═══"
    $KOPS -cmd groups -b "$KAFKA_BROKERS" -export csv > "$AUDIT_DIR/consumer-groups-$MONTH.csv"
    
    echo "═══ TOPIC INVENTORY ═══"
    $KOPS -cmd topics -b "$KAFKA_BROKERS" -export csv > "$AUDIT_DIR/topics-$MONTH.csv"
    
    echo ""
    echo "Audit completed: $AUDIT_DIR"
    
} | tee "$AUDIT_DIR/audit-summary-$MONTH.txt"

# 压缩审计文件
tar -czf "$AUDIT_DIR/audit-$MONTH.tar.gz" "$AUDIT_DIR"/*-$MONTH.*

echo "Audit archive created: $AUDIT_DIR/audit-$MONTH.tar.gz"
```

## 🔧 高级配置

### 监控集成

#### Prometheus 指标导出

创建 `scripts/export-metrics.sh`:

```bash
#!/bin/bash

# 导出 Kafka 指标为 Prometheus 格式
source config/production.env

# 获取数据
BROKER_COUNT=$($KOPS -cmd brokers -b "$KAFKA_BROKERS" | grep -c "Online" || echo "0")
TOPIC_COUNT=$($KOPS -cmd topics -b "$KAFKA_BROKERS" | grep -c "^" || echo "0")
GROUP_COUNT=$($KOPS -cmd groups -b "$KAFKA_BROKERS" | grep -c "^" || echo "0")

# 输出 Prometheus 格式
cat << EOF > /var/lib/node_exporter/textfile_collector/kafka.prom
# HELP kafka_brokers_total Total number of Kafka brokers
# TYPE kafka_brokers_total gauge
kafka_brokers_total $BROKER_COUNT

# HELP kafka_topics_total Total number of Kafka topics
# TYPE kafka_topics_total gauge
kafka_topics_total $TOPIC_COUNT

# HELP kafka_consumer_groups_total Total number of consumer groups
# TYPE kafka_consumer_groups_total gauge
kafka_consumer_groups_total $GROUP_COUNT
EOF
```

#### Grafana Dashboard JSON

可以导出指标后在 Grafana 中创建仪表板。

### 告警集成

创建 `scripts/send-alert.sh`:

```bash
#!/bin/bash

LEVEL="$1"  # info, warning, critical
MESSAGE="$2"
WEBHOOK_URL="$ALERT_WEBHOOK"

# 发送到 Slack
curl -X POST "$WEBHOOK_URL" \
  -H 'Content-Type: application/json' \
  -d "{
    \"text\": \"[$LEVEL] Kafka Alert\",
    \"attachments\": [{
      \"color\": \"$([ "$LEVEL" = "critical" ] && echo "danger" || echo "warning")\",
      \"text\": \"$MESSAGE\",
      \"footer\": \"Kafka Ops\",
      \"ts\": $(date +%s)
    }]
  }"

# 发送邮件
if [ "$LEVEL" = "critical" ]; then
    echo "$MESSAGE" | mail -s "[CRITICAL] Kafka Alert" "$ALERT_EMAIL"
fi
```

## 📊 监控最佳实践

### 1. 关键指标监控

```bash
# 每分钟检查关键指标
*/1 * * * * /path/to/scripts/check-critical-metrics.sh
```

`check-critical-metrics.sh`:

```bash
#!/bin/bash
source config/production.env

# Under-replicated 检查
UNDER_REP=$($KOPS -cmd under-replicated -b "$KAFKA_BROKERS" | grep -c "Under-Replicated")
if [ "$UNDER_REP" -gt 0 ]; then
    ./scripts/send-alert.sh warning "Found $UNDER_REP under-replicated partitions"
fi

# Offline 分区检查
OFFLINE=$($KOPS -cmd offline-partitions -b "$KAFKA_BROKERS" | grep -c "Offline")
if [ "$OFFLINE" -gt 0 ]; then
    ./scripts/send-alert.sh critical "Found $OFFLINE offline partitions!"
fi

# Lag 检查（示例）
# 这里可以添加更多检查逻辑
```

### 2. 容量趋势分析

创建每日数据收集任务，用于趋势分析：

```bash
# 每天收集容量数据
0 0 * * * /path/to/scripts/collect-capacity-data.sh
```

### 3. SLA 监控

根据业务需求定义SLA并监控：

```bash
# 定义 SLA
MAX_LAG=10000
MAX_UNDER_REPLICATED=5
MAX_OFFLINE=0

# 每 5 分钟检查
*/5 * * * * /path/to/scripts/check-sla.sh
```

## 🎯 总结

通过以上配置，您可以实现：

1. ✅ 全面的健康监控
2. ✅ 自动化备份
3. ✅ 主动告警
4. ✅ 定期报告
5. ✅ 配置审计
6. ✅ 容量规划
7. ✅ 应急响应

建议根据实际环境调整配置参数和告警阈值。
