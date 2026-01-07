package main

import (
	"context"
	"encoding/csv"
	_ "encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	_ "strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/IBM/sarama"
)

var (
	brokersStr      string
	command         string
	topic           string
	group           string
	partition       int
	key             string
	data            string
	limit           int
	export          string
	filter          string
	showInternal    bool
	timeout         int
	configType      string
	brokerID        int
	partitionCount  int
	replicationFact int
	retentionHours  int
	cleanupPolicy   string
	compressionType string
	minISR          int
	maxMessageSize  int
	fromOffset      int64
	toOffset        int64
	searchKey       string
	searchValue     string
	targetTopic     string
	targetGroup     string
	dryRun          bool
	force           bool
	configFile      string
	outputFile      string
	monitorInterval int
	alertThreshold  int64
)

type TopicInfo struct {
	Name         string
	Partitions   int32
	Replicas     int16
	TotalLogSize int64
	Internal     bool
	Configs      map[string]*string
}

type TopicConfigInfo struct {
	Topic  string
	Config map[string]string
}

type BrokerConfigInfo struct {
	BrokerID int32
	Host     string
	Port     int32
	Config   map[string]string
}

func init() {
	// 基础参数
	flag.StringVar(&brokersStr, "brokers", "localhost:9092", "Kafka brokers (comma separated)")
	flag.StringVar(&brokersStr, "b", "localhost:9092", "Kafka brokers (short)")
	flag.StringVar(&command, "cmd", "", "Command to execute")
	flag.StringVar(&topic, "topic", "", "Topic name")
	flag.StringVar(&topic, "t", "", "Topic name (short)")
	flag.StringVar(&group, "group", "", "Consumer group name")
	flag.StringVar(&group, "g", "", "Consumer group name (short)")

	// 分区和副本参数
	flag.IntVar(&partition, "partition", -1, "Partition number (-1 for auto)")
	flag.IntVar(&partition, "p", -1, "Partition number (short)")
	flag.IntVar(&partitionCount, "partitions", 3, "Number of partitions for topic creation")
	flag.IntVar(&replicationFact, "replicas", 2, "Replication factor for topic creation")

	// 消息参数
	flag.StringVar(&key, "key", "", "Message key")
	flag.StringVar(&data, "data", "", "Message data")
	flag.StringVar(&data, "d", "", "Message data (short)")
	flag.IntVar(&limit, "limit", 10, "Number of messages")
	flag.IntVar(&limit, "n", 10, "Number of messages (short)")

	// 偏移量参数
	flag.Int64Var(&fromOffset, "from", -1, "Start offset")
	flag.Int64Var(&toOffset, "to", -1, "End offset")

	// 搜索参数
	flag.StringVar(&searchKey, "search-key", "", "Search messages by key pattern")
	flag.StringVar(&searchValue, "search-value", "", "Search messages by value pattern")

	// 配置参数
	flag.StringVar(&configType, "type", "topic", "Config type: topic or broker")
	flag.IntVar(&brokerID, "broker-id", -1, "Broker ID")
	flag.IntVar(&retentionHours, "retention", 168, "Retention time in hours")
	flag.StringVar(&cleanupPolicy, "cleanup", "delete", "Cleanup policy: delete or compact")
	flag.StringVar(&compressionType, "compression", "producer", "Compression type")
	flag.IntVar(&minISR, "min-isr", 1, "Minimum in-sync replicas")
	flag.IntVar(&maxMessageSize, "max-msg-size", 1048576, "Max message size in bytes")

	// 导出和过滤参数
	flag.StringVar(&export, "export", "", "Export format: csv, json, table")
	flag.StringVar(&filter, "filter", "", "Filter pattern")
	flag.BoolVar(&showInternal, "internal", false, "Show internal topics")
	flag.IntVar(&timeout, "timeout", 10, "Timeout in seconds")

	// 目标参数（用于迁移、复制等）
	flag.StringVar(&targetTopic, "target-topic", "", "Target topic for operations")
	flag.StringVar(&targetGroup, "target-group", "", "Target consumer group")

	// 操作控制参数
	flag.BoolVar(&dryRun, "dry-run", false, "Dry run mode (show what would be done)")
	flag.BoolVar(&force, "force", false, "Force operation without confirmation")

	// 文件参数
	flag.StringVar(&configFile, "config-file", "", "Config file path")
	flag.StringVar(&outputFile, "output", "", "Output file path")

	// 监控参数
	flag.IntVar(&monitorInterval, "interval", 5, "Monitor interval in seconds")
	flag.Int64Var(&alertThreshold, "threshold", 1000, "Alert threshold for lag")
}

func main() {
	flag.Parse()

	if command == "" {
		printUsage()
		os.Exit(1)
	}

	brokers := strings.Split(brokersStr, ",")
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0

	switch command {
	// === 基础信息查询 ===
	case "topics":
		cmdListTopics(brokers, config)
	case "details":
		requireTopic()
		cmdTopicDetails(brokers, config, topic)
	case "groups":
		cmdListGroups(brokers, config)
	case "brokers":
		cmdListBrokers(brokers, config)
	case "health":
		cmdHealthCheck(brokers, config)
	case "inspect":
		cmdInspect(brokers, config)

	// === 消息操作 ===
	case "peek":
		requireTopic()
		cmdPeek(brokers, config, topic, limit)
	case "produce":
		requireTopicAndData()
		cmdProduce(brokers, config, topic, key, data, int32(partition))
	case "consume":
		requireTopic()
		cmdConsume(brokers, config, topic, group, fromOffset, toOffset, limit)
	case "search":
		requireTopic()
		cmdSearchMessages(brokers, config, topic, searchKey, searchValue, limit)
	case "count":
		requireTopic()
		cmdCountMessages(brokers, config, topic)
	case "sample":
		requireTopic()
		cmdSampleMessages(brokers, config, topic, limit)

	// === Consumer Group 管理 ===
	case "lag":
		requireGroup()
		cmdGroupLag(brokers, config, group)
	case "reset-offset":
		requireGroupAndTopic()
		cmdResetOffset(brokers, config, group, topic, fromOffset)
	case "delete-group":
		requireGroup()
		cmdDeleteGroup(brokers, config, group)
	case "describe-group":
		requireGroup()
		cmdDescribeGroup(brokers, config, group)

	// === Topic 管理 ===
	case "create-topic":
		requireTopic()
		cmdCreateTopic(brokers, config, topic, partitionCount, replicationFact)
	case "delete-topic":
		requireTopic()
		cmdDeleteTopic(brokers, config, topic)
	case "alter-topic":
		requireTopic()
		cmdAlterTopic(brokers, config, topic)
	case "increase-partitions":
		requireTopic()
		cmdIncreasePartitions(brokers, config, topic, partitionCount)

	// === 配置管理 ===
	case "config":
		cmdShowConfig(brokers, config)
	case "set-config":
		cmdSetConfig(brokers, config)
	case "compare-config":
		cmdCompareConfig(brokers, config)

	// === 监控和诊断 ===
	case "monitor":
		cmdMonitor(brokers, config, monitorInterval)
	case "monitor-lag":
		cmdMonitorLag(brokers, config, group, monitorInterval, alertThreshold)
	case "disk-usage":
		cmdDiskUsage(brokers, config)
	case "under-replicated":
		cmdUnderReplicated(brokers, config)
	case "offline-partitions":
		cmdOfflinePartitions(brokers, config)

	// === 数据操作 ===
	case "mirror":
		requireTopicAndTarget()
		cmdMirrorTopic(brokers, config, topic, targetTopic)
	case "backup":
		requireTopic()
		cmdBackupTopic(brokers, config, topic, outputFile)
	case "restore":
		requireTopic()
		cmdRestoreData(brokers, config, topic, configFile)
	case "purge":
		requireTopic()
		cmdPurgeTopic(brokers, config, topic)

	// === 性能分析 ===
	case "throughput":
		cmdThroughput(brokers, config, topic)
	case "producer-perf":
		requireTopic()
		cmdProducerPerf(brokers, config, topic, limit)
	case "consumer-perf":
		requireTopic()
		cmdConsumerPerf(brokers, config, topic, limit)

	// === 批量操作 ===
	case "batch-create":
		cmdBatchCreateTopics(brokers, config, configFile)
	case "batch-delete":
		cmdBatchDeleteTopics(brokers, config, configFile)
	case "batch-alter":
		cmdBatchAlterConfig(brokers, config, configFile)

	// === ACL 和安全 ===
	case "acls":
		cmdListACLs(brokers, config)
	case "add-acl":
		cmdAddACL(brokers, config)
	case "remove-acl":
		cmdRemoveACL(brokers, config)

	// === 报告生成 ===
	case "report":
		cmdGenerateReport(brokers, config, outputFile)
	case "audit":
		cmdAuditCluster(brokers, config, outputFile)

	default:
		log.Fatalf("Unknown command: %s", command)
	}
}

func printUsage() {
	fmt.Println(`
╔══════════════════════════════════════════════════════════════════════════════╗
║                    KafkaOps - 全面 Kafka 运维工具                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

📋 基础信息查询:
  topics              列出所有 topics
  details             显示 topic 详细信息
  groups              列出所有 consumer groups
  brokers             列出所有 brokers
  health              集群健康检查
  inspect             集群全面巡检

📨 消息操作:
  peek                查看最新 N 条消息
  produce             发送消息到 topic
  consume             从 topic 消费消息
  search              搜索消息（按 key/value）
  count               统计消息数量
  sample              随机采样消息

👥 Consumer Group 管理:
  lag                 查看 consumer group lag
  reset-offset        重置 consumer group offset
  delete-group        删除 consumer group
  describe-group      查看 group 详细信息

📂 Topic 管理:
  create-topic        创建 topic
  delete-topic        删除 topic
  alter-topic         修改 topic 配置
  increase-partitions 增加分区数

⚙️  配置管理:
  config              查看配置
  set-config          设置配置
  compare-config      对比配置

📊 监控和诊断:
  monitor             实时监控集群
  monitor-lag         监控 consumer lag
  disk-usage          查看磁盘使用情况
  under-replicated    查找未充分复制的分区
  offline-partitions  查找离线分区

💾 数据操作:
  mirror              镜像 topic 数据
  backup              备份 topic 数据
  restore             恢复数据
  purge               清空 topic 数据

🚀 性能分析:
  throughput          吞吐量分析
  producer-perf       生产者性能测试
  consumer-perf       消费者性能测试

🔄 批量操作:
  batch-create        批量创建 topics
  batch-delete        批量删除 topics
  batch-alter         批量修改配置

🔐 ACL 和安全:
  acls                列出 ACLs
  add-acl             添加 ACL
  remove-acl          删除 ACL

📄 报告生成:
  report              生成运维报告
  audit               集群审计报告

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📖 使用示例:

  # 查看集群信息
  kops -cmd health -b kafka1:9092,kafka2:9092
  kops -cmd inspect -export json > report.json
  
  # Topic 操作
  kops -cmd create-topic -t my-topic -partitions 6 -replicas 3
  kops -cmd details -t my-topic
  kops -cmd increase-partitions -t my-topic -partitions 12
  
  # 消息操作
  kops -cmd produce -t test -d '{"user":"alice","action":"login"}' -key "user:1"
  kops -cmd peek -t test -n 20
  kops -cmd search -t test -search-value "alice" -n 100
  kops -cmd count -t test
  
  # Consumer Group 管理
  kops -cmd lag -g my-consumer
  kops -cmd reset-offset -g my-consumer -t my-topic -from 0
  kops -cmd describe-group -g my-consumer
  
  # 配置管理
  kops -cmd config -type topic -t my-topic
  kops -cmd set-config -type topic -t my-topic -retention 336
  kops -cmd compare-config -t topic1,topic2
  
  # 监控
  kops -cmd monitor -interval 10
  kops -cmd monitor-lag -g my-consumer -interval 5 -threshold 5000
  kops -cmd disk-usage
  kops -cmd under-replicated
  
  # 数据操作
  kops -cmd backup -t my-topic -output backup.json
  kops -cmd mirror -t source-topic -target-topic dest-topic
  kops -cmd purge -t test-topic -force
  
  # 性能测试
  kops -cmd producer-perf -t perf-test -n 10000
  kops -cmd consumer-perf -t perf-test -n 10000
  kops -cmd throughput -t my-topic
  
  # 批量操作
  kops -cmd batch-create -config-file topics.yaml
  kops -cmd batch-delete -config-file delete-list.txt
  
  # 报告生成
  kops -cmd report -output cluster-report.html
  kops -cmd audit -output audit-$(date +%Y%m%d).json

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔧 常用参数:
  -b, -brokers        Kafka 集群地址 (默认: localhost:9092)
  -t, -topic          Topic 名称
  -g, -group          Consumer group 名称
  -n, -limit          数量限制 (默认: 10)
  -export             导出格式: csv, json, table
  -filter             过滤模式
  -dry-run            演练模式（不实际执行）
  -force              强制执行（跳过确认）
  -timeout            超时时间（秒）

💡 提示: 使用 'kops -cmd <command> -h' 查看特定命令的详细帮助
`)
}

// ============================================================================
// 辅助函数
// ============================================================================

func requireTopic() {
	if topic == "" {
		log.Fatal("❌ 请使用 -topic 指定 topic 名称")
	}
}

func requireGroup() {
	if group == "" {
		log.Fatal("❌ 请使用 -group 指定 consumer group 名称")
	}
}

func requireTopicAndData() {
	if topic == "" || data == "" {
		log.Fatal("❌ 请使用 -topic 和 -data 参数")
	}
}

func requireGroupAndTopic() {
	if group == "" || topic == "" {
		log.Fatal("❌ 请使用 -group 和 -topic 参数")
	}
}

func requireTopicAndTarget() {
	if topic == "" || targetTopic == "" {
		log.Fatal("❌ 请使用 -topic 和 -target-topic 参数")
	}
}

func confirmAction(message string) bool {
	if force {
		return true
	}

	fmt.Printf("\n⚠️  %s\n", message)
	fmt.Print("确认继续? (yes/no): ")
	var response string
	fmt.Scanln(&response)
	return strings.ToLower(response) == "yes" || strings.ToLower(response) == "y"
}

func cmdListBrokers(brokers []string, config *sarama.Config) {
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	brokersList, controllerID, err := admin.DescribeCluster()
	if err != nil {
		log.Fatalf("Failed to get cluster info: %v", err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintf(w, "🖥️  Kafka Brokers (%d total)\n\n", len(brokersList))
	fmt.Fprintln(w, "ID\tHOST:PORT\tRACK\tROLE\tSTATUS")

	for _, broker := range brokersList {
		role := "Broker"
		if broker.ID() == controllerID {
			role = "Controller ⭐"
		}

		rack := "-"
		if broker.Rack() != "" {
			rack = broker.Rack()
		}

		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t✅ Online\n",
			broker.ID(), broker.Addr(), rack, role)
	}
	w.Flush()
}

func cmdConsume(brokers []string, config *sarama.Config, topicName, groupName string, from, to int64, limit int) {
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions(topicName)
	if err != nil {
		log.Fatalf("Failed to get partitions: %v", err)
	}

	fmt.Printf("📥 Consuming from topic: %s\n", topicName)
	if groupName != "" {
		fmt.Printf("   Consumer Group: %s\n", groupName)
	}
	fmt.Printf("   Partitions: %v\n", partitions)
	fmt.Printf("   Offset Range: %d to %d\n", from, to)
	fmt.Println()

	var wg sync.WaitGroup
	messageCount := 0
	var mu sync.Mutex

	for _, partition := range partitions {
		wg.Add(1)
		go func(p int32) {
			defer wg.Done()

			startOffset := from
			if from == -1 {
				startOffset = sarama.OffsetNewest
			}

			pc, err := consumer.ConsumePartition(topicName, p, startOffset)
			if err != nil {
				log.Printf("Error consuming partition %d: %v", p, err)
				return
			}
			defer pc.Close()

			for msg := range pc.Messages() {
				mu.Lock()
				if limit > 0 && messageCount >= limit {
					mu.Unlock()
					return
				}
				messageCount++

				fmt.Printf("[P:%d O:%d] Key: %s | Value: %s | Time: %s\n",
					msg.Partition, msg.Offset,
					string(msg.Key), string(msg.Value),
					msg.Timestamp.Format("15:04:05.000"))
				mu.Unlock()

				if to >= 0 && msg.Offset >= to {
					return
				}
			}
		}(partition)
	}

	wg.Wait()
	fmt.Printf("\n✅ Consumed %d messages\n", messageCount)
}

func cmdSearchMessages(brokers []string, config *sarama.Config, topicName, keyPattern, valuePattern string, limit int) {
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions(topicName)
	if err != nil {
		log.Fatalf("Failed to get partitions: %v", err)
	}

	fmt.Printf("🔍 Searching in topic: %s\n", topicName)
	if keyPattern != "" {
		fmt.Printf("   Key pattern: %s\n", keyPattern)
	}
	if valuePattern != "" {
		fmt.Printf("   Value pattern: %s\n", valuePattern)
	}
	fmt.Println()

	var results []MessageInfo
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, partition := range partitions {
		wg.Add(1)
		go func(p int32) {
			defer wg.Done()

			pc, err := consumer.ConsumePartition(topicName, p, sarama.OffsetOldest)
			if err != nil {
				return
			}
			defer pc.Close()

			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			for {
				select {
				case msg := <-pc.Messages():
					match := false
					if keyPattern != "" && strings.Contains(string(msg.Key), keyPattern) {
						match = true
					}
					if valuePattern != "" && strings.Contains(string(msg.Value), valuePattern) {
						match = true
					}
					if keyPattern == "" && valuePattern == "" {
						match = true
					}

					if match {
						mu.Lock()
						if len(results) < limit {
							results = append(results, MessageInfo{
								Partition: p,
								Offset:    msg.Offset,
								Key:       string(msg.Key),
								Value:     string(msg.Value),
								Timestamp: msg.Timestamp,
							})
						}
						mu.Unlock()
					}

					if len(results) >= limit {
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}(partition)
	}

	wg.Wait()

	if len(results) == 0 {
		fmt.Println("❌ No matching messages found")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintf(w, "Found %d matching messages:\n\n", len(results))
	fmt.Fprintln(w, "TIMESTAMP\tPARTITION\tOFFSET\tKEY\tVALUE")

	for _, msg := range results {
		fmt.Fprintf(w, "%s\t%d\t%d\t%s\t%s\n",
			msg.Timestamp.Format("15:04:05.000"),
			msg.Partition, msg.Offset, msg.Key, msg.Value)
	}
	w.Flush()
}

func cmdCountMessages(brokers []string, config *sarama.Config, topicName string) {
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	partitions, err := client.Partitions(topicName)
	if err != nil {
		log.Fatalf("Failed to get partitions: %v", err)
	}

	var totalCount int64
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintf(w, "📊 Message Count for Topic: %s\n\n", topicName)
	fmt.Fprintln(w, "PARTITION\tOLDEST\tNEWEST\tCOUNT")

	for _, p := range partitions {
		oldest, _ := client.GetOffset(topicName, p, sarama.OffsetOldest)
		newest, _ := client.GetOffset(topicName, p, sarama.OffsetNewest)
		count := newest - oldest
		totalCount += count

		fmt.Fprintf(w, "%d\t%d\t%d\t%d\n", p, oldest, newest, count)
	}

	fmt.Fprintf(w, "\nTOTAL\t-\t-\t%d\n", totalCount)
	w.Flush()
}

func cmdSampleMessages(brokers []string, config *sarama.Config, topicName string, sampleSize int) {
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	partitions, err := client.Partitions(topicName)
	if err != nil {
		log.Fatalf("Failed to get partitions: %v", err)
	}

	// 计算总消息数
	var totalMessages int64
	for _, p := range partitions {
		oldest, _ := client.GetOffset(topicName, p, sarama.OffsetOldest)
		newest, _ := client.GetOffset(topicName, p, sarama.OffsetNewest)
		totalMessages += (newest - oldest)
	}

	if totalMessages == 0 {
		fmt.Println("❌ Topic is empty")
		return
	}

	fmt.Printf("📊 Sampling %d messages from %d total messages\n\n", sampleSize, totalMessages)

	// 简化：从每个分区均匀采样
	samplesPerPartition := sampleSize / len(partitions)
	if samplesPerPartition == 0 {
		samplesPerPartition = 1
	}

	// 这里实现简化版本，实际可以更复杂
	cmdPeek(brokers, config, topicName, samplesPerPartition*len(partitions))
}

func cmdCreateTopic(brokers []string, config *sarama.Config, topicName string, partitions, replication int) {
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	if dryRun {
		fmt.Printf("🔍 [DRY RUN] Would create topic:\n")
		fmt.Printf("   Name: %s\n", topicName)
		fmt.Printf("   Partitions: %d\n", partitions)
		fmt.Printf("   Replication Factor: %d\n", replication)
		return
	}

	if !confirmAction(fmt.Sprintf("Create topic '%s' with %d partitions and %d replicas?", topicName, partitions, replication)) {
		fmt.Println("❌ Cancelled")
		return
	}

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     int32(partitions),
		ReplicationFactor: int16(replication),
		ConfigEntries: map[string]*string{
			"retention.ms":        stringPtr(fmt.Sprintf("%d", retentionHours*3600000)),
			"cleanup.policy":      stringPtr(cleanupPolicy),
			"compression.type":    stringPtr(compressionType),
			"min.insync.replicas": stringPtr(fmt.Sprintf("%d", minISR)),
		},
	}

	err = admin.CreateTopic(topicName, topicDetail, false)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}

	fmt.Printf("✅ Topic '%s' created successfully!\n", topicName)
	fmt.Printf("   Partitions: %d\n", partitions)
	fmt.Printf("   Replication: %d\n", replication)
	fmt.Printf("   Retention: %dh\n", retentionHours)
}

func cmdDeleteTopic(brokers []string, config *sarama.Config, topicName string) {
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	if dryRun {
		fmt.Printf("🔍 [DRY RUN] Would delete topic: %s\n", topicName)
		return
	}

	if !confirmAction(fmt.Sprintf("⚠️  Delete topic '%s'? This action CANNOT be undone!", topicName)) {
		fmt.Println("❌ Cancelled")
		return
	}

	err = admin.DeleteTopic(topicName)
	if err != nil {
		log.Fatalf("Failed to delete topic: %v", err)
	}

	fmt.Printf("✅ Topic '%s' deleted successfully\n", topicName)
}

func cmdIncreasePartitions(brokers []string, config *sarama.Config, topicName string, newCount int) {
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	// 获取当前分区数
	metadata, err := admin.DescribeTopics([]string{topicName})
	if err != nil {
		log.Fatalf("Failed to describe topic: %v", err)
	}

	if len(metadata) == 0 {
		log.Fatal("Topic not found")
	}

	currentPartitions := len(metadata[0].Partitions)

	if newCount <= currentPartitions {
		log.Fatalf("New partition count (%d) must be greater than current (%d)", newCount, currentPartitions)
	}

	if dryRun {
		fmt.Printf("🔍 [DRY RUN] Would increase partitions:\n")
		fmt.Printf("   Topic: %s\n", topicName)
		fmt.Printf("   Current: %d → New: %d\n", currentPartitions, newCount)
		return
	}

	if !confirmAction(fmt.Sprintf("Increase partitions for '%s' from %d to %d?", topicName, currentPartitions, newCount)) {
		fmt.Println("❌ Cancelled")
		return
	}

	err = admin.CreatePartitions(topicName, int32(newCount), nil, false)
	if err != nil {
		log.Fatalf("Failed to increase partitions: %v", err)
	}

	fmt.Printf("✅ Partitions increased: %d → %d\n", currentPartitions, newCount)
}

func cmdResetOffset(brokers []string, config *sarama.Config, groupName, topicName string, newOffset int64) {
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	if dryRun {
		fmt.Printf("🔍 [DRY RUN] Would reset offset:\n")
		fmt.Printf("   Group: %s\n", groupName)
		fmt.Printf("   Topic: %s\n", topicName)
		fmt.Printf("   New Offset: %d\n", newOffset)
		return
	}

	if !confirmAction(fmt.Sprintf("Reset offset for group '%s' on topic '%s' to %d?", groupName, topicName, newOffset)) {
		fmt.Println("❌ Cancelled")
		return
	}

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	partitions, err := client.Partitions(topicName)
	if err != nil {
		log.Fatalf("Failed to get partitions: %v", err)
	}

	offsets := make(map[string]map[int32]int64)
	offsets[topicName] = make(map[int32]int64)

	for _, p := range partitions {
		offsets[topicName][p] = newOffset
	}

	coordinator, err := client.Coordinator(groupName)
	if err != nil {
		log.Fatalf("Failed to get coordinator: %v", err)
	}

	request := &sarama.OffsetCommitRequest{
		Version:                 2,
		ConsumerGroup:           groupName,
		ConsumerGroupGeneration: -1,
	}

	for topic, partitionOffsets := range offsets {
		for partition, offset := range partitionOffsets {
			request.AddBlock(topic, partition, offset, 0, "")
		}
	}

	_, err = coordinator.CommitOffset(request)
	if err != nil {
		log.Fatalf("Failed to commit offset: %v", err)
	}

	fmt.Printf("✅ Offset reset successfully for group '%s'\n", groupName)
}

func cmdDeleteGroup(brokers []string, config *sarama.Config, groupName string) {
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	if dryRun {
		fmt.Printf("🔍 [DRY RUN] Would delete consumer group: %s\n", groupName)
		return
	}

	if !confirmAction(fmt.Sprintf("Delete consumer group '%s'?", groupName)) {
		fmt.Println("❌ Cancelled")
		return
	}

	err = admin.DeleteConsumerGroup(groupName)
	if err != nil {
		log.Fatalf("Failed to delete group: %v", err)
	}

	fmt.Printf("✅ Consumer group '%s' deleted\n", groupName)
}

func cmdDescribeGroup(brokers []string, config *sarama.Config, groupName string) {
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	groups, err := admin.DescribeConsumerGroups([]string{groupName})
	if err != nil {
		log.Fatalf("Failed to describe group: %v", err)
	}

	if len(groups) == 0 {
		log.Fatal("Group not found")
	}

	group := groups[0]

	fmt.Printf("👥 Consumer Group: %s\n\n", groupName)
	fmt.Printf("State: %s\n", group.State)
	fmt.Printf("Protocol Type: %s\n", group.ProtocolType)
	fmt.Printf("Protocol: %s\n", group.Protocol)
	fmt.Printf("Members: %d\n\n", len(group.Members))

	if len(group.Members) > 0 {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "MEMBER ID\tCLIENT ID\tHOST\tASSIGNED PARTITIONS")

		for _, member := range group.Members {
			assignment, _ := member.GetMemberAssignment()
			partitionsStr := ""
			for topic, partitions := range assignment.Topics {
				partitionsStr += fmt.Sprintf("%s:%v ", topic, partitions)
			}

			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
				member.MemberId, member.ClientId, member.ClientHost, partitionsStr)
		}
		w.Flush()
	}
}

func cmdMonitor(brokers []string, config *sarama.Config, interval int) {
	fmt.Println("📊 Real-time Cluster Monitor (Press Ctrl+C to stop)")
	fmt.Println(strings.Repeat("=", 80))

	for {
		admin, err := sarama.NewClusterAdmin(brokers, config)
		if err != nil {
			log.Printf("Error: %v", err)
			time.Sleep(time.Duration(interval) * time.Second)
			continue
		}

		// Clear screen (Unix/Linux)
		fmt.Print("\033[H\033[2J")

		timestamp := time.Now().Format("2006-01-02 15:04:05")
		fmt.Printf("⏰ %s | 🔄 Refresh: %ds\n\n", timestamp, interval)

		// Broker status
		brokersList, controllerID, _ := admin.DescribeCluster()
		fmt.Printf("🖥️  Brokers: %d online | Controller: %d\n", len(brokersList), controllerID)

		// Topics summary
		topics, _ := admin.ListTopics()
		var totalPartitions int32
		for _, detail := range topics {
			totalPartitions += detail.NumPartitions
		}
		fmt.Printf("📂 Topics: %d | Partitions: %d\n", len(topics), totalPartitions)

		// Consumer groups
		groups, _ := admin.ListConsumerGroups()
		fmt.Printf("👥 Consumer Groups: %d\n\n", len(groups))

		// Top lagging groups
		fmt.Println("🔝 Top Lagging Groups:")
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "GROUP\tSTATE\tLAG\tSTATUS")

		groupLags := make(map[string]int64)
		count := 0
		for groupID := range groups {
			if count >= 10 {
				break
			}
			lag := calculateGroupLag(brokers, config, groupID)
			groupLags[groupID] = lag
			count++
		}

		// Sort by lag
		type groupLag struct {
			name string
			lag  int64
		}
		var sortedLags []groupLag
		for name, lag := range groupLags {
			sortedLags = append(sortedLags, groupLag{name, lag})
		}
		sort.Slice(sortedLags, func(i, j int) bool {
			return sortedLags[i].lag > sortedLags[j].lag
		})

		for _, gl := range sortedLags {
			status := "✅"
			if gl.lag > alertThreshold {
				status = "⚠️"
			}

			groupDescs, _ := admin.DescribeConsumerGroups([]string{gl.name})
			state := "Unknown"
			if len(groupDescs) > 0 {
				state = groupDescs[0].State
			}

			fmt.Fprintf(w, "%s\t%s\t%d\t%s\n", gl.name, state, gl.lag, status)
		}
		w.Flush()

		admin.Close()
		time.Sleep(time.Duration(interval) * time.Second)
	}
}

func cmdMonitorLag(brokers []string, config *sarama.Config, groupName string, interval int, threshold int64) {
	fmt.Printf("📊 Monitoring Consumer Group Lag: %s (Threshold: %d)\n", groupName, threshold)
	fmt.Printf("Press Ctrl+C to stop\n\n")

	for {
		lag := calculateGroupLag(brokers, config, groupName)
		timestamp := time.Now().Format("15:04:05")

		status := "✅ OK"
		if lag > threshold {
			status = "⚠️  WARNING"
		}

		fmt.Printf("[%s] Lag: %d | %s\n", timestamp, lag, status)

		if lag > threshold {
			// 可以在这里添加告警逻辑
			fmt.Printf("        🚨 ALERT: Lag exceeds threshold!\n")
		}

		time.Sleep(time.Duration(interval) * time.Second)
	}
}

func cmdDiskUsage(brokers []string, config *sarama.Config) {
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	topics, err := admin.ListTopics()
	if err != nil {
		log.Fatalf("Failed to list topics: %v", err)
	}

	type topicSize struct {
		name string
		size int64
	}

	var topicSizes []topicSize
	var totalSize int64

	for name := range topics {
		if !showInternal && strings.HasPrefix(name, "__") {
			continue
		}

		size := getTopicSize(brokers, config, name)
		totalSize += size
		topicSizes = append(topicSizes, topicSize{name, size})
	}

	sort.Slice(topicSizes, func(i, j int) bool {
		return topicSizes[i].size > topicSizes[j].size
	})

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintf(w, "💾 Disk Usage Summary (Total: %s)\n\n", formatBytes(totalSize))
	fmt.Fprintln(w, "TOPIC\tMESSAGES\tSIZE\tPERCENTAGE")

	for _, ts := range topicSizes {
		percentage := float64(ts.size) / float64(totalSize) * 100
		fmt.Fprintf(w, "%s\t%d\t%s\t%.2f%%\n",
			ts.name, ts.size, formatBytes(ts.size), percentage)
	}

	fmt.Fprintf(w, "\nTOTAL\t-\t%s\t100.00%%\n", formatBytes(totalSize))
	w.Flush()
}

func cmdUnderReplicated(brokers []string, config *sarama.Config) {
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	topics, err := admin.ListTopics()
	if err != nil {
		log.Fatalf("Failed to list topics: %v", err)
	}

	fmt.Println("🔍 Checking for Under-Replicated Partitions...")

	var underRepPartitions []struct {
		topic     string
		partition int32
		replicas  int
		isr       int
	}

	for topicName := range topics {
		metadata, err := admin.DescribeTopics([]string{topicName})
		if err != nil {
			continue
		}

		for _, topicMeta := range metadata {
			for _, partMeta := range topicMeta.Partitions {
				if len(partMeta.Isr) < len(partMeta.Replicas) {
					underRepPartitions = append(underRepPartitions, struct {
						topic     string
						partition int32
						replicas  int
						isr       int
					}{topicName, partMeta.ID, len(partMeta.Replicas), len(partMeta.Isr)})
				}
			}
		}
	}

	if len(underRepPartitions) == 0 {
		fmt.Println("✅ All partitions are fully replicated!")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintf(w, "⚠️  Found %d Under-Replicated Partitions:\n\n", len(underRepPartitions))
	fmt.Fprintln(w, "TOPIC\tPARTITION\tREPLICAS\tIN-SYNC\tMISSING")

	for _, urp := range underRepPartitions {
		missing := urp.replicas - urp.isr
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\n",
			urp.topic, urp.partition, urp.replicas, urp.isr, missing)
	}
	w.Flush()
}

func cmdOfflinePartitions(brokers []string, config *sarama.Config) {
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	topics, err := admin.ListTopics()
	if err != nil {
		log.Fatalf("Failed to list topics: %v", err)
	}

	fmt.Println("🔍 Checking for Offline Partitions...")

	var offlinePartitions []struct {
		topic     string
		partition int32
		leader    int32
	}

	for topicName := range topics {
		metadata, err := admin.DescribeTopics([]string{topicName})
		if err != nil {
			continue
		}

		for _, topicMeta := range metadata {
			for _, partMeta := range topicMeta.Partitions {
				if partMeta.Leader == -1 {
					offlinePartitions = append(offlinePartitions, struct {
						topic     string
						partition int32
						leader    int32
					}{topicName, partMeta.ID, partMeta.Leader})
				}
			}
		}
	}

	if len(offlinePartitions) == 0 {
		fmt.Println("✅ No offline partitions found!")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintf(w, "❌ Found %d Offline Partitions:\n\n", len(offlinePartitions))
	fmt.Fprintln(w, "TOPIC\tPARTITION\tSTATUS")

	for _, op := range offlinePartitions {
		fmt.Fprintf(w, "%s\t%d\t❌ No Leader\n", op.topic, op.partition)
	}
	w.Flush()
}

func cmdAlterTopic(brokers []string, config *sarama.Config, topicName string) {
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	if dryRun {
		fmt.Printf("🔍 [DRY RUN] Would alter topic config for: %s\n", topicName)
		fmt.Printf("   Retention: %dh\n", retentionHours)
		fmt.Printf("   Cleanup Policy: %s\n", cleanupPolicy)
		fmt.Printf("   Compression: %s\n", compressionType)
		return
	}

	if !confirmAction(fmt.Sprintf("Alter configuration for topic '%s'?", topicName)) {
		fmt.Println("❌ Cancelled")
		return
	}

	configEntries := make(map[string]*string)
	configEntries["retention.ms"] = stringPtr(fmt.Sprintf("%d", retentionHours*3600000))
	configEntries["cleanup.policy"] = stringPtr(cleanupPolicy)
	configEntries["compression.type"] = stringPtr(compressionType)

	err = admin.AlterConfig(sarama.TopicResource, topicName, configEntries, false)
	if err != nil {
		log.Fatalf("Failed to alter topic: %v", err)
	}

	fmt.Printf("✅ Topic '%s' configuration updated\n", topicName)
}

func cmdSetConfig(brokers []string, config *sarama.Config) {
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	resourceName := topic
	if configType == "broker" {
		resourceName = fmt.Sprintf("%d", brokerID)
	}

	if resourceName == "" {
		log.Fatal("Please specify resource name (-topic or -broker-id)")
	}

	configEntries := make(map[string]*string)
	if retentionHours > 0 {
		configEntries["retention.ms"] = stringPtr(fmt.Sprintf("%d", retentionHours*3600000))
	}
	if cleanupPolicy != "" {
		configEntries["cleanup.policy"] = stringPtr(cleanupPolicy)
	}

	if dryRun {
		fmt.Printf("🔍 [DRY RUN] Would set config for %s: %s\n", configType, resourceName)
		for k, v := range configEntries {
			fmt.Printf("   %s = %s\n", k, *v)
		}
		return
	}

	var resourceType sarama.ConfigResourceType
	if configType == "topic" {
		resourceType = sarama.TopicResource
	} else {
		resourceType = sarama.BrokerResource
	}

	err = admin.AlterConfig(resourceType, resourceName, configEntries, false)
	if err != nil {
		log.Fatalf("Failed to set config: %v", err)
	}

	fmt.Printf("✅ Configuration updated for %s: %s\n", configType, resourceName)
}

func cmdCompareConfig(brokers []string, config *sarama.Config) {
	if topic == "" {
		log.Fatal("Please specify topics to compare (comma-separated): -topic topic1,topic2")
	}

	topics := strings.Split(topic, ",")
	if len(topics) < 2 {
		log.Fatal("Please specify at least 2 topics to compare")
	}

	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	fmt.Printf("🔍 Comparing configurations for: %v\n\n", topics)

	configs := make(map[string]map[string]string)
	for _, t := range topics {
		configEntries, err := admin.DescribeConfig(sarama.ConfigResource{
			Type: sarama.TopicResource,
			Name: t,
		})
		if err != nil {
			log.Printf("Warning: Could not get config for %s: %v", t, err)
			continue
		}

		configs[t] = make(map[string]string)
		for _, entry := range configEntries {
			if entry.Value != "" {
				configs[t][entry.Name] = entry.Value
			}
		}
	}

	// Find all config keys
	allKeys := make(map[string]bool)
	for _, cfg := range configs {
		for key := range cfg {
			allKeys[key] = true
		}
	}

	// Print comparison
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	header := "CONFIG KEY"
	for _, t := range topics {
		header += "\t" + t
	}
	fmt.Fprintln(w, header)

	var keys []string
	for key := range allKeys {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		line := key
		different := false
		var firstValue string

		for i, t := range topics {
			value := configs[t][key]
			if value == "" {
				value = "-"
			}
			line += "\t" + value

			if i == 0 {
				firstValue = value
			} else if value != firstValue {
				different = true
			}
		}

		if different {
			line += " ⚠️"
		}
		fmt.Fprintln(w, line)
	}
	w.Flush()
}

func cmdMirrorTopic(brokers []string, config *sarama.Config, sourceTopic, destTopic string) {
	fmt.Printf("🔄 Mirroring topic: %s → %s\n", sourceTopic, destTopic)

	if dryRun {
		fmt.Println("🔍 [DRY RUN] Would mirror messages")
		return
	}

	// Create consumer
	config.Consumer.Return.Errors = true
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Create producer
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	partitions, err := consumer.Partitions(sourceTopic)
	if err != nil {
		log.Fatalf("Failed to get partitions: %v", err)
	}

	var wg sync.WaitGroup
	totalMirrored := 0
	var mu sync.Mutex

	for _, partition := range partitions {
		wg.Add(1)
		go func(p int32) {
			defer wg.Done()

			pc, err := consumer.ConsumePartition(sourceTopic, p, sarama.OffsetOldest)
			if err != nil {
				log.Printf("Error consuming partition %d: %v", p, err)
				return
			}
			defer pc.Close()

			for msg := range pc.Messages() {
				newMsg := &sarama.ProducerMessage{
					Topic:     destTopic,
					Key:       sarama.ByteEncoder(msg.Key),
					Value:     sarama.ByteEncoder(msg.Value),
					Timestamp: msg.Timestamp,
				}

				_, _, err := producer.SendMessage(newMsg)
				if err != nil {
					log.Printf("Error producing message: %v", err)
					continue
				}

				mu.Lock()
				totalMirrored++
				if totalMirrored%1000 == 0 {
					fmt.Printf("  Mirrored: %d messages\n", totalMirrored)
				}
				mu.Unlock()
			}
		}(partition)
	}

	wg.Wait()
	fmt.Printf("✅ Mirrored %d messages from %s to %s\n", totalMirrored, sourceTopic, destTopic)
}

func cmdBackupTopic(brokers []string, config *sarama.Config, topicName, output string) {
	if output == "" {
		output = fmt.Sprintf("%s_backup_%s.json", topicName, time.Now().Format("20060102_150405"))
	}

	fmt.Printf("💾 Backing up topic: %s → %s\n", topicName, output)

	config.Consumer.Return.Errors = true
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions(topicName)
	if err != nil {
		log.Fatalf("Failed to get partitions: %v", err)
	}

	file, err := os.Create(output)
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	file.WriteString("[")

	var wg sync.WaitGroup
	totalBackedUp := 0
	var mu sync.Mutex
	first := true

	for _, partition := range partitions {
		wg.Add(1)
		go func(p int32) {
			defer wg.Done()

			pc, err := consumer.ConsumePartition(topicName, p, sarama.OffsetOldest)
			if err != nil {
				return
			}
			defer pc.Close()

			for msg := range pc.Messages() {
				record := map[string]interface{}{
					"partition": msg.Partition,
					"offset":    msg.Offset,
					"key":       string(msg.Key),
					"value":     string(msg.Value),
					"timestamp": msg.Timestamp,
				}

				mu.Lock()
				if !first {
					file.WriteString(",")
				}
				first = false
				encoder.Encode(record)
				totalBackedUp++
				if totalBackedUp%1000 == 0 {
					fmt.Printf("  Backed up: %d messages\n", totalBackedUp)
				}
				mu.Unlock()
			}
		}(partition)
	}

	wg.Wait()
	file.WriteString("]")

	fmt.Printf("✅ Backed up %d messages to %s\n", totalBackedUp, output)
}

func cmdPurgeTopic(brokers []string, config *sarama.Config, topicName string) {
	if dryRun {
		fmt.Printf("🔍 [DRY RUN] Would purge all messages from topic: %s\n", topicName)
		return
	}

	if !confirmAction(fmt.Sprintf("⚠️  Purge ALL messages from topic '%s'?", topicName)) {
		fmt.Println("❌ Cancelled")
		return
	}

	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	// Set retention to 1ms temporarily
	configEntries := make(map[string]*string)
	configEntries["retention.ms"] = stringPtr("1")

	err = admin.AlterConfig(sarama.TopicResource, topicName, configEntries, false)
	if err != nil {
		log.Fatalf("Failed to purge topic: %v", err)
	}

	fmt.Println("⏳ Waiting for messages to be purged...")
	time.Sleep(5 * time.Second)

	// Restore original retention
	configEntries["retention.ms"] = stringPtr(fmt.Sprintf("%d", retentionHours*3600000))
	admin.AlterConfig(sarama.TopicResource, topicName, configEntries, false)

	fmt.Printf("✅ Topic '%s' purged successfully\n", topicName)
}

func cmdThroughput(brokers []string, config *sarama.Config, topicName string) {
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	if topicName == "" {
		fmt.Println("📊 Cluster-wide Throughput Analysis")
	} else {
		fmt.Printf("📊 Throughput Analysis for Topic: %s\n", topicName)
	}

	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	topics := []string{topicName}
	if topicName == "" {
		allTopics, _ := admin.ListTopics()
		for name := range allTopics {
			if !strings.HasPrefix(name, "__") {
				topics = append(topics, name)
			}
		}
	}

	fmt.Println("\nMeasuring throughput over 10 seconds...")

	type throughputData struct {
		topic          string
		startMessages  int64
		endMessages    int64
		messagesPerSec float64
	}

	var results []throughputData

	for _, t := range topics {
		partitions, _ := client.Partitions(t)
		var startTotal, endTotal int64

		// Initial count
		for _, p := range partitions {
			offset, _ := client.GetOffset(t, p, sarama.OffsetNewest)
			startTotal += offset
		}

		time.Sleep(10 * time.Second)

		// Final count
		for _, p := range partitions {
			offset, _ := client.GetOffset(t, p, sarama.OffsetNewest)
			endTotal += offset
		}

		diff := endTotal - startTotal
		perSec := float64(diff) / 10.0

		results = append(results, throughputData{
			topic:          t,
			startMessages:  startTotal,
			endMessages:    endTotal,
			messagesPerSec: perSec,
		})
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "\nTOPIC\tSTART\tEND\tDIFF\tMSG/SEC")

	for _, r := range results {
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%.2f\n",
			r.topic, r.startMessages, r.endMessages,
			r.endMessages-r.startMessages, r.messagesPerSec)
	}
	w.Flush()
}

func cmdGenerateReport(brokers []string, config *sarama.Config, output string) {
	if output == "" {
		output = fmt.Sprintf("kafka_report_%s.html", time.Now().Format("20060102_150405"))
	}

	fmt.Printf("📄 Generating cluster report → %s\n", output)

	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	// Collect data
	brokersList, controllerID, _ := admin.DescribeCluster()
	topics, _ := admin.ListTopics()
	groups, _ := admin.ListConsumerGroups()

	// Generate HTML report
	html := fmt.Sprintf(`
<!DOCTYPE html>
<html>
<head>
	<title>Kafka Cluster Report</title>
	<style>
		body { font-family: Arial, sans-serif; margin: 20px; }
		h1 { color: #333; }
		table { border-collapse: collapse; width: 100%%; margin: 20px 0; }
		th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
		th { background-color: #4CAF50; color: white; }
		.metric { background: #f0f0f0; padding: 15px; margin: 10px 0; border-radius: 5px; }
	</style>
</head>
<body>
	<h1>Kafka Cluster Report</h1>
	<p>Generated: %s</p>
	
	<div class="metric">
		<h2>Cluster Overview</h2>
		<p>Brokers: %d</p>
		<p>Controller: %d</p>
		<p>Topics: %d</p>
		<p>Consumer Groups: %d</p>
	</div>

	<h2>Topics</h2>
	<table>
		<tr><th>Topic</th><th>Partitions</th><th>Replication Factor</th></tr>
`, time.Now().Format("2006-01-02 15:04:05"), len(brokersList), controllerID, len(topics), len(groups))

	for name, detail := range topics {
		if !strings.HasPrefix(name, "__") {
			html += fmt.Sprintf("<tr><td>%s</td><td>%d</td><td>%d</td></tr>\n",
				name, detail.NumPartitions, detail.ReplicationFactor)
		}
	}

	html += `
	</table>
</body>
</html>
`

	err = os.WriteFile(output, []byte(html), 0644)
	if err != nil {
		log.Fatalf("Failed to write report: %v", err)
	}

	fmt.Printf("✅ Report generated: %s\n", output)
}

// Helper function implementations from original code...
func getTopicSize(brokers []string, config *sarama.Config, topic string) int64 {
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return 0
	}
	defer client.Close()

	partitions, err := client.Partitions(topic)
	if err != nil {
		return 0
	}

	var totalSize int64
	for _, p := range partitions {
		oldest, _ := client.GetOffset(topic, p, sarama.OffsetOldest)
		newest, _ := client.GetOffset(topic, p, sarama.OffsetNewest)
		totalSize += (newest - oldest)
	}
	return totalSize
}

func calculateGroupLag(brokers []string, config *sarama.Config, groupID string) int64 {
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		return 0
	}
	defer admin.Close()

	offsets, err := admin.ListConsumerGroupOffsets(groupID, nil)
	if err != nil {
		return 0
	}

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return 0
	}
	defer client.Close()

	var totalLag int64
	for topic, partitions := range offsets.Blocks {
		for partition, block := range partitions {
			endOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				continue
			}
			current := block.Offset
			if current == -1 {
				current = 0
			}
			lag := endOffset - current
			if lag > 0 {
				totalLag += lag
			}
		}
	}
	return totalLag
}

func formatBytes(bytes int64) string {
	const unit = 1000
	if bytes < unit {
		return fmt.Sprintf("%d", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%c", float64(bytes)/float64(div), "kMGTPE"[exp])
}

func stringPtr(s string) *string {
	return &s
}

type MessageInfo struct {
	Partition int32
	Offset    int64
	Key       string
	Value     string
	Timestamp time.Time
}

// Include all previous command implementations (cmdListTopics, cmdTopicDetails, etc.)
// ... (keeping all the original functions from the previous code)

func cmdRestoreData(brokers []string, config *sarama.Config, topicName, inputFile string) {
	fmt.Println("⚠️  Restore command not yet implemented")
}

func cmdProducerPerf(brokers []string, config *sarama.Config, topicName string, count int) {
	fmt.Println("⚠️  Producer performance test not yet implemented")
}

func cmdConsumerPerf(brokers []string, config *sarama.Config, topicName string, count int) {
	fmt.Println("⚠️  Consumer performance test not yet implemented")
}

func cmdBatchCreateTopics(brokers []string, config *sarama.Config, configFile string) {
	fmt.Println("⚠️  Batch create not yet implemented")
}

func cmdBatchDeleteTopics(brokers []string, config *sarama.Config, configFile string) {
	fmt.Println("⚠️  Batch delete not yet implemented")
}

func cmdBatchAlterConfig(brokers []string, config *sarama.Config, configFile string) {
	fmt.Println("⚠️  Batch alter not yet implemented")
}

func cmdAddACL(brokers []string, config *sarama.Config) {
	fmt.Println("⚠️  Add ACL not yet implemented")
}

func cmdRemoveACL(brokers []string, config *sarama.Config) {
	fmt.Println("⚠️  Remove ACL not yet implemented")
}

func cmdAuditCluster(brokers []string, config *sarama.Config, output string) {
	fmt.Println("⚠️  Audit command not yet implemented")
}

// Original functions from previous code...

func cmdShowConfig(brokers []string, config *sarama.Config) {
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin client: %v", err)
	}
	defer admin.Close()

	switch configType {
	case "topic":
		if topic != "" {
			// 显示指定 topic 的配置
			cmdShowTopicConfig(admin, topic)
		} else {
			// 显示所有 topic 的关键配置
			cmdShowAllTopicsConfig(admin)
		}
	case "broker":
		cmdShowBrokerConfig(admin, brokers, config)
	default:
		log.Fatalf("Invalid config type: %s (use 'topic' or 'broker')", configType)
	}
}

func cmdShowTopicConfig(admin sarama.ClusterAdmin, topicName string) {
	configEntries, err := admin.DescribeConfig(sarama.ConfigResource{
		Type: sarama.TopicResource,
		Name: topicName,
	})
	if err != nil {
		log.Fatalf("Failed to get topic config: %v", err)
	}

	configMap := make(map[string]string)
	for _, entry := range configEntries {
		if entry.Value != "" {
			configMap[entry.Name] = entry.Value
		}
	}

	if export == "json" {
		exportTopicConfigJSON(topicName, configMap)
	} else if export == "csv" {
		exportTopicConfigCSV(topicName, configMap)
	} else {
		printTopicConfig(topicName, configMap)
	}
}

func cmdShowAllTopicsConfig(admin sarama.ClusterAdmin) {
	topics, err := admin.ListTopics()
	if err != nil {
		log.Fatalf("Failed to list topics: %v", err)
	}

	var topicConfigs []TopicConfigInfo

	for name := range topics {
		if !showInternal && strings.HasPrefix(name, "__") {
			continue
		}
		if filter != "" && !strings.Contains(name, filter) {
			continue
		}

		configEntries, err := admin.DescribeConfig(sarama.ConfigResource{
			Type: sarama.TopicResource,
			Name: name,
		})
		if err != nil {
			log.Printf("Warning: Could not get config for topic %s: %v", name, err)
			continue
		}

		configMap := make(map[string]string)
		// 只显示关键配置
		keyConfigs := []string{
			"retention.ms", "retention.bytes", "segment.ms", "segment.bytes",
			"cleanup.policy", "compression.type", "min.insync.replicas",
			"max.message.bytes", "min.compaction.lag.ms",
		}

		for _, entry := range configEntries {
			for _, key := range keyConfigs {
				if entry.Name == key && entry.Value != "" {
					configMap[entry.Name] = entry.Value
				}
			}
		}

		topicConfigs = append(topicConfigs, TopicConfigInfo{
			Topic:  name,
			Config: configMap,
		})
	}

	sort.Slice(topicConfigs, func(i, j int) bool {
		return topicConfigs[i].Topic < topicConfigs[j].Topic
	})

	if export == "json" {
		exportAllTopicsConfigJSON(topicConfigs)
	} else if export == "csv" {
		exportAllTopicsConfigCSV(topicConfigs)
	} else {
		printAllTopicsConfig(topicConfigs)
	}
}

func cmdShowBrokerConfig(admin sarama.ClusterAdmin, brokers []string, config *sarama.Config) {
	brokerInfo, _, err := admin.DescribeCluster()
	if err != nil {
		log.Fatalf("Failed to get cluster info: %v", err)
	}

	var brokerConfigs []BrokerConfigInfo

	for _, broker := range brokerInfo {
		// 如果指定了 broker-id，只显示该 broker
		if brokerID >= 0 && int(broker.ID()) != brokerID {
			continue
		}

		configEntries, err := admin.DescribeConfig(sarama.ConfigResource{
			Type: sarama.BrokerResource,
			Name: strconv.Itoa(int(broker.ID())),
		})
		if err != nil {
			log.Printf("Warning: Could not get config for broker %d: %v", broker.ID(), err)
			continue
		}

		configMap := make(map[string]string)
		// 只显示关键配置
		keyConfigs := []string{
			"log.retention.hours", "log.retention.bytes", "log.segment.bytes",
			"num.network.threads", "num.io.threads", "socket.send.buffer.bytes",
			"socket.receive.buffer.bytes", "num.replica.fetchers",
			"replica.lag.time.max.ms", "auto.create.topics.enable",
		}

		for _, entry := range configEntries {
			for _, key := range keyConfigs {
				if entry.Name == key && entry.Value != "" {
					configMap[entry.Name] = entry.Value
				}
			}
		}

		brokerConfigs = append(brokerConfigs, BrokerConfigInfo{
			BrokerID: broker.ID(),
			Host:     broker.Addr(),
			Port:     int32(broker.ID()), // Port info may not be directly available
			Config:   configMap,
		})
	}

	if export == "json" {
		exportBrokerConfigJSON(brokerConfigs)
	} else if export == "csv" {
		exportBrokerConfigCSV(brokerConfigs)
	} else {
		printBrokerConfig(brokerConfigs)
	}
}

func printTopicConfig(topicName string, config map[string]string) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintf(w, "📋 Configuration for Topic: %s\n\n", topicName)
	fmt.Fprintln(w, "CONFIG KEY\tVALUE\tDESCRIPTION")

	// 按字母顺序排序
	var keys []string
	for k := range config {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		desc := getConfigDescription(k)
		value := config[k]

		// 格式化某些特殊值
		if strings.HasSuffix(k, ".ms") {
			if ms, err := strconv.ParseInt(value, 10, 64); err == nil {
				hours := ms / 3600000
				if hours > 0 {
					value = fmt.Sprintf("%s (%dh)", value, hours)
				}
			}
		} else if strings.HasSuffix(k, ".bytes") {
			if bytes, err := strconv.ParseInt(value, 10, 64); err == nil {
				value = fmt.Sprintf("%s (%s)", value, formatBytes(bytes))
			}
		}

		fmt.Fprintf(w, "%s\t%s\t%s\n", k, value, desc)
	}
	w.Flush()
}

func printAllTopicsConfig(configs []TopicConfigInfo) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintf(w, "📋 Topic Configurations Summary (%d topics)\n\n", len(configs))
	fmt.Fprintln(w, "TOPIC\tRETENTION\tCLEANUP\tCOMPRESSION\tMIN.ISR\tMAX.MSG.SIZE")

	for _, tc := range configs {
		retention := tc.Config["retention.ms"]
		if retention != "" {
			if ms, err := strconv.ParseInt(retention, 10, 64); err == nil {
				hours := ms / 3600000
				retention = fmt.Sprintf("%dh", hours)
			}
		} else {
			retention = "-"
		}

		cleanup := tc.Config["cleanup.policy"]
		if cleanup == "" {
			cleanup = "-"
		}

		compression := tc.Config["compression.type"]
		if compression == "" {
			compression = "-"
		}

		minISR := tc.Config["min.insync.replicas"]
		if minISR == "" {
			minISR = "-"
		}

		maxMsg := tc.Config["max.message.bytes"]
		if maxMsg != "" {
			if bytes, err := strconv.ParseInt(maxMsg, 10, 64); err == nil {
				maxMsg = formatBytes(bytes)
			}
		} else {
			maxMsg = "-"
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
			tc.Topic, retention, cleanup, compression, minISR, maxMsg)
	}
	w.Flush()
}

func printBrokerConfig(configs []BrokerConfigInfo) {
	for _, bc := range configs {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintf(w, "🖥️  Broker %d Configuration (%s)\n\n", bc.BrokerID, bc.Host)
		fmt.Fprintln(w, "CONFIG KEY\tVALUE\tDESCRIPTION")

		// 按字母顺序排序
		var keys []string
		for k := range bc.Config {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			desc := getConfigDescription(k)
			value := bc.Config[k]

			// 格式化某些特殊值
			if strings.HasSuffix(k, ".bytes") {
				if bytes, err := strconv.ParseInt(value, 10, 64); err == nil {
					value = fmt.Sprintf("%s (%s)", value, formatBytes(bytes))
				}
			}

			fmt.Fprintf(w, "%s\t%s\t%s\n", k, value, desc)
		}
		w.Flush()
		fmt.Println()
	}
}

func getConfigDescription(key string) string {
	descriptions := map[string]string{
		"retention.ms":                "消息保留时间",
		"retention.bytes":             "分区最大保留字节数",
		"segment.ms":                  "日志段滚动时间",
		"segment.bytes":               "日志段大小",
		"cleanup.policy":              "清理策略 (delete/compact)",
		"compression.type":            "压缩类型",
		"min.insync.replicas":         "最小同步副本数",
		"max.message.bytes":           "最大消息大小",
		"min.compaction.lag.ms":       "最小压缩延迟",
		"log.retention.hours":         "日志保留小时数",
		"log.retention.bytes":         "日志最大保留字节数",
		"log.segment.bytes":           "日志段大小",
		"num.network.threads":         "网络线程数",
		"num.io.threads":              "IO线程数",
		"socket.send.buffer.bytes":    "发送缓冲区大小",
		"socket.receive.buffer.bytes": "接收缓冲区大小",
		"num.replica.fetchers":        "副本拉取线程数",
		"replica.lag.time.max.ms":     "副本最大滞后时间",
		"auto.create.topics.enable":   "自动创建主题",
	}
	if desc, ok := descriptions[key]; ok {
		return desc
	}
	return ""
}

func exportTopicConfigJSON(topic string, config map[string]string) {
	data := map[string]interface{}{
		"topic":  topic,
		"config": config,
	}
	jsonBytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(jsonBytes))
}

func exportTopicConfigCSV(topic string, config map[string]string) {
	w := csv.NewWriter(os.Stdout)
	defer w.Flush()

	w.Write([]string{"Topic", "ConfigKey", "Value"})

	var keys []string
	for k := range config {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		w.Write([]string{topic, k, config[k]})
	}
}

func exportAllTopicsConfigJSON(configs []TopicConfigInfo) {
	jsonBytes, err := json.MarshalIndent(configs, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(jsonBytes))
}

func exportAllTopicsConfigCSV(configs []TopicConfigInfo) {
	w := csv.NewWriter(os.Stdout)
	defer w.Flush()

	w.Write([]string{"Topic", "ConfigKey", "Value"})

	for _, tc := range configs {
		var keys []string
		for k := range tc.Config {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			w.Write([]string{tc.Topic, k, tc.Config[k]})
		}
	}
}

func exportBrokerConfigJSON(configs []BrokerConfigInfo) {
	jsonBytes, err := json.MarshalIndent(configs, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(jsonBytes))
}

func exportBrokerConfigCSV(configs []BrokerConfigInfo) {
	w := csv.NewWriter(os.Stdout)
	defer w.Flush()

	w.Write([]string{"BrokerID", "Host", "ConfigKey", "Value"})

	for _, bc := range configs {
		var keys []string
		for k := range bc.Config {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			w.Write([]string{
				strconv.Itoa(int(bc.BrokerID)),
				bc.Host,
				k,
				bc.Config[k],
			})
		}
	}
}

func cmdListTopics(brokers []string, config *sarama.Config) {
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin client: %v", err)
	}
	defer admin.Close()

	topics, err := admin.ListTopics()
	if err != nil {
		log.Fatalf("Failed to list topics: %v", err)
	}

	brokersList, _, err := admin.DescribeCluster()
	if err != nil {
		log.Printf("Warning: Could not get cluster info: %v", err)
	}

	var topicList []TopicInfo
	for name, detail := range topics {
		if !showInternal && strings.HasPrefix(name, "__") {
			continue
		}
		if filter != "" && !strings.Contains(name, filter) {
			continue
		}

		topicList = append(topicList, TopicInfo{
			Name:         name,
			Partitions:   detail.NumPartitions,
			Replicas:     detail.ReplicationFactor,
			Configs:      detail.ConfigEntries,
			Internal:     strings.HasPrefix(name, "__"),
			TotalLogSize: getTopicSize(brokers, config, name),
		})
	}

	sort.Slice(topicList, func(i, j int) bool {
		return topicList[i].Name < topicList[j].Name
	})

	if export == "csv" {
		exportToCSV(topicList)
	} else if export == "json" {
		exportToJSON(topicList)
	} else {
		printTopicTable(topicList, len(brokersList))
	}
}

func printTopicTable(topics []TopicInfo, totalBrokers int) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintf(w, "Found %d topics (Brokers: %d)\n\n", len(topics), totalBrokers)
	fmt.Fprintln(w, "TOPIC\tPARTITIONS\tREPLICAS\tLOG SIZE\tINTERNAL\tCONFIG")

	for _, t := range topics {
		internal := "No"
		if t.Internal {
			internal = "Yes"
		}

		configSummary := ""
		if retention, ok := t.Configs["retention.ms"]; ok && retention != nil {
			retentionHours, _ := strconv.Atoi(*retention)
			retentionHours = retentionHours / 3600000
			configSummary = fmt.Sprintf("retention=%dh", retentionHours)
		}

		sizeStr := formatBytes(t.TotalLogSize)
		fmt.Fprintf(w, "%s\t%d\t%d\t%s\t%s\t%s\n",
			t.Name, t.Partitions, t.Replicas, sizeStr, internal, configSummary)
	}
	w.Flush()
}

type PartitionDetail struct {
	Partition       int32
	Leader          int32
	Replicas        []int32
	ISR             []int32
	OldestOffset    int64
	NewestOffset    int64
	LogSize         int64
	UnderReplicated bool
}

func cmdTopicDetails(brokers []string, config *sarama.Config, topicName string) {
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	partitions, err := client.Partitions(topicName)
	if err != nil {
		log.Fatalf("Failed to get partitions: %v", err)
	}

	metadata, err := admin.DescribeTopics([]string{topicName})
	if err != nil {
		log.Printf("Warning: Could not get topic metadata: %v", err)
	}

	var details []PartitionDetail
	var totalSize int64

	for _, p := range partitions {
		oldest, _ := client.GetOffset(topicName, p, sarama.OffsetOldest)
		newest, _ := client.GetOffset(topicName, p, sarama.OffsetNewest)
		size := newest - oldest
		totalSize += size

		detail := PartitionDetail{
			Partition:    p,
			OldestOffset: oldest,
			NewestOffset: newest,
			LogSize:      size,
		}

		if len(metadata) > 0 {
			for _, topicMetadata := range metadata {
				if topicMetadata.Name == topicName {
					for _, partitionMetadata := range topicMetadata.Partitions {
						if partitionMetadata.ID == p {
							detail.Leader = partitionMetadata.Leader
							detail.Replicas = partitionMetadata.Replicas
							detail.ISR = partitionMetadata.Isr
							detail.UnderReplicated = len(partitionMetadata.Isr) < len(partitionMetadata.Replicas)
							break
						}
					}
					break
				}
			}
		}

		details = append(details, detail)
	}

	if export == "csv" {
		exportPartitionDetailsToCSV(topicName, details, totalSize)
	} else {
		printPartitionDetails(topicName, details, totalSize)
	}
}

func printPartitionDetails(topic string, details []PartitionDetail, totalSize int64) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintf(w, "Topic: %s (Total messages: %d)\n\n", topic, totalSize)
	fmt.Fprintln(w, "PARTITION\tLEADER\tREPLICAS\tISR\tUNDER-REP\tOLDEST\tNEWEST\tLOG SIZE")

	for _, d := range details {
		underRep := "No"
		if d.UnderReplicated {
			underRep = "Yes"
		}

		replicasStr := intSliceToString(d.Replicas)
		isrStr := intSliceToString(d.ISR)

		fmt.Fprintf(w, "%d\t%d\t%s\t%s\t%s\t%d\t%d\t%d\n",
			d.Partition, d.Leader, replicasStr, isrStr, underRep,
			d.OldestOffset, d.NewestOffset, d.LogSize)
	}

	fmt.Fprintf(w, "\nTOTAL\t-\t-\t-\t-\t-\t-\t%d\n", totalSize)
	w.Flush()
}

func cmdPeek(brokers []string, config *sarama.Config, topicName string, n int) {
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions(topicName)
	if err != nil {
		log.Fatalf("Failed to get partitions: %v", err)
	}

	var allMessages []MessageInfo
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, p := range partitions {
		wg.Add(1)
		go func(partition int32) {
			defer wg.Done()

			client, err := sarama.NewClient(brokers, config)
			if err != nil {
				return
			}
			defer client.Close()

			newestOffset, err := client.GetOffset(topicName, partition, sarama.OffsetNewest)
			if err != nil {
				return
			}

			startOffset := newestOffset - int64(n)
			if startOffset < 0 {
				startOffset = sarama.OffsetOldest
			}

			pc, err := consumer.ConsumePartition(topicName, partition, startOffset)
			if err != nil {
				return
			}
			defer pc.Close()

			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			defer cancel()

			count := 0
			for {
				select {
				case msg := <-pc.Messages():
					mu.Lock()
					allMessages = append(allMessages, MessageInfo{
						Partition: partition,
						Offset:    msg.Offset,
						Key:       string(msg.Key),
						Value:     string(msg.Value),
						Timestamp: msg.Timestamp,
					})
					mu.Unlock()
					count++

					if count >= n || msg.Offset >= newestOffset-1 {
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}(p)
	}

	wg.Wait()

	sort.Slice(allMessages, func(i, j int) bool {
		return allMessages[i].Timestamp.After(allMessages[j].Timestamp)
	})

	if len(allMessages) > n {
		allMessages = allMessages[:n]
	}

	printMessages(topicName, allMessages)
}

func printMessages(topic string, messages []MessageInfo) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintf(w, "Last %d messages from topic: %s\n\n", len(messages), topic)
	fmt.Fprintln(w, "TIMESTAMP\tPARTITION\tOFFSET\tKEY\tVALUE (truncated)")

	for _, msg := range messages {
		value := msg.Value
		fmt.Fprintf(w, "%s\t%d\t%d\t%s\t%s\n",
			msg.Timestamp.Format("15:04:05.000"),
			msg.Partition, msg.Offset, msg.Key, value)
	}
	w.Flush()
}

type GroupInfo struct {
	ID           string
	State        string
	Protocol     string
	ProtocolType string
	Members      int
	TotalLag     int64
}

func cmdListGroups(brokers []string, config *sarama.Config) {
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	groups, err := admin.ListConsumerGroups()
	if err != nil {
		log.Fatalf("Failed to list groups: %v", err)
	}

	var groupIDs []string
	for id := range groups {
		if filter != "" && !strings.Contains(id, filter) {
			continue
		}
		groupIDs = append(groupIDs, id)
	}

	if len(groupIDs) == 0 {
		fmt.Println("No consumer groups found")
		return
	}

	descriptions, err := admin.DescribeConsumerGroups(groupIDs)
	if err != nil {
		log.Fatalf("Failed to describe groups: %v", err)
	}

	var groupInfos []GroupInfo
	for _, desc := range descriptions {
		lag := calculateGroupLag(brokers, config, desc.GroupId)
		groupInfos = append(groupInfos, GroupInfo{
			ID:           desc.GroupId,
			State:        desc.State,
			Protocol:     desc.Protocol,
			ProtocolType: desc.ProtocolType,
			Members:      len(desc.Members),
			TotalLag:     lag,
		})
	}

	sort.Slice(groupInfos, func(i, j int) bool {
		return groupInfos[i].TotalLag > groupInfos[j].TotalLag
	})

	printGroupsTable(groupInfos)
}

func printGroupsTable(groups []GroupInfo) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintf(w, "Found %d consumer groups\n\n", len(groups))
	fmt.Fprintln(w, "GROUP ID\tSTATE\tPROTOCOL\tMEMBERS\tTOTAL LAG")

	for _, g := range groups {
		lagColor := ""
		if g.TotalLag > 1000 {
			lagColor = "⚠️ "
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s%d\n",
			g.ID, g.State, g.ProtocolType, g.Members, lagColor, g.TotalLag)
	}
	w.Flush()
}

type LagInfo struct {
	Topic      string
	Partition  int32
	Current    int64
	End        int64
	Lag        int64
	ConsumerID string
	ClientID   string
	Host       string
}

func cmdGroupLag(brokers []string, config *sarama.Config, groupID string) {
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	offsets, err := admin.ListConsumerGroupOffsets(groupID, nil)
	if err != nil {
		log.Fatalf("Failed to get group offsets: %v", err)
	}

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	var lagInfos []LagInfo
	var totalLag int64

	groups, _ := admin.DescribeConsumerGroups([]string{groupID})
	var members map[string]*sarama.GroupMemberDescription
	if len(groups) > 0 {
		members = groups[0].Members
	}

	for topic, partitions := range offsets.Blocks {
		for partition, block := range partitions {
			endOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				continue
			}

			current := block.Offset
			if current == -1 {
				current = 0
			}

			lag := endOffset - current
			if lag < 0 {
				lag = 0
			}
			totalLag += lag

			info := LagInfo{
				Topic:     topic,
				Partition: partition,
				Current:   current,
				End:       endOffset,
				Lag:       lag,
			}

			if members != nil {
				for _, member := range members {
					if assignment, err := member.GetMemberAssignment(); err == nil {
						for t, ps := range assignment.Topics {
							if t == topic {
								for _, p := range ps {
									if p == partition {
										info.ConsumerID = member.MemberId
										info.ClientID = member.ClientId
										info.Host = member.ClientHost
									}
								}
							}
						}
					}
				}
			}

			lagInfos = append(lagInfos, info)
		}
	}

	sort.Slice(lagInfos, func(i, j int) bool {
		if lagInfos[i].Lag != lagInfos[j].Lag {
			return lagInfos[i].Lag > lagInfos[j].Lag
		}
		return lagInfos[i].Topic < lagInfos[j].Topic
	})

	printLagTable(groupID, lagInfos, totalLag)
}

func printLagTable(groupID string, infos []LagInfo, totalLag int64) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)

	status := "✅"
	if totalLag > 1000 {
		status = "⚠️"
	} else if totalLag > 10000 {
		status = "❌"
	}

	fmt.Fprintf(w, "Consumer Group: %s %s (Total Lag: %d)\n\n", groupID, status, totalLag)
	fmt.Fprintln(w, "TOPIC\tPARTITION\tCURRENT\tEND\tLAG\tCONSUMER\tCLIENT ID\tHOST")

	for _, info := range infos {
		lagStr := fmt.Sprintf("%d", info.Lag)
		if info.Lag > 1000 {
			lagStr = fmt.Sprintf("⚠️ %d", info.Lag)
		}

		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%s\t%s\t%s\t%s\n",
			info.Topic, info.Partition, info.Current, info.End, lagStr,
			info.ConsumerID, info.ClientID, info.Host)
	}

	fmt.Fprintf(w, "\nTOTAL\t-\t-\t-\t%d\t-\t-\t-\n", totalLag)
	w.Flush()
}

func cmdProduce(brokers []string, config *sarama.Config, topic, key, data string, partition int32) {
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.StringEncoder(data),
		Timestamp: time.Now(),
	}

	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}
	if partition >= 0 {
		msg.Partition = partition
	}

	partitionResult, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	fmt.Printf("✅ Message sent successfully!\n")
	fmt.Printf("   Topic:     %s\n", topic)
	fmt.Printf("   Partition: %d\n", partitionResult)
	fmt.Printf("   Offset:    %d\n", offset)
	fmt.Printf("   Key:       %s\n", key)
	fmt.Printf("   Value:     %s\n", data)
	fmt.Printf("   Timestamp: %s\n", time.Now().Format("2006-01-02 15:04:05.000"))
}

func cmdListACLs(brokers []string, config *sarama.Config) {
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	filter := sarama.AclFilter{
		ResourceType:   sarama.AclResourceAny,
		Operation:      sarama.AclOperationAny,
		PermissionType: sarama.AclPermissionAny,
	}

	//acls, err := admin.DescribeAcls(filter)
	acls, err := admin.ListAcls(filter)
	if err != nil {
		// Kafka 版本可能不支持 ACLs 或需要认证
		fmt.Println("ACLs not available or permission denied")
		return
	}

	printACLsTable(acls)
}

func printACLsTable(acls []sarama.ResourceAcls) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintf(w, "Found %d ACL entries\n\n", len(acls))
	fmt.Fprintln(w, "PRINCIPAL\tRESOURCE\tTYPE\tNAME\tOPERATION\tPERMISSION\tHOST")

	for _, resource := range acls {
		for _, acl := range resource.Acls {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				acl.Principal,
				resource.ResourceType.String(),
				resource.ResourceName,
				acl.Operation.String(),
				acl.PermissionType.String(),
				acl.Host)
		}
	}
	w.Flush()
}

func cmdInspect(brokers []string, config *sarama.Config) {
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create admin: %v", err)
	}
	defer admin.Close()

	fmt.Println("🔍 Kafka Cluster Inspection Report")
	fmt.Println("=" + strings.Repeat("=", 60))

	brokerInfo, controller, err := admin.DescribeCluster()
	if err != nil {
		log.Printf("Warning: Could not get cluster info: %v", err)
	} else {
		fmt.Printf("\n📊 Cluster Overview:\n")
		fmt.Printf("   Brokers:        %d\n", len(brokerInfo))
		controllerID := -1
		if controller != 0 {
			controllerID = int(controller)
		}
		fmt.Printf("   Controller ID:  %d\n", controllerID)
	}

	topics, err := admin.ListTopics()
	if err != nil {
		log.Printf("Warning: Could not list topics: %v", err)
	} else {
		var internalCount, userCount int
		var totalPartitions int32

		for name, detail := range topics {
			totalPartitions += detail.NumPartitions
			if strings.HasPrefix(name, "__") {
				internalCount++
			} else {
				userCount++
			}
		}

		fmt.Printf("\n📈 Topics:\n")
		fmt.Printf("   Total Topics:   %d\n", len(topics))
		fmt.Printf("   User Topics:    %d\n", userCount)
		fmt.Printf("   Internal:       %d\n", internalCount)
		fmt.Printf("   Total Partitions: %d\n", totalPartitions)
	}

	groups, err := admin.ListConsumerGroups()
	if err != nil {
		log.Printf("Warning: Could not list groups: %v", err)
	} else {
		fmt.Printf("\n👥 Consumer Groups:\n")
		fmt.Printf("   Total Groups:   %d\n", len(groups))

		var groupIDs []string
		for id := range groups {
			groupIDs = append(groupIDs, id)
			if len(groupIDs) >= 5 {
				break
			}
		}

		if len(groupIDs) > 0 {
			fmt.Printf("\n   Sample Group LAG:\n")
			for _, gid := range groupIDs {
				lag := calculateGroupLag(brokers, config, gid)
				status := "✅"
				if lag > 1000 {
					status = "⚠️"
				}
				fmt.Printf("   - %s: %s Lag=%d\n", gid, status, lag)
			}
		}
	}

	fmt.Printf("\n⚙️  Configuration Check:\n")
	fmt.Printf("   Broker Version: %s\n", config.Version.String())
	fmt.Printf("   Timeout:        %ds\n", timeout)

	fmt.Println("\n" + strings.Repeat("=", 62))
	fmt.Println("✅ Inspection completed")
}

func cmdHealthCheck(brokers []string, config *sarama.Config) {
	fmt.Println("🏥 Kafka Cluster Health Check")

	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("❌ Connection failed: %v", err)
	}
	defer admin.Close()
	brokerInfo, controller, err := admin.DescribeCluster()
	if err != nil {
		log.Fatalf("❌ Cluster check failed: %v", err)
	}

	fmt.Printf("✅ Connected successfully\n")
	fmt.Printf("   Active Brokers: %d\n", len(brokerInfo))
	controllerID := -1
	if controller != 0 {
		controllerID = int(controller)
	}
	fmt.Printf("   Controller:     Broker %d\n", controllerID)

	topics, err := admin.ListTopics()
	if err != nil {
		fmt.Println("⚠️  Could not list topics")
	} else {
		fmt.Printf("✅ Topics accessible: %d topics found\n", len(topics))
	}

	fmt.Println("\n📊 Quick Stats:")
	fmt.Println("   Run './kops -cmd inspect' for detailed inspection")
	fmt.Println("   Run './kops -cmd topics' to list all topics")
	fmt.Println("   Run './kops -cmd groups' to list consumer groups")
}

func intSliceToString(nums []int32) string {
	var str []string
	for _, n := range nums {
		str = append(str, strconv.Itoa(int(n)))
	}
	return strings.Join(str, ",")
}

func exportToCSV(data interface{}) {
	w := csv.NewWriter(os.Stdout)
	defer w.Flush()

	switch v := data.(type) {
	case []TopicInfo:
		w.Write([]string{"Topic", "Partitions", "Replicas", "LogSize", "Internal"})
		for _, t := range v {
			w.Write([]string{
				t.Name,
				strconv.Itoa(int(t.Partitions)),
				strconv.Itoa(int(t.Replicas)),
				strconv.FormatInt(t.TotalLogSize, 10),
				strconv.FormatBool(t.Internal),
			})
		}
	}
}

func exportToJSON(data interface{}) {
	jsonBytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(jsonBytes))
}

func exportPartitionDetailsToCSV(topic string, details []PartitionDetail, totalSize int64) {
	w := csv.NewWriter(os.Stdout)
	defer w.Flush()

	w.Write([]string{"Topic", "Partition", "Leader", "Replicas", "ISR", "UnderReplicated", "OldestOffset", "NewestOffset", "LogSize"})
	for _, d := range details {
		w.Write([]string{
			topic,
			strconv.Itoa(int(d.Partition)),
			strconv.Itoa(int(d.Leader)),
			intSliceToString(d.Replicas),
			intSliceToString(d.ISR),
			strconv.FormatBool(d.UnderReplicated),
			strconv.FormatInt(d.OldestOffset, 10),
			strconv.FormatInt(d.NewestOffset, 10),
			strconv.FormatInt(d.LogSize, 10),
		})
	}
}
