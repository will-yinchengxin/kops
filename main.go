package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/IBM/sarama"
)

var (
	brokersStr   string
	command      string
	topic        string
	group        string
	partition    int
	key          string
	data         string
	limit        int
	export       string
	filter       string
	showInternal bool
	timeout      int
)

func init() {
	flag.StringVar(&brokersStr, "brokers", "localhost:9092", "Kafka brokers (comma separated)")
	flag.StringVar(&brokersStr, "b", "localhost:9092", "Kafka brokers (short)")
	flag.StringVar(&command, "cmd", "", "Command: topics, details, peek, groups, lag, produce, acls, inspect, health")
	flag.StringVar(&topic, "topic", "", "Topic name")
	flag.StringVar(&topic, "t", "", "Topic name (short)")
	flag.StringVar(&group, "group", "", "Consumer group name")
	flag.StringVar(&group, "g", "", "Consumer group name (short)")
	flag.IntVar(&partition, "partition", -1, "Partition number (-1 for auto)")
	flag.IntVar(&partition, "p", -1, "Partition number (short)")
	flag.StringVar(&key, "key", "", "Message key for produce")
	flag.StringVar(&data, "data", "", "Message data for produce")
	flag.StringVar(&data, "d", "", "Message data (short)")
	flag.IntVar(&limit, "limit", 10, "Number of messages for peek")
	flag.IntVar(&limit, "n", 10, "Number of messages (short)")
	flag.StringVar(&export, "export", "", "Export format: csv, json (empty for console)")
	flag.StringVar(&filter, "filter", "", "Filter topics/groups by pattern")
	flag.BoolVar(&showInternal, "internal", false, "Show internal topics")
	flag.IntVar(&timeout, "timeout", 10, "Timeout in seconds")
}

// go build -o kops main.go
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
	case "topics":
		cmdListTopics(brokers, config)
	case "details":
		if topic == "" {
			log.Fatal("Please specify topic with -topic")
		}
		cmdTopicDetails(brokers, config, topic)
	case "peek":
		if topic == "" {
			log.Fatal("Please specify topic with -topic")
		}
		cmdPeek(brokers, config, topic, limit)
	case "groups":
		cmdListGroups(brokers, config)
	case "lag":
		if group == "" {
			log.Fatal("Please specify group with -group")
		}
		cmdGroupLag(brokers, config, group)
	case "produce":
		if topic == "" || data == "" {
			log.Fatal("Please specify topic and data with -topic and -data")
		}
		cmdProduce(brokers, config, topic, key, data, int32(partition))
	case "acls":
		cmdListACLs(brokers, config)
	case "inspect":
		cmdInspect(brokers, config)
	case "health":
		cmdHealthCheck(brokers, config)
	default:
		log.Fatalf("Unknown command: %s", command)
	}
}

func printUsage() {
	fmt.Println(`
KafkaOps - Kafka Operations CLI Tool

Commands:
  topics     List all topics (with partitions and replicas)
  details    Show topic details (offsets, size, ISR)
  peek       Peek last N messages from a topic
  groups     List all consumer groups
  lag        Show consumer group lag and offsets
  produce    Produce a message to a topic
  acls       List ACLs (if supported)
  inspect    Inspect cluster health and metrics
  health     Quick health check

Examples:
  ./kops -cmd topics -brokers kafka1:9092,kafka2:9092
  ./kops -cmd details -t my-topic -b localhost:9092
  ./kops -cmd lag -g my-consumer-group
  ./kops -cmd peek -t my-topic -n 5
  ./kops -cmd produce -t test -d '{"id":1}' -key "key1"
  ./kops -cmd inspect -export csv > report.csv
  `)
}

type TopicInfo struct {
	Name         string
	Partitions   int32
	Replicas     int16
	TotalLogSize int64
	Internal     bool
	Configs      map[string]*string
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

type MessageInfo struct {
	Partition int32
	Offset    int64
	Key       string
	Value     string
	Timestamp time.Time
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

func intSliceToString(nums []int32) string {
	var str []string
	for _, n := range nums {
		str = append(str, strconv.Itoa(int(n)))
	}
	return strings.Join(str, ",")
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
