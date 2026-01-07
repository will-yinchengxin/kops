#!/bin/bash

set -e

echo "=================================="
echo "步骤 1: 启动 Zookeeper"
echo "=================================="

# 配置目录, "<本地目录>"
CONF_DIR="/var/lib/conf"

# 检查配置目录
if [ ! -d "$CONF_DIR/zookeeper" ]; then
    echo "错误: 配置目录不存在: $CONF_DIR/zookeeper"
    exit 1
fi

# 创建数据目录
mkdir -p "$CONF_DIR/zookeeper/data"
mkdir -p "$CONF_DIR/zookeeper/log"

# 启动 Zookeeper
docker run -d \
  --name zookeeper \
  --network host \
  -p 2181:2181 \
  -v "$CONF_DIR/zookeeper/zoo.cfg:/conf/zoo.cfg" \
  -v "$CONF_DIR/zookeeper/data:/var/lib/zookeeper/data" \
  -v "$CONF_DIR/zookeeper/log:/var/lib/zookeeper/log" \
  -e ZOOKEEPER_CLIENT_PORT=2181 \
  -e ZOOKEEPER_TICK_TIME=2000 \
  confluentinc/cp-zookeeper:7.5.0

echo "✅ Zookeeper 启动成功!"
echo ""
echo "查看日志: docker logs -f zookeeper"
echo "检查状态: docker ps | grep zookeeper"
echo ""
echo "等待 10 秒让 Zookeeper 完全启动..."
sleep 10

# 验证 Zookeeper
docker exec zookeeper bash -c "echo ruok | nc localhost 2181"
echo ""
echo "如果看到 'imok'，说明 Zookeeper 运行正常"


set -e

echo "=================================="
echo "步骤 2: 启动 Kafka"
echo "=================================="


# 检查配置目录
if [ ! -d "$CONF_DIR/kafka" ]; then
    echo "错误: 配置目录不存在: $CONF_DIR/kafka"
    exit 1
fi

# 检查 Zookeeper 是否在运行
if ! docker ps | grep -q zookeeper; then
    echo "错误: Zookeeper 未运行，请先安装 zookeeper"
    exit 1
fi

# 创建数据目录
mkdir -p "$CONF_DIR/kafka/data"

# 启动 Kafka
docker run -d \
  --name kafka \
  --network host \
  --restart unless-stopped \
  -v /var/lib/conf/kafka/data:/var/lib/kafka/data \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_ZOOKEEPER_CONNECT=localhost:2181 \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://172.16.27.228:9092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
  -e KAFKA_LOG_DIRS=/var/lib/kafka/data \
  -e KAFKA_MESSAGE_MAX_BYTES=20242880 \
  -e KAFKA_REPLICA_FETCH_MAX_BYTES=20242880 \
  -e KAFKA_MAX_REQUEST_SIZE=20242880 \
  confluentinc/cp-kafka:7.5.0



echo "✅ Kafka 启动成功!"
echo ""
echo "查看日志: docker logs -f kafka"
echo "检查状态: docker ps | grep kafka"
echo ""
echo "等待 20 秒让 Kafka 完全启动..."
sleep 20

# 验证 Kafka
echo "验证 Kafka broker..."
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:29092

echo ""
echo "✅ Kafka 运行正常!"
