### Spark 的流式处理框架

#### 启动 kafka

```bash
# 修改 kafka 配置文件 server.properties
listeners=PLAINTEXT://[ip]:9092

# 启动 zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties &

# 启动 kafka
bin/kafka-server-start.sh config/server.properties &
```



#### 准备 topic

```bash
# 创建订阅 topic
bin/kafka-topics.sh --bootstrap-server [ip]:9092 --create --topic stream --partitions 1 --replication-factor 1

# 创建输出 topic
bin/kafka-topics.sh --bootstrap-server [ip]:9092 --create --partitions 1 --replication-factor 1 --topic output

# 查看 topic 列表
bin/kafka-topics.sh --bootstrap-server [ip]:9092 --list

# 向 stream topic 输入
bin/kafka-console-producer.sh --bootstrap-server [ip]:9092 --topic stream

# 监听 output topic
bin/kafka-console-consumer.sh --bootstrap-server [ip]:9092 --topic output
```



#### 运行文件作为输入流的例子

```bash
python structured_wordcount.py
```



#### 运行 kafka 作为输入输出的例子

```bash
./bin/spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 ./kafka_wordcount.py
```

