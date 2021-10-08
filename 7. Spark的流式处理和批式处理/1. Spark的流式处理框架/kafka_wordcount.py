'''
    kafka 做 input source 需要指定 package

    清除 PYSPARK_DRIVER_PYTHON 环境变量

    ./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 ./kafka_wordcount.py
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, explode, last, count, array, transform, unix_timestamp
from pyspark.sql.functions import split

kafka_bootstrap_servers = "192.168.0.200:9092"
kafka_subscribe_topic = "stream"
kafka_output_topic = "output"

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

df = spark \
     .readStream \
     .format("kafka") \
     .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
     .option("subscribe", kafka_subscribe_topic) \
     .option("startingOffsets", "earliest") \
     .load()

words = df.select(
        df["key"],
        explode(
            split(df.value, " ")
        )
        .alias("value"),
        df["timestamp"],
        df["offset"]
    ) \
    .where("value != ''")


#  使用 agg，groupBy，统计 value 出现的次数，同时保留其他列
words = words \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy("timestamp", "value") \
    .agg(
        last("key").alias("key"),
        last("offset").alias("offset"),
        count("offset").alias("count")
        )

words = words.withColumn("unix_timestamp", unix_timestamp("timestamp"))
words = words.withColumn("unix_timestamp", transform(array("unix_timestamp"), lambda x: x*1000))
words = words.withColumn("value", concat_ws("+", "unix_timestamp", "timestamp", "offset", "key", "value", "count"))


# query = words \
#     .selectExpr("CAST(value AS STRING)") \
#     .writeStream \
#     .format("text") \
#     .option("path", "./output/") \
#     .option("checkpointLocation", "./checkpoints") \
#     .start()


query = words \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", kafka_output_topic) \
    .option("checkpointLocation", "./checkpoints") \
    .start()

query.awaitTermination()
