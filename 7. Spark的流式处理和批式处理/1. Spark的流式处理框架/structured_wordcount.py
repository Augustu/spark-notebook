from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

lines = spark \
        .readStream \
        .format("text") \
        .option("path", "../../data/streaming") \
        .option("latestFirst", "false") \
        .load()

# map reduce transform join reduceByKey
words = lines.select(
    split(lines.value, "\t")[0]
    .alias("word")
)

wordCounts = words.groupBy("word").count()

query = wordCounts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()


query.awaitTermination()

