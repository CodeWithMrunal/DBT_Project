from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row
import json

spark = SparkSession \
    .builder \
    .appName("KafkaTweetProcessor") \
    .getOrCreate()

# Set log level to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tweets") \
    .load()

# Convert the binary "value" column to string
df = df.selectExpr("CAST(value AS STRING) as json_data")

# Define schema
schema = StructType([
    StructField("tweet_id", LongType()),
    StructField("tweet", StringType()),
    StructField("date_time", StringType()),
    StructField("language", StringType())
])

tweets_df = df.select(from_json(col("json_data"), schema).alias("data")).select("data.*")

# Extract hashtags from tweet text (very basic)
hashtags_df = tweets_df.select(
    explode(split(col("tweet"), " ")).alias("word")
).filter(col("word").startswith("#"))

hashtag_count = hashtags_df.groupBy("word").count()

# Output to console
query = hashtag_count.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
