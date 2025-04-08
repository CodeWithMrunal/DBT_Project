from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StringType
import mysql.connector
import time
# Define schema
schema = StructType() \
    .add("tweet_id", StringType()) \
    .add("tweet", StringType()) \
    .add("date_time", StringType()) \
    .add("language", StringType())

# Initialize Spark
spark = SparkSession.builder \
    .appName("TweetLanguageCounter") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tweets") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON value
json_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Count per language
lang_count = json_df.groupBy("language").count()

def write_to_mysql(batch_df, batch_id):
    # Convert to Pandas
    start_time=time.time()
    lang_counts = batch_df.toPandas()

    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='CodeMrunal2004',
            database='tweet_hashtags_db'
        )
        cursor = conn.cursor()

        for _, row in lang_counts.iterrows():
            language = row['language']
            count = int(row['count'])

            cursor.execute("""
                INSERT INTO language_counts (language, count)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE count = count + VALUES(count)
            """, (language, count))

        conn.commit()

        elapsed = time.time() - start_time
        log_message = f"[INFO] Batch {batch_id} processed in {elapsed:.2f} seconds"
        print(log_message)
        print(f"[INFO] Updated language counts: {lang_counts.to_dict('records')}")
        with open("batch_execution_times.log", "a") as f:
            f.write(f"Batch {batch_id}: {elapsed:.2f} seconds\n")
    except mysql.connector.Error as err:
        print(f"[ERROR] MySQL: {err}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# Write stream to foreachBatch
query = lang_count.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_mysql) \
    .start()

query.awaitTermination()
