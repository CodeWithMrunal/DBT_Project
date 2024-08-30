# Stream and Batch Processing of Twitter Data using Apache Spark and Kafka

## 📜 Project Overview

This project demonstrates the **real-time stream processing** and **batch processing** of simulated Twitter data using **Apache Kafka** and **Apache Spark**. By implementing a Lambda architecture, this project simulates Twitter API-like performance by publishing and subscribing to Kafka topics and processing the data with Spark.

The project is designed to showcase how large volumes of Twitter-like data can be handled efficiently in real-time, offering insights and analysis through advanced data processing techniques.

## 🛠️ Technologies and Frameworks

- **Operating System:** Ubuntu 20.04/22.04 or any Linux environment
- **Apache Spark:** For stream and batch processing
- **Apache Kafka:** For real-time data streaming
- **MySQL Database:** For storing simulated Twitter data
- **Zookeeper:** For coordinating distributed Kafka servers
- **Python:** For scripting and orchestration

## 📈 Key Features

- **Real-Time Streaming:** Data is streamed in real-time from a MySQL database, simulating live Twitter feeds.
- **Batch Processing:** Perform complex batch processing operations to analyze historical data.
- **Lambda Architecture:** Combines both real-time and batch processing capabilities to deliver comprehensive insights.
- **Kafka Topics:** Multiple Kafka topics like `tweets`, `hashtags`, `top_tweets`, and `top_hashtags` are used to structure the data streams effectively.
- **Simulation without APIs:** Achieves near real-time simulation without using Twitter APIs, ensuring broader applicability.

## 📚 Project Structure

- `kafka_producer.py` - Publishes simulated Twitter data to Kafka topics.
- `spark_streaming_consumer.py` - Subscribes to Kafka topics for real-time data processing using Spark Streaming.
- `spark_batch_consumer.py` - Performs batch processing of the Kafka topics using Apache Spark.
- `tweet_hashtags_db.sql` - SQL script for creating the database and tables for storing tweets and hashtags.
- `TABLES_TWEET_DATABASE.sql` - Additional SQL script for managing tables in the MySQL database.

## 🚀 Getting Started

### 1. Prerequisites

- Install the necessary technologies: Kafka, Zookeeper, Spark, and MySQL.
- Load the provided SQL scripts into your MySQL database.
- Install Python dependencies:
  \`\`\`bash
  pip3 install pyspark
  \`\`\`

### 2. Setup

1. **Start Zookeeper and Kafka:**
    \`\`\`bash
    sudo systemctl start zookeeper
    sudo systemctl start kafka
    \`\`\`
   
2. **Create Kafka Topics:**
    \`\`\`bash
    sudo -u "user_username" /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic tweets
    sudo -u "user_username" /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic hashtags
    sudo -u "user_username" /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic top_tweets
    sudo -u "user_username" /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic top_hashtags
    \`\`\`

3. **Simulate Data Streaming:**
    \`\`\`bash
    python3 kafka_producer.py
    \`\`\`

4. **Stream Processing with Spark:**
    \`\`\`bash
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3 spark_streaming_consumer.py
    \`\`\`

5. **Batch Processing with Spark:**
    \`\`\`bash
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3 spark_batch_consumer.py
    \`\`\`

Both consumers can run simultaneously, demonstrating the capability to handle both real-time and batch processing of data.

## 🎯 Use Cases

This project serves as a foundation for anyone looking to understand or build:

- Real-time data analytics platforms
- Batch processing pipelines
- Lambda architecture implementations
- Stream processing applications using Kafka and Spark

## 📈 Performance Insights

Explore how stream and batch processing compare, particularly in handling large-scale data scenarios, and discover how different windowing techniques affect data processing.
