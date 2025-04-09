import random
import time
import json
import mysql.connector
import signal
from datetime import datetime
from kafka import KafkaProducer

# Graceful shutdown flag
running = True
def handle_sigint(sig, frame):
    global running
    print("Shutting down Kafka producer...")
    running = False
signal.signal(signal.SIGINT, handle_sigint)

# MySQL connection
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="CodeMrunal2004",
    database="tweet_hashtags_db"
)
cursor = mydb.cursor(dictionary=True)

# Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Store last seen timestamp for tweets
last_tweet_time = '2000-01-01 00:00:00'

while running:
    try:
        # =================== TWEETS ===================
        try:
            print(f"[{datetime.now()}] Fetching new tweets...")
            sql_tweets = f"""
                SELECT tweet_id, tweet, date_time, language 
                FROM tweets 
                WHERE date_time > '{last_tweet_time}' 
                ORDER BY date_time ASC 
                LIMIT 25
            """
            cursor.execute(sql_tweets)
            tweets = cursor.fetchall()

            for row in tweets:
                message_dict = {
                    "tweet_id": row["tweet_id"],
                    "tweet": row["tweet"],
                    "date_time": str(row["date_time"]),
                    "language": row["language"]
                }
                producer.send('tweets', value=bytes(json.dumps(message_dict), encoding='utf-8'))
                print(f"[{datetime.now()}] Published tweet: {row['tweet']}")

            if tweets:
                last_tweet_time = str(tweets[-1]['date_time'])

            producer.flush()

        except Exception as e:
            print(f"[ERROR] TWEETS: {e}")

        # Sleep for 5 to 15 seconds
        time.sleep(5)

    except Exception as e:
        print(f"[ERROR] Outer loop: {e}")
        time.sleep(2)

# Cleanup
cursor.close()
mydb.close()
producer.close()
print("Producer stopped.")
