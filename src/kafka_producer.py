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

        # =================== HASHTAGS ===================
        try:
            print(f"[{datetime.now()}] Fetching hashtags...")
            sql_hashtags = """
                SELECT hashtag_id, hashtag 
                FROM hashtags 
                WHERE hashtag_id < (SELECT MAX(hashtag_id) FROM tweet_hashtags)
                LIMIT 10
            """
            cursor.execute(sql_hashtags)
            hashtags = cursor.fetchall()

            for row in hashtags:
                message_dict = {
                    "hashtag_id": row["hashtag_id"],
                    "hashtag": row["hashtag"]
                }
                producer.send('hashtags', value=bytes(json.dumps(message_dict), encoding='utf-8'))
                print(f"[{datetime.now()}] Published hashtag: {row['hashtag']}")

            producer.flush()

        except Exception as e:
            print(f"[ERROR] HASHTAGS: {e}")

        # =================== TOP TWEETS ===================
        try:
            print(f"[{datetime.now()}] Fetching top tweets...")
            sql_top_tweets = """
                SELECT t.tweet_id, t.tweet, t.date_time, t.language, COUNT(th.hashtag_id) as hashtag_count 
                FROM tweets t 
                JOIN tweet_hashtags th ON t.tweet_id = th.tweet_id 
                GROUP BY t.tweet_id 
                ORDER BY hashtag_count DESC 
                LIMIT 10
            """
            cursor.execute(sql_top_tweets)
            top_tweets = cursor.fetchall()

            for row in top_tweets:
                message_dict = {
                    "tweet_id": row["tweet_id"],
                    "tweet": row["tweet"],
                    "date_time": str(row["date_time"]),
                    "language": row["language"],
                    "hashtag_count": row["hashtag_count"]
                }
                producer.send('top_tweets', value=bytes(json.dumps(message_dict), encoding='utf-8'))
                print(f"[{datetime.now()}] Published top tweet: {row['tweet']}")

            producer.flush()

        except Exception as e:
            print(f"[ERROR] TOP TWEETS: {e}")

        # =================== TOP HASHTAGS ===================
        try:
            print(f"[{datetime.now()}] Fetching top hashtags...")
            sql_top_hashtags = """
                SELECT h.hashtag_id, h.hashtag, SUM(th.count) as count 
                FROM hashtags h 
                JOIN tweet_hashtags th ON h.hashtag_id = th.hashtag_id 
                JOIN (
                    SELECT t.tweet_id 
                    FROM tweets t 
                    JOIN tweet_hashtags th ON t.tweet_id = th.tweet_id 
                    GROUP BY t.tweet_id 
                    ORDER BY COUNT(th.hashtag_id) DESC 
                    LIMIT 10
                ) AS subquery ON th.tweet_id = subquery.tweet_id 
                GROUP BY h.hashtag_id 
                ORDER BY count DESC 
                LIMIT 10
            """
            cursor.execute(sql_top_hashtags)
            top_hashtags = cursor.fetchall()

            for row in top_hashtags:
                message_dict = {
                    "hashtag_id": row["hashtag_id"],
                    "hashtag": row["hashtag"],
                    "count": int(row["count"])
                }
                producer.send('top_hashtags', value=bytes(json.dumps(message_dict), encoding='utf-8'))
                print(f"[{datetime.now()}] Published top hashtag: {row['hashtag']}")

            producer.flush()

        except Exception as e:
            print(f"[ERROR] TOP HASHTAGS: {e}")

        # Sleep for 5 to 15 seconds
        # time.sleep(random.randint(5, 15))
        time.sleep(5)

    except Exception as e:
        print(f"[ERROR] Outer loop: {e}")
        time.sleep(2)

# Cleanup
cursor.close()
mydb.close()
producer.close()
print("Producer stopped.")
