import json
from kafka import KafkaConsumer
import mysql.connector
from mysql.connector import Error

# MySQL connection setup
try:
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='CodeMrunal2004',
        database='tweet_hashtags_db'
    )
    cursor = conn.cursor()
    print("[INFO] Connected to MySQL.")
except Error as e:
    print(f"[ERROR] MySQL connection error: {e}")
    exit()

# Kafka consumer setup
consumer = KafkaConsumer(
    'tweets',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='tweet-consumer-group'
)

print("[INFO] Kafka consumer started. Listening for messages...")

for message in consumer:
    try:
        data = json.loads(message.value.decode('utf-8'))

        tweet_id = data['tweet_id']
        tweet = data['tweet']
        date_time = data['date_time']
        language = data['language']

        # Remove timezone abbreviation (e.g., 'EDT', 'UTC') if present
        if isinstance(date_time, str) and len(date_time.split()) == 3:
            date_time = date_time.rsplit(' ', 1)[0]

        # Insert into MySQL
        cursor.execute('''
    INSERT IGNORE INTO tweets_table (tweet_id, tweet, date_time, language)
    VALUES (%s, %s, %s, %s)
''', (tweet_id, tweet, date_time, language))

        conn.commit()
        print(f"[INFO] Inserted: {tweet_id}")

    except Exception as e:
        print(f"[ERROR] Failed to insert row: {data} => {e}")
