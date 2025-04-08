import mysql.connector
import time

def execute_batch_queries():
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='CodeMrunal2004',
            database='tweet_hashtags_db'
        )
        cursor = conn.cursor()
        print("[INFO] Connected to MySQL.")

        # Start timer
        start_time = time.time()

        # 1. Language Count
        print("\n[Batch Query 1] Language-wise Tweet Count:")
        cursor.execute("""
            SELECT language, COUNT(*) AS count 
            FROM tweets 
            GROUP BY language 
            ORDER BY count DESC;
        """)
        for row in cursor.fetchall():
            print(f"Language: {row[0]}, Count: {row[1]}")

        # 2. Top Words (Optional - simplistic hashtag simulation)
        print("\n[Batch Query 2] Most Common Words (excluding stop words):")
        cursor.execute("SELECT tweet FROM tweets;")
        tweets = [row[0] for row in cursor.fetchall()]
        
        from collections import Counter
        import re

        stop_words = {"the", "and", "to", "a", "of", "in", "is", "it", "on", "for", "with"}
        all_words = []

        for tweet in tweets:
            words = re.findall(r'\b\w+\b', tweet.lower())
            filtered = [word for word in words if word not in stop_words and len(word) > 2]
            all_words.extend(filtered)

        counter = Counter(all_words)
        print(counter.most_common(10))

        # End timer
        end_time = time.time()
        print(f"\n[INFO] Batch processing completed in {end_time - start_time:.2f} seconds.")

    except mysql.connector.Error as err:
        print(f"[ERROR] MySQL Error: {err}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    execute_batch_queries()
