import mysql.connector
import time
import psutil
import os

def execute_batch_queries():
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='CodeMrunal2004',
            database='tweet_hashtags_db'
        )
        cursor = conn.cursor()
        print("[INFO] Connected to MySQL.")

        process = psutil.Process(os.getpid())

        start_cpu = psutil.cpu_percent(interval=1)
        mem = process.memory_full_info().rss / (1024 * 1024)  # MB
        start_time = time.time()

        # Query
        print("\n[Batch Query 1] Language-wise Tweet Count:")
        cursor.execute("""
            SELECT language, COUNT(*) AS count 
            FROM tweets 
            GROUP BY language 
            ORDER BY count DESC;
        """)
        for row in cursor.fetchall():
            print(f"Language: {row[0]}, Count: {row[1]}")

        end_time = time.time()
        end_cpu = psutil.cpu_percent(interval=1)
        mem = process.memory_full_info().rss / (1024 * 1024)  # MB

        print(f"\n[INFO] Batch processing completed in {end_time - start_time:.2f} seconds.")
        print(f"[RESOURCE USAGE] CPU Usage: {(start_cpu + end_cpu) / 2:.1f}%")
        print(f"[MEMORY] {mem:.2f} MB")

    except mysql.connector.Error as err:
        print(f"[ERROR] MySQL Error: {err}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    execute_batch_queries()
