# movie_producer.py
from pyspark.sql import SparkSession
from crawler import MovieDB
from schema import MOVIE_SCHEMA
import os
from dotenv import load_dotenv
import time

load_dotenv()

KAFKA_BROKER1 = os.environ["KAFKA_BROKER1"]
MOVIE_TOPIC = os.environ["MOVIE_TOPIC"]

packages = [
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1',
]

spark = SparkSession.builder \
    .appName("MovieProducer") \
    .master("local[*]") \
    .config("spark.jars.packages", ",".join(packages)) \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint/movie_producer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

movie_db = MovieDB()
page = 1

print("=== MOVIE PRODUCER STARTED - SENDING FOREVER ===")

while True:
    try:
        print(f"\nFetching page {page}...")
        movies = movie_db.get_movies(page=page)
        if not movies or len(movies) == 0:
            print("No more data or rate limited. Resetting to page 1. Sleep 60s...")
            time.sleep(60)
            page = 1  # Reset page để bắt đầu từ đầu
            continue

        df = spark.createDataFrame(movies, schema=MOVIE_SCHEMA)

        df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
          .write \
          .format("kafka") \
          .option("kafka.bootstrap.servers", KAFKA_BROKER1) \
          .option("topic", MOVIE_TOPIC) \
          .mode("append") \
          .save()

        print(f"Sent {len(movies)} movies (page {page})")
        page += 1
        time.sleep(1.5)  # tránh rate limit TMDB

    except Exception as e:
        print(f"Error: {e}")
        time.sleep(10)