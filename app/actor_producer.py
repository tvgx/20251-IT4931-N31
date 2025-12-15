# actor_producer.py
from pyspark.sql import SparkSession
from crawler import MovieDB
from schema import ACTOR_SCHEMA
import os
from dotenv import load_dotenv
import time

load_dotenv()

KAFKA_BROKER1 = os.environ["KAFKA_BROKER1"]
ACTOR_TOPIC = os.environ["ACTOR_TOPIC"]

packages = ['org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1']

spark = SparkSession.builder \
    .appName("ActorProducer") \
    .master("local[*]") \
    .config("spark.jars.packages", ",".join(packages)) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

movie_db = MovieDB()
page = 1

print("=== ACTOR PRODUCER STARTED ===")

while True:
    try:
        print(f"\nFetching actors page {page}...")
        actors = movie_db.get_actors(page=page)
        if not actors or len(actors) == 0:
            print("No more data. Resetting to page 1. Sleep 60s...")
            time.sleep(60)
            page = 1  # Reset page để bắt đầu từ đầu
            continue

        df = spark.createDataFrame(actors, schema=ACTOR_SCHEMA)

        df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
          .write \
          .format("kafka") \
          .option("kafka.bootstrap.servers", KAFKA_BROKER1) \
          .option("topic", ACTOR_TOPIC) \
          .mode("append") \
          .save()

        print(f"Sent {len(actors)} actors (page {page})")
        page += 1
        time.sleep(1.5)

    except Exception as e:
        print(f"Error: {e}")
        time.sleep(10)