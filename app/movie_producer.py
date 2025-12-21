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


# Initialize với 8 concurrent workers để fetch details nhanh hơn
movie_db = MovieDB(max_workers=8)
page = 1


print("=== MOVIE PRODUCER STARTED - SENDING FOREVER ===")


while True:
    try:
        print(f"\nFetching page {page}...")
        start_time = time.time()
        movies = movie_db.get_movies(page=page)
        fetch_time = time.time() - start_time
       
        if not movies or len(movies) == 0:
            print("No more data or rate limited. Resetting to page 1. Sleep 60s...")
            time.sleep(60)
            page = 1
            continue


        df = spark.createDataFrame(movies, schema=MOVIE_SCHEMA)


        df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
          .write \
          .format("kafka") \
          .option("kafka.bootstrap.servers", KAFKA_BROKER1) \
          .option("topic", MOVIE_TOPIC) \
          .mode("append") \
          .save()


        print(f"✓ Sent {len(movies)} movies (page {page}) in {fetch_time:.2f}s")
        page += 1
        time.sleep(0.5)  # Giảm từ 1.5s xuống 0.5s - rate limit đã handle bằng retry strategy


    except Exception as e:
        print(f"Error: {e}")
        time.sleep(10)
