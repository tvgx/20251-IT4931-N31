# movie_producer.py
from pyspark.sql import SparkSession
from crawler import MovieDB
from schema import MOVIE_SCHEMA
import os
from dotenv import load_dotenv
import time
from datetime import datetime

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

# Initialize với 4 concurrent workers để tránh rate limit
movie_db = MovieDB(max_workers=4)

print("="*70)
print("🚀 MOVIE PRODUCER STARTED - DIVIDE & CONQUER BY YEAR")
print("="*70)

# Strategy: Lấy từng năm để vượt qua limit 10k
current_year = datetime.now().year
years_to_fetch = list(range(current_year, 1974, -1))  # 2024, 2023, 2022, ..., 1975

print(f"\n📅 Priority order: {current_year} → 1975")
print(f"   Total years: {len(years_to_fetch)}")
print(f"   First few years: {years_to_fetch[:5]}")
print(f"   Last few years: {years_to_fetch[-5:]}\n")

year_index = 0
page_in_year = 1

while True:
    try:
        # Lấy năm hiện tại
        year = years_to_fetch[year_index % len(years_to_fetch)]
        
        print(f"\n📅 Year: {year} | Page: {page_in_year}")
        start_time = time.time()
        
        movies, total_pages = movie_db.get_movies_by_year(year=year, page=page_in_year)
        fetch_time = time.time() - start_time
        
        if not movies or len(movies) == 0:
            print(f"   ✓ Year {year} complete (pages: {page_in_year-1})")
            # Chuyển sang năm tiếp theo
            year_index += 1
            page_in_year = 1
            
            if year_index >= len(years_to_fetch):
                print(f"\n✓ All years fetched! Resetting to 1975. Sleep 60s...")
                year_index = 0
                time.sleep(60)
            continue

        df = spark.createDataFrame(movies, schema=MOVIE_SCHEMA)

        df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
          .write \
          .format("kafka") \
          .option("kafka.bootstrap.servers", KAFKA_BROKER1) \
          .option("topic", MOVIE_TOPIC) \
          .mode("append") \
          .save()

        print(f"   ✓ Sent {len(movies)} movies in {fetch_time:.2f}s (page {page_in_year}/{total_pages})")
        
        # Nếu còn page, tiếp tục với year này
        if page_in_year < total_pages:
            page_in_year += 1
        else:
            # Sang năm tiếp theo
            year_index += 1
            page_in_year = 1
            
            if year_index >= len(years_to_fetch):
                print(f"\n✓ All years fetched! Resetting to 1975. Sleep 60s...")
                year_index = 0
                time.sleep(60)
        
        time.sleep(0.5)  # Rate limit

    except Exception as e:
        print(f"❌ Error: {e}")
        time.sleep(10)