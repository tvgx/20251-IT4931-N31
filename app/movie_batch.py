# movie_batch.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, year, to_date, when, expr
from schema import MOVIE_SCHEMA
import os
from dotenv import load_dotenv
import sys

load_dotenv()

KAFKA_BROKER1 = os.environ["KAFKA_BROKER1"]
MOVIE_TOPIC = os.environ["MOVIE_TOPIC"]
ES_NODES = os.environ["ES_NODES"]
CONNECTION_STRING = os.environ["CONNECTION_STRING"]
# Use local[*] if MASTER is not set, or use the one from env
MASTER = os.environ.get("MASTER", "local[*]")

packages = [
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1',
    'org.mongodb.spark:mongo-spark-connector_2.12:10.4.0',
    'org.elasticsearch:elasticsearch-spark-30_2.12:8.15.0'
]

print("=== STARTING MOVIE BATCH JOB ===")

spark = SparkSession.builder \
    .appName("MovieBatch") \
    .master(MASTER) \
    .config("spark.jars.packages", ",".join(packages)) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka in BATCH mode (defaults to earliest -> latest available)
print(f"Reading from Kafka topic: {MOVIE_TOPIC}...")
df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER1) \
    .option("subscribe", MOVIE_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# Parse JSON
parsed = df.selectExpr("CAST(value AS STRING) AS json") \
           .select(from_json(col("json"), MOVIE_SCHEMA).alias("data")) \
           .select("data.*")

if parsed.rdd.isEmpty():
    print("No data found in Kafka topic.")
    sys.exit(0)

# Transformations (Same as Consumer)
final_df = parsed \
    .withColumn("popularity", col("popularity").cast("double")) \
    .withColumn("vote_average", col("vote_average").cast("double")) \
    .withColumn("budget", col("budget").cast("double")) \
    .withColumn("revenue", col("revenue").cast("double")) \
    .withColumn("runtime", col("runtime").cast("double")) \
    .withColumn("genres", expr("transform(genres, g -> g.name)")) \
    .withColumn("production_companies", expr("transform(production_companies, c -> c.name)")) \
    .withColumn("production_countries", expr("transform(production_countries, c -> c.name)")) \
    .withColumn("release_year", year(to_date("release_date", "yyyy-MM-dd"))) \
    .withColumn("profit_ratio", when(col("budget") > 0, (col("revenue") - col("budget")) / col("budget")).otherwise(None))

count = final_df.count()
print(f"Processing {count} movie records...")

# --- MongoDB Write (Batch Layer View) ---
try:
    print("Writing to MongoDB (BIGDATA.batch_movie)...")
    final_df.write \
        .format("mongodb") \
        .option("connection.uri", CONNECTION_STRING) \
        .option("database", "BIGDATA") \
        .option("collection", "batch_movie") \
        .option("idFieldList", "id") \
        .option("replaceDocument", "true") \
        .mode("overwrite") \
        .save()
    print("MongoDB write SUCCESS")
except Exception as e:
    print(f"MongoDB write FAILED: {e}")

# --- Elasticsearch Write (Batch Layer View) ---
try:
    print("Writing to Elasticsearch (batch-movie)...")
    final_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", ES_NODES) \
        .option("es.port", "9200") \
        .option("es.resource", "batch-movie") \
        .option("es.mapping.id", "id") \
        .option("es.write.operation", "upsert") \
        .option("es.nodes.wan.only", "false") \
        .option("es.net.ssl", "false") \
        .mode("append") \
        .save()
    print("Elasticsearch write SUCCESS")
except Exception as e:
    print(f"Elasticsearch write FAILED: {e}")

print("=== MOVIE BATCH JOB COMPLETED ===")
spark.stop()
