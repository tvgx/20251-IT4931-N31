# actor_batch.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from schema import ACTOR_SCHEMA
import os
import json
from dotenv import load_dotenv
import sys

load_dotenv()

KAFKA_BROKER1 = os.environ["KAFKA_BROKER1"]
ACTOR_TOPIC = os.environ["ACTOR_TOPIC"]
ES_NODES = os.environ["ES_NODES"]
CONNECTION_STRING = os.environ["CONNECTION_STRING"]
MASTER = os.environ.get("MASTER", "local[*]")

# Load gender mapping
try:
    with open('genders.json', 'r') as f:
        gender_names = {item['id']: item['name'] for item in json.load(f)}
except FileNotFoundError:
    print("Warning: genders.json not found. Gender mapping will fail.")
    gender_names = {}

packages = [
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1',
    'org.mongodb.spark:mongo-spark-connector_2.12:10.4.0',
    'org.elasticsearch:elasticsearch-spark-30_2.12:8.15.0'
]

print("=== STARTING ACTOR BATCH JOB ===")

spark = SparkSession.builder \
    .appName("ActorBatch") \
    .master(MASTER) \
    .config("spark.jars.packages", ",".join(packages)) \
    .getOrCreate()

# Register UDF
def map_gender(gender_id):
    return gender_names.get(gender_id)
spark.udf.register("map_gender", map_gender, "string")

spark.sparkContext.setLogLevel("WARN")

# Read from Kafka in BATCH mode
print(f"Reading from Kafka topic: {ACTOR_TOPIC}...")
df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER1) \
    .option("subscribe", ACTOR_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING) AS json") \
           .select(from_json(col("json"), ACTOR_SCHEMA).alias("data")) \
           .select("data.*")

if parsed.rdd.isEmpty():
    print("No data found in Kafka topic.")
    sys.exit(0)

# Transformations
final_df = parsed \
    .withColumn("popularity", col("popularity").cast("double")) \
    .withColumn("gender", expr("map_gender(gender)"))

count = final_df.count()
print(f"Processing {count} actor records...")

# --- MongoDB Write ---
try:
    print("Writing to MongoDB (BIGDATA.batch_actor)...")
    final_df.write \
        .format("mongodb") \
        .option("connection.uri", CONNECTION_STRING) \
        .option("database", "BIGDATA") \
        .option("collection", "batch_actor") \
        .option("idFieldList", "id") \
        .option("replaceDocument", "true") \
        .mode("overwrite") \
        .save()
    print("MongoDB write SUCCESS")
except Exception as e:
    print(f"MongoDB write FAILED: {e}")

# --- Elasticsearch Write ---
try:
    print("Writing to Elasticsearch (batch-actor)...")
    final_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", ES_NODES) \
        .option("es.port", "9200") \
        .option("es.resource", "batch-actor") \
        .option("es.mapping.id", "id") \
        .option("es.write.operation", "upsert") \
        .option("es.nodes.wan.only", "false") \
        .option("es.net.ssl", "false") \
        .mode("append") \
        .save()
    print("Elasticsearch write SUCCESS")
except Exception as e:
    print(f"Elasticsearch write FAILED: {e}")

print("=== ACTOR BATCH JOB COMPLETED ===")
spark.stop()
