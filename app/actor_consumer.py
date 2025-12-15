# actor_consumer.py
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
MASTER = os.environ["MASTER"]

# Load gender mapping
with open('genders.json', 'r') as f:
    gender_names = {item['id']: item['name'] for item in json.load(f)}

packages = [
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1',
    'org.mongodb.spark:mongo-spark-connector_2.12:10.4.0',
    'org.elasticsearch:elasticsearch-spark-30_2.12:8.15.0'
]

spark = SparkSession.builder \
    .appName("ActorConsumer") \
    .master(MASTER) \
    .config("spark.jars.packages", ",".join(packages)) \
    .getOrCreate()

# Register UDF
def map_gender(gender_id):
    return gender_names.get(gender_id)
spark.udf.register("map_gender", map_gender, "string")

spark.sparkContext.setLogLevel("WARN")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER1) \
    .option("subscribe", ACTOR_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING) AS json") \
           .select(from_json(col("json"), ACTOR_SCHEMA).alias("data")) \
           .select("data.*")

final_df = parsed \
    .withColumn("popularity", col("popularity").cast("double")) \
    .withColumn("gender", expr("map_gender(gender)"))

def write_batch(df, epoch_id):
    if df.rdd.isEmpty():
        sys.stderr.write(f"[{epoch_id}] Empty batch, skipping\n")
        sys.stderr.flush()
        return

    count = df.count()
    sys.stderr.write(f"[{epoch_id}] Writing {count} records\n")
    sys.stderr.flush()

    # MongoDB
    try:
        sys.stderr.write(f"[{epoch_id}] Writing to MongoDB...\n")
        sys.stderr.flush()
        df.write \
          .format("mongodb") \
          .option("connection.uri", CONNECTION_STRING) \
          .option("database", "BIGDATA") \
          .option("collection", "actor") \
          .option("idFieldList", "id") \
          .option("replaceDocument", "true") \
          .mode("append") \
          .save()
        sys.stderr.write(f"[{epoch_id}] MongoDB write SUCCESS\n")
        sys.stderr.flush()
    except Exception as e:
        sys.stderr.write(f"[{epoch_id}] MongoDB write FAILED: {str(e)}\n")
        sys.stderr.flush()

    # Elasticsearch
    try:
        sys.stderr.write(f"[{epoch_id}] Writing to Elasticsearch...\n")
        sys.stderr.flush()
        df.write \
          .format("org.elasticsearch.spark.sql") \
          .option("es.nodes", ES_NODES) \
          .option("es.port", "9200") \
          .option("es.resource", "actor") \
          .option("es.mapping.id", "id") \
          .option("es.write.operation", "upsert") \
          .option("es.nodes.wan.only", "false") \
          .option("es.net.ssl", "false") \
          .mode("append") \
          .save()
        sys.stderr.write(f"[{epoch_id}] Elasticsearch write SUCCESS\n")
        sys.stderr.flush()
    except Exception as e:
        sys.stderr.write(f"[{epoch_id}] Elasticsearch write FAILED: {str(e)}\n")
        sys.stderr.flush()

query = final_df.writeStream \
    .foreachBatch(write_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint/actor_consumer") \
    .start()

query.awaitTermination()