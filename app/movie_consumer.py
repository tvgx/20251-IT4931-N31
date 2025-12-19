# movie_consumer.py - ADVANCED VERSION
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, year, to_date, when, expr, broadcast, 
    avg, count, round as spark_round, max as spark_max, current_timestamp, udf
)
from pyspark.sql.types import StringType, DoubleType
from schema import MOVIE_SCHEMA
import os
from dotenv import load_dotenv
import sys


load_dotenv()


KAFKA_BROKER1 = os.environ["KAFKA_BROKER1"]
MOVIE_TOPIC = os.environ["MOVIE_TOPIC"]
ES_NODES = os.environ["ES_NODES"]
CONNECTION_STRING = os.environ["CONNECTION_STRING"]
MASTER = os.environ["MASTER"]


packages = [
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1',
    'org.mongodb.spark:mongo-spark-connector_2.12:10.4.0',
    'org.elasticsearch:elasticsearch-spark-30_2.12:8.15.0'
]


spark = SparkSession.builder \
    .appName("MovieConsumer") \
    .master(MASTER) \
    .config("spark.jars.packages", ",".join(packages)) \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint/movie_consumer") \
    .getOrCreate()


spark.sparkContext.setLogLevel("WARN")


df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER1) \
    .option("subscribe", MOVIE_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()


parsed = df.selectExpr("CAST(value AS STRING) AS json") \
           .select(from_json(col("json"), MOVIE_SCHEMA).alias("data")) \
           .select("data.*")


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
          .option("collection", "movie") \
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
          .option("es.resource", "movie") \
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
    .option("checkpointLocation", "/tmp/checkpoint/movie_consumer") \
    .start()


query.awaitTermination()
