#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, avg, count, sum as spark_sum,
    current_timestamp, lit, to_timestamp, expr, when, year,
    round as spark_round, desc, asc, max as spark_max, min as spark_min,
    collect_list, first, last, split, explode, trim,
    # Advanced aggregations
    approx_count_distinct, stddev, variance, percentile_approx,
    countDistinct, sumDistinct,
    # Date/Time functions
    date_format, hour, minute, dayofweek, dayofyear,
    from_unixtime, unix_timestamp, to_date,
    # String functions
    concat, concat_ws, coalesce, length, lower, upper,
    # Array functions
    array_contains, size, array_distinct,
    # Window functions
    row_number, rank, dense_rank, lag, lead,
    # Other functions
    struct, create_map, monotonically_increasing_id,
    spark_partition_id, input_file_name
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, LongType, TimestampType, FloatType,
    ArrayType, MapType
)
from pyspark import StorageLevel
import os
import sys
from dotenv import load_dotenv

load_dotenv()

# ENVIRONMENT CONFIGURATION
MASTER = os.environ.get("MASTER", "local[*]")
KAFKA_BROKER1 = os.environ.get("KAFKA_BROKER1", "localhost:9092")
MOVIE_TOPIC = os.environ.get("MOVIE_TOPIC", "movie")
CONNECTION_STRING = os.environ.get("CONNECTION_STRING", "mongodb://localhost:27017")
MONGO_ENABLED = os.environ.get("MONGO_ENABLED", "true").lower() == "true"

# Streaming Configuration
WATERMARK_DELAY = os.environ.get("WATERMARK_DELAY", "10 minutes")
WINDOW_DURATION = os.environ.get("WINDOW_DURATION", "5 minutes")
SLIDE_DURATION = os.environ.get("SLIDE_DURATION", "1 minute")
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "30 seconds")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/tmp/checkpoint/speed_layer")

# State Store Configuration
STATE_STORE_MAINTENANCE_INTERVAL = os.environ.get("STATE_STORE_MAINTENANCE", "30s")
MAX_RECORDS_PER_TRIGGER = os.environ.get("MAX_RECORDS_PER_TRIGGER", "1000")

# Spark packages
packages = [
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1',
]
if MONGO_ENABLED:
    packages.append('org.mongodb.spark:mongo-spark-connector_2.12:10.4.0')

print("=" * 80)
print("⚡ ENHANCED SPEED LAYER - LAMBDA ARCHITECTURE")
print("=" * 80)
print(f"📡 Kafka: {KAFKA_BROKER1}")
print(f"📺 Topic: {MOVIE_TOPIC}")
print(f"📦 MongoDB: {CONNECTION_STRING} ({'enabled' if MONGO_ENABLED else 'disabled'})")
print(f"⏱️  Watermark Delay: {WATERMARK_DELAY}")
print(f"🪟 Window Duration: {WINDOW_DURATION}")
print(f"📊 Trigger Interval: {TRIGGER_INTERVAL}")
print(f"📁 Checkpoint Dir: {CHECKPOINT_DIR}")
print("=" * 80)

# SPARK SESSION WITH STREAMING OPTIMIZATIONS
spark = SparkSession.builder \
    .appName("EnhancedSpeedLayer-LambdaArchitecture") \
    .master(MASTER) \
    .config("spark.jars.packages", ",".join(packages)) \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    .config("spark.mongodb.write.connection.uri", CONNECTION_STRING) \
    .config("spark.sql.streaming.stateStore.providerClass", 
            "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
    .config("spark.sql.streaming.stateStore.maintenanceInterval", STATE_STORE_MAINTENANCE_INTERVAL) \
    .config("spark.sql.streaming.metricsEnabled", "true") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.streaming.noDataMicroBatches.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# SCHEMAS
MOVIE_STREAM_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("vote_average", StringType(), True),
    StructField("vote_count", IntegerType(), True),
    StructField("popularity", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("revenue", StringType(), True),
    StructField("budget", StringType(), True),
    StructField("runtime", StringType(), True),
    StructField("genres", StringType(), True),
    StructField("original_language", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("adult", BooleanType(), True),
    StructField("production_companies", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("ingested_at", StringType(), True)
])

# Nested genre schema for streaming
GENRE_ARRAY_SCHEMA = ArrayType(StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
]))

# CUSTOM UDFs FOR SPEED LAYER
from pyspark.sql.functions import udf

@udf(returnType=StringType())
def categorize_popularity_stream(popularity):
    """UDF: Categorize movie by popularity score"""
    if popularity is None:
        return "Unknown"
    try:
        pop = float(popularity)
        if pop >= 100:
            return "Viral"
        elif pop >= 50:
            return "Trending"
        elif pop >= 20:
            return "Popular"
        elif pop >= 10:
            return "Moderate"
        else:
            return "Niche"
    except:
        return "Unknown"

@udf(returnType=StringType())
def categorize_rating_stream(rating):
    """UDF: Categorize movie by rating"""
    if rating is None:
        return "Unknown"
    try:
        r = float(rating)
        if r >= 8.0:
            return "Excellent"
        elif r >= 7.0:
            return "Good"
        elif r >= 5.0:
            return "Average"
        elif r >= 3.0:
            return "Below Average"
        else:
            return "Poor"
    except:
        return "Unknown"

@udf(returnType=StringType())
def extract_hour_period(hour_val):
    """UDF: Extract time period from hour"""
    if hour_val is None:
        return "Unknown"
    if 6 <= hour_val < 12:
        return "Morning"
    elif 12 <= hour_val < 17:
        return "Afternoon"
    elif 17 <= hour_val < 21:
        return "Evening"
    else:
        return "Night"

@udf(returnType=FloatType())
def safe_float_convert(value):
    """UDF: Safely convert string to float"""
    if value is None:
        return None
    try:
        return float(value)
    except:
        return None

@udf(returnType=StringType())
def flatten_genres(genres_str):
    """UDF: Flatten genres array from JSON to comma-separated string"""
    if genres_str is None:
        return ""
    try:
        import json
        genres = json.loads(genres_str)
        if isinstance(genres, list):
            return ", ".join([g.get("name", "") for g in genres if g and g.get("name")])
        return ""
    except:
        return genres_str if isinstance(genres_str, str) else ""


# MONGODB HELPERS (EXACTLY-ONCE WRITES)
def save_to_mongodb_upsert(df, collection_name, id_field, epoch_id=None):
    """
    Save DataFrame to MongoDB with UPSERT for exactly-once semantics
    Using idFieldList ensures idempotent writes
    """
    if not MONGO_ENABLED:
        return
    
    try:
        df.write \
            .format("mongodb") \
            .option("connection.uri", CONNECTION_STRING) \
            .option("database", "BIGDATA") \
            .option("collection", collection_name) \
            .option("idFieldList", id_field) \
            .option("replaceDocument", "true") \
            .mode("append") \
            .save()
        
        if epoch_id is not None:
            sys.stderr.write(f"[Epoch {epoch_id}] ✅ MongoDB: {collection_name}\n")
        else:
            print(f"   ✅ Saved to MongoDB: {collection_name}")
        return True
    except Exception as e:
        if epoch_id is not None:
            sys.stderr.write(f"[Epoch {epoch_id}] ❌ MongoDB ({collection_name}): {str(e)}\n")
        else:
            print(f"   ❌ MongoDB error ({collection_name}): {str(e)}")
        return False


# STREAM PROCESSING FUNCTIONS
def process_micro_batch(df, epoch_id):
    """
    Process each micro-batch with complex aggregations
    Implements exactly-once semantics through idempotent upsert operations
    """
    if df.rdd.isEmpty():
        sys.stderr.write(f"[Epoch {epoch_id}] Empty batch, skipping\n")
        return
    
    batch_count = df.count()
    sys.stderr.write(f"\n{'='*60}\n")
    sys.stderr.write(f"[Epoch {epoch_id}] Processing {batch_count} records...\n")
    sys.stderr.write(f"{'='*60}\n")
    
    # STAGE 1: DATA TRANSFORMATION WITH CUSTOM UDFs
    processed_df = df \
        .withColumn("vote_average", safe_float_convert(col("vote_average"))) \
        .withColumn("popularity", safe_float_convert(col("popularity"))) \
        .withColumn("revenue", col("revenue").cast("long")) \
        .withColumn("budget", col("budget").cast("long")) \
        .withColumn("runtime", col("runtime").cast("integer")) \
        .withColumn("release_year", year(to_date(col("release_date")))) \
        .withColumn("popularity_category", categorize_popularity_stream(col("popularity"))) \
        .withColumn("rating_category", categorize_rating_stream(col("vote_average"))) \
        .withColumn("genres_flat", flatten_genres(col("genres"))) \
        .withColumn("processed_at", current_timestamp()) \
        .withColumn("layer", lit("speed")) \
        .withColumn("epoch_id", lit(epoch_id)) \
        .withColumn("processing_hour", hour(current_timestamp())) \
        .withColumn("processing_day", dayofweek(current_timestamp())) \
        .withColumn("time_period", extract_hour_period(hour(current_timestamp()))) \
        .withColumn("is_adult", col("adult") == True) \
        .withColumn("has_revenue", col("revenue") > 0) \
        .withColumn("has_budget", col("budget") > 0) \
        .withColumn("profit", 
            when((col("revenue") > 0) & (col("budget") > 0), 
                 col("revenue") - col("budget"))
            .otherwise(None)) \
        .withColumn("is_profitable",
            when((col("revenue") > 0) & (col("budget") > 0),
                 col("revenue") > col("budget"))
            .otherwise(None))
    
    # Cache for multiple uses
    processed_df.cache()
    
    # STAGE 2: SAVE RAW SPEED DATA (EXACTLY-ONCE)
    sys.stderr.write(f"[Epoch {epoch_id}] Saving raw speed data...\n")
    save_to_mongodb_upsert(processed_df, "speed_movies", "id", epoch_id)
    
    # STAGE 3: REAL-TIME AGGREGATIONS
    compute_realtime_aggregations(processed_df, epoch_id)
    
    # Unpersist cached data
    processed_df.unpersist()


def compute_realtime_aggregations(df, epoch_id):
    """
    Compute various real-time aggregations for Speed Layer Views
    """
    sys.stderr.write(f"[Epoch {epoch_id}] Computing aggregations...\n")
    
    # 3.1: REAL-TIME GENRE STATISTICS
    try:
        genre_exploded = df \
            .filter(col("genres_flat").isNotNull()) \
            .filter(col("genres_flat") != "") \
            .withColumn("genre", explode(split(col("genres_flat"), ", "))) \
            .withColumn("genre", trim(col("genre"))) \
            .filter(col("genre") != "")
        
        if genre_exploded.count() > 0:
            # Basic genre stats
            genre_stats = genre_exploded.groupBy("genre").agg(
                count("*").alias("movie_count"),
                spark_round(avg("vote_average"), 2).alias("avg_rating"),
                spark_round(avg("popularity"), 2).alias("avg_popularity"),
                spark_round(stddev("vote_average"), 2).alias("rating_stddev"),
                spark_max("vote_average").alias("max_rating"),
                spark_min("vote_average").alias("min_rating"),
                spark_sum("revenue").alias("total_revenue"),
                approx_count_distinct("original_language").alias("unique_languages")
            ).withColumn("updated_at", current_timestamp()) \
             .withColumn("layer", lit("speed")) \
             .withColumn("epoch_id", lit(epoch_id))
            
            save_to_mongodb_upsert(genre_stats, "speed_genre_stats", "genre", epoch_id)
            
            # Genre with window ranking (within this batch)
            genre_window = Window.orderBy(desc("avg_rating"))
            genre_ranked = genre_stats \
                .withColumn("rating_rank", rank().over(genre_window)) \
                .withColumn("rating_percentile", 
                    spark_round((1 - (row_number().over(genre_window) - 1) / count("*").over(Window.partitionBy(lit(1)))) * 100, 1))
            
            save_to_mongodb_upsert(genre_ranked, "speed_genre_ranked", "genre", epoch_id)
    except Exception as e:
        sys.stderr.write(f"[Epoch {epoch_id}] ❌ Genre stats error: {str(e)}\n")
    
    # 3.2: REAL-TIME YEAR STATISTICS
    try:
        year_stats = df.filter(col("release_year").isNotNull()).groupBy("release_year").agg(
            count("*").alias("movie_count"),
            spark_round(avg("vote_average"), 2).alias("avg_rating"),
            spark_round(avg("popularity"), 2).alias("avg_popularity"),
            spark_sum("revenue").alias("total_revenue"),
            spark_sum("budget").alias("total_budget"),
            count(when(col("is_profitable") == True, True)).alias("profitable_count"),
            spark_round(avg("runtime"), 0).alias("avg_runtime")
        ).withColumn("updated_at", current_timestamp()) \
         .withColumn("layer", lit("speed")) \
         .withColumn("epoch_id", lit(epoch_id)) \
         .withColumn("profitability_rate",
            when(col("movie_count") > 0,
                 spark_round(col("profitable_count") / col("movie_count") * 100, 2))
            .otherwise(None))
        
        if year_stats.count() > 0:
            # Add year-over-year analysis within batch
            year_window = Window.orderBy("release_year")
            year_stats_with_yoy = year_stats \
                .withColumn("prev_year_rating", lag("avg_rating", 1).over(year_window)) \
                .withColumn("yoy_rating_change",
                    spark_round(col("avg_rating") - col("prev_year_rating"), 2))
            
            save_to_mongodb_upsert(year_stats_with_yoy, "speed_year_stats", "release_year", epoch_id)
    except Exception as e:
        sys.stderr.write(f"[Epoch {epoch_id}] ❌ Year stats error: {str(e)}\n")
    
    # 3.3: REAL-TIME LANGUAGE STATISTICS
    try:
        lang_stats = df.filter(col("original_language").isNotNull()).groupBy("original_language").agg(
            count("*").alias("movie_count"),
            spark_round(avg("vote_average"), 2).alias("avg_rating"),
            spark_round(avg("popularity"), 2).alias("avg_popularity"),
            approx_count_distinct("release_year").alias("active_years")
        ).withColumn("updated_at", current_timestamp()) \
         .withColumn("layer", lit("speed")) \
         .withColumn("epoch_id", lit(epoch_id))
        
        if lang_stats.count() > 0:
            save_to_mongodb_upsert(lang_stats, "speed_language_stats", "original_language", epoch_id)
    except Exception as e:
        sys.stderr.write(f"[Epoch {epoch_id}] ❌ Language stats error: {str(e)}\n")
    
    # 3.4: REAL-TIME TOP MOVIES (WITH WINDOW RANKINGS)
    try:
        rating_window = Window.orderBy(desc("vote_average"), desc("popularity"))
        popularity_window = Window.orderBy(desc("popularity"))
        
        top_movies = df \
            .filter(col("title").isNotNull()) \
            .withColumn("rating_rank", rank().over(rating_window)) \
            .withColumn("popularity_rank", rank().over(popularity_window)) \
            .orderBy(desc("popularity")) \
            .limit(100) \
            .select(
                "id", "title", "vote_average", "popularity", "release_year",
                "genres_flat", "original_language", "rating_category",
                "popularity_category", "rating_rank", "popularity_rank",
                "processed_at", "layer", "epoch_id"
            ).withColumn("updated_at", current_timestamp())
        
        save_to_mongodb_upsert(top_movies, "speed_top_movies", "id", epoch_id)
    except Exception as e:
        sys.stderr.write(f"[Epoch {epoch_id}] ❌ Top movies error: {str(e)}\n")
    
    # 3.5: POPULARITY DISTRIBUTION (PIVOT-LIKE)
    try:
        popularity_dist = df.groupBy("popularity_category").agg(
            count("*").alias("movie_count"),
            spark_round(avg("vote_average"), 2).alias("avg_rating"),
            spark_round(avg("popularity"), 2).alias("avg_popularity"),
            spark_max("popularity").alias("max_popularity"),
            spark_min("popularity").alias("min_popularity")
        ).withColumn("updated_at", current_timestamp()) \
         .withColumn("layer", lit("speed")) \
         .withColumn("epoch_id", lit(epoch_id))
        
        if popularity_dist.count() > 0:
            save_to_mongodb_upsert(popularity_dist, "speed_popularity_distribution", "popularity_category", epoch_id)
    except Exception as e:
        sys.stderr.write(f"[Epoch {epoch_id}] ❌ Popularity distribution error: {str(e)}\n")
    
    # 3.6: RATING DISTRIBUTION
    try:
        rating_dist = df.groupBy("rating_category").agg(
            count("*").alias("movie_count"),
            spark_round(avg("popularity"), 2).alias("avg_popularity"),
            spark_round(avg("vote_average"), 2).alias("avg_rating")
        ).withColumn("updated_at", current_timestamp()) \
         .withColumn("layer", lit("speed")) \
         .withColumn("epoch_id", lit(epoch_id))
        
        if rating_dist.count() > 0:
            save_to_mongodb_upsert(rating_dist, "speed_rating_distribution", "rating_category", epoch_id)
    except Exception as e:
        sys.stderr.write(f"[Epoch {epoch_id}] ❌ Rating distribution error: {str(e)}\n")
    
    # 3.7: TIME-BASED PROCESSING STATS
    try:
        time_stats = df.groupBy("time_period").agg(
            count("*").alias("movies_processed"),
            spark_round(avg("vote_average"), 2).alias("avg_rating"),
            spark_round(avg("popularity"), 2).alias("avg_popularity")
        ).withColumn("updated_at", current_timestamp()) \
         .withColumn("layer", lit("speed")) \
         .withColumn("epoch_id", lit(epoch_id))
        
        if time_stats.count() > 0:
            save_to_mongodb_upsert(time_stats, "speed_time_stats", "time_period", epoch_id)
    except Exception as e:
        sys.stderr.write(f"[Epoch {epoch_id}] ❌ Time stats error: {str(e)}\n")
    
    sys.stderr.write(f"[Epoch {epoch_id}] ✅ All aggregations completed\n")


# WINDOW AGGREGATION STREAMS
def create_window_aggregation_query(df, output_mode="update"):
    """
    Create window-based aggregation stream
    Uses tumbling windows with watermarking
    """
    # Genre-based windowed aggregation
    genre_windowed = df \
        .withColumn("genres_flat", flatten_genres(col("genres"))) \
        .filter(col("genres_flat") != "") \
        .withColumn("genre", explode(split(col("genres_flat"), ", "))) \
        .withColumn("genre", trim(col("genre"))) \
        .filter(col("genre") != "") \
        .withColumn("vote_average", safe_float_convert(col("vote_average"))) \
        .withColumn("popularity", safe_float_convert(col("popularity"))) \
        .groupBy(
            window(col("event_time"), WINDOW_DURATION, SLIDE_DURATION),
            col("genre")
        ).agg(
            count("*").alias("movie_count"),
            spark_round(avg("vote_average"), 2).alias("avg_rating"),
            spark_round(avg("popularity"), 2).alias("avg_popularity"),
            approx_count_distinct("id").alias("unique_movies"),
            spark_max("vote_average").alias("max_rating"),
            spark_min("vote_average").alias("min_rating")
        )
    
    return genre_windowed


def create_trending_analysis_query(df):
    """
    Create sliding window for trending analysis
    Identifies trending content based on popularity changes
    """
    trending_df = df \
        .withColumn("popularity", safe_float_convert(col("popularity"))) \
        .withColumn("vote_average", safe_float_convert(col("vote_average"))) \
        .groupBy(
            window(col("event_time"), "10 minutes", "2 minutes")
        ).agg(
            count("*").alias("total_movies"),
            spark_round(avg("popularity"), 2).alias("avg_popularity"),
            spark_max("popularity").alias("max_popularity"),
            spark_min("popularity").alias("min_popularity"),
            stddev("popularity").alias("popularity_stddev"),
            spark_round(avg("vote_average"), 2).alias("avg_rating"),
            percentile_approx("popularity", 0.9).alias("popularity_90th_percentile")
        )
    
    return trending_df


# MAIN SPEED LAYER EXECUTION
def run_speed_layer():
    """
    Main function to run enhanced speed layer with advanced streaming features
    
    STREAMING GUARANTEES:
    - Watermarking: Handle late data up to WATERMARK_DELAY
    - Checkpointing: Exactly-once delivery via checkpoint recovery
    - Idempotent writes: MongoDB upsert operations prevent duplicates
    """
    print("\n" + "=" * 80)
    print("⚡ STARTING ENHANCED SPEED LAYER STREAMING")
    print("=" * 80)
    
    # READ FROM KAFKA WITH EXACTLY-ONCE CONSUMER SEMANTICS
    print("\n📡 Connecting to Kafka...")
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER1) \
        .option("subscribe", MOVIE_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", MAX_RECORDS_PER_TRIGGER) \
        .load()
    
    print("   ✅ Kafka connection established")
    
    # PARSE JSON AND ADD EVENT TIMESTAMP FOR WATERMARKING
    print("\n🔄 Parsing stream with watermarking...")
    parsed_df = kafka_df \
        .selectExpr(
            "CAST(value AS STRING) AS json",
            "timestamp AS kafka_timestamp",
            "partition AS kafka_partition",
            "offset AS kafka_offset"
        ) \
        .select(
            from_json(col("json"), MOVIE_STREAM_SCHEMA).alias("data"),
            col("kafka_timestamp"),
            col("kafka_partition"),
            col("kafka_offset")
        ) \
        .select("data.*", "kafka_timestamp", "kafka_partition", "kafka_offset") \
        .withColumn("event_time", 
            coalesce(
                to_timestamp(col("event_time")),
                to_timestamp(col("ingested_at")),
                col("kafka_timestamp")
            )
        )
    
    # APPLY WATERMARKING FOR LATE DATA HANDLING
    print(f"   ⏱️  Applying watermark: {WATERMARK_DELAY}")
    watermarked_df = parsed_df.withWatermark("event_time", WATERMARK_DELAY)
    
    # STREAMING QUERY 1: MAIN PROCESSING (foreachBatch)
    # - Handles complex transformations
    # - Exactly-once via checkpointing + idempotent writes
    print("\n🚀 Starting streaming queries...")
    print("   📊 Query 1: Main micro-batch processing")
    
    query1 = watermarked_df \
        .writeStream \
        .queryName("speed_layer_main") \
        .foreachBatch(process_micro_batch) \
        .outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/main") \
        .trigger(processingTime=TRIGGER_INTERVAL) \
        .start()
    
    # STREAMING QUERY 2: WINDOW-BASED AGGREGATIONS
    # - Tumbling window aggregation
    # - Update mode for aggregations
    print("   📊 Query 2: Window-based genre aggregation")
    
    genre_windowed = create_window_aggregation_query(watermarked_df)
    
    query2 = genre_windowed \
        .writeStream \
        .queryName("genre_window_aggregation") \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/windows") \
        .trigger(processingTime=TRIGGER_INTERVAL) \
        .start()
    
    # STREAMING QUERY 3: TRENDING ANALYSIS (Sliding Window)
    print("   📊 Query 3: Trending analysis (sliding window)")
    
    trending_df = create_trending_analysis_query(watermarked_df)
    
    query3 = trending_df \
        .writeStream \
        .queryName("trending_analysis") \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/trending") \
        .trigger(processingTime="1 minute") \
        .start()
    
    # STREAMING QUERY 4: DEDUPLICATION (Stateful)
    # - Drop duplicates based on id within watermark window
    print("   📊 Query 4: Deduplication stream")
    
    deduped_df = watermarked_df \
        .dropDuplicates(["id"]) \
        .select("id", "title", "vote_average", "popularity", "event_time")
    
    query4 = deduped_df \
        .writeStream \
        .queryName("deduplication_stream") \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "true") \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/dedup") \
        .trigger(processingTime=TRIGGER_INTERVAL) \
        .start()
    
    # STREAMING STATUS
    print("\n" + "=" * 80)
    print("⚡ ENHANCED SPEED LAYER STREAMING STARTED!")
    print("=" * 80)
    
    print("\n🎯 ADVANCED STREAMING FEATURES ACTIVE:")
    print(f"   ✅ Watermarking: {WATERMARK_DELAY} delay tolerance")
    print(f"   ✅ Window Aggregations: {WINDOW_DURATION} tumbling windows")
    print(f"   ✅ Sliding Windows: 10 minutes with 2 minute slide")
    print(f"   ✅ Exactly-once: Checkpointing + idempotent MongoDB upserts")
    print(f"   ✅ State Management: HDFS-backed state store")
    print(f"   ✅ Deduplication: Stateful duplicate removal")
    print(f"   ✅ Trigger Interval: {TRIGGER_INTERVAL}")
    print(f"   ✅ Rate Limiting: Max {MAX_RECORDS_PER_TRIGGER} records/trigger")
    
    print("\n📊 ACTIVE QUERIES:")
    for query in spark.streams.active:
        print(f"   - {query.name}: {query.status}")
    
    print("\n   Waiting for data from Kafka...")
    print("   Press Ctrl+C to stop gracefully")
    
    # Wait for all queries
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n\n⚠️  Graceful shutdown initiated...")
        for query in spark.streams.active:
            query.stop()
        print("   ✅ All queries stopped")


if __name__ == "__main__":
    try:
        run_speed_layer()
    except Exception as e:
        print(f"\n❌ SPEED LAYER FAILED: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
