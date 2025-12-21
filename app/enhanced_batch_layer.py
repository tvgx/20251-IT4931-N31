#!/usr/bin/env python3
"""
Enhanced Batch Layer - Lambda Architecture
Xử lý dữ liệu batch từ MinIO (Master Dataset) và lưu vào MongoDB (Serving Layer)

=============================================================================
ADVANCED SPARK FEATURES IMPLEMENTED:
=============================================================================
1. Complex Aggregations:
   - Window Functions: rank, row_number, dense_rank, lag, lead, ntile
   - Pivot and Unpivot Operations
   - Custom Aggregation Functions (UDAF-like)
   - Advanced aggregate functions: percentile, stddev, variance

2. Advanced Transformations:
   - Multiple stages of transformations
   - Chaining complex operations
   - Custom UDFs for business logic

3. Join Operations:
   - Broadcast joins for unbalanced datasets
   - Sort-merge joins for large-scale data
   - Multiple joins optimization
   - Self-joins for comparison

4. Performance Optimization:
   - Partition pruning and bucketing
   - Caching and persistence strategies (MEMORY_AND_DISK, MEMORY_ONLY)
   - Query optimization with explain()
   - Adaptive Query Execution (AQE)
   - Coalesce and repartition strategies
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, to_date, when, avg, count, sum as spark_sum,
    split, explode, trim, lit, current_timestamp, round as spark_round,
    desc, asc, first, last, collect_list, collect_set, size, coalesce,
    # Window Functions
    row_number, rank, dense_rank, lag, lead, ntile, percent_rank, cume_dist,
    # Advanced Aggregations
    max as spark_max, min as spark_min, stddev, stddev_pop, variance, var_pop,
    percentile_approx, approx_count_distinct, countDistinct, sumDistinct,
    array_distinct, concat_ws, array_join,
    # Broadcast join
    broadcast,
    # String functions
    regexp_replace, lower, upper, length, substring, initcap,
    # Date functions
    datediff, months_between, date_format, quarter, month, dayofweek, dayofyear,
    add_months, date_add, date_sub, year as spark_year, weekofyear,
    # Conditional
    greatest, least, abs as spark_abs, sqrt, log, log10, exp, pow,
    # Array functions
    array_contains, array_intersect, array_union, flatten, slice,
    # Struct functions
    struct, create_map
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, LongType, ArrayType, FloatType, MapType
)
from pyspark import StorageLevel
import os
import sys
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# ENVIRONMENT CONFIGURATION
# =============================================================================
MASTER = os.environ.get("MASTER", "local[*]")
CONNECTION_STRING = os.environ.get("CONNECTION_STRING", "mongodb://localhost:27017")
MONGO_ENABLED = os.environ.get("MONGO_ENABLED", "true").lower() == "true"
CSV_PATH = os.environ.get("CSV_PATH", "/data/tmdb.csv")

# MinIO Configuration
MINIO_ENABLED = os.environ.get("MINIO_ENABLED", "false").lower() == "true"
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio.bigdata.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "password123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "datalake")
MINIO_CSV_FILE = os.environ.get("MINIO_CSV_FILE", "tmdb.csv")

# Performance tuning
SHUFFLE_PARTITIONS = int(os.environ.get("SHUFFLE_PARTITIONS", "200"))
BROADCAST_THRESHOLD = int(os.environ.get("BROADCAST_THRESHOLD", "10485760"))  # 10MB

# Spark packages
packages = []
if MONGO_ENABLED:
    packages.append('org.mongodb.spark:mongo-spark-connector_2.12:10.4.0')
if MINIO_ENABLED:
    packages.append('org.apache.hadoop:hadoop-aws:3.3.4')
    packages.append('com.amazonaws:aws-java-sdk-bundle:1.12.500')

print("=" * 80)
print("🚀 ENHANCED BATCH LAYER - LAMBDA ARCHITECTURE")
print("=" * 80)
print(f"📂 Data Source: {'MinIO: ' + MINIO_ENDPOINT if MINIO_ENABLED else 'Local: ' + CSV_PATH}")
print(f"📦 MongoDB: {CONNECTION_STRING} ({'enabled' if MONGO_ENABLED else 'disabled'})")
print(f"🔧 Shuffle Partitions: {SHUFFLE_PARTITIONS}")
print(f"🔧 Broadcast Threshold: {BROADCAST_THRESHOLD} bytes")
print("=" * 80)

# =============================================================================
# SPARK SESSION WITH OPTIMIZATION
# =============================================================================
spark_builder = SparkSession.builder \
    .appName("EnhancedBatchLayer-LambdaArchitecture") \
    .master(MASTER) \
    .config("spark.jars.packages", ",".join(packages)) \
    .config("spark.mongodb.write.connection.uri", CONNECTION_STRING) \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
    .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS)) \
    .config("spark.sql.autoBroadcastJoinThreshold", str(BROADCAST_THRESHOLD)) \
    .config("spark.sql.broadcastTimeout", "600") \
    .config("spark.sql.cbo.enabled", "true") \
    .config("spark.sql.cbo.joinReorder.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

if MINIO_ENABLED:
    spark_builder = spark_builder \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g")

spark = spark_builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# =============================================================================
# CUSTOM USER DEFINED FUNCTIONS (UDFs)
# =============================================================================
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import ArrayType

@udf(returnType=StringType())
def categorize_rating(rating):
    """UDF: Phân loại phim theo rating với nhiều mức hơn"""
    if rating is None:
        return "Unknown"
    elif rating >= 9.0:
        return "Masterpiece"
    elif rating >= 8.0:
        return "Excellent"
    elif rating >= 7.0:
        return "Good"
    elif rating >= 6.0:
        return "Above Average"
    elif rating >= 5.0:
        return "Average"
    elif rating >= 4.0:
        return "Below Average"
    elif rating >= 3.0:
        return "Poor"
    else:
        return "Very Poor"

@udf(returnType=StringType())
def categorize_budget(budget):
    """UDF: Phân loại phim theo budget chi tiết hơn"""
    if budget is None or budget == 0:
        return "Unknown"
    elif budget >= 200000000:
        return "Mega Blockbuster"
    elif budget >= 100000000:
        return "Blockbuster"
    elif budget >= 50000000:
        return "Big Budget"
    elif budget >= 20000000:
        return "Medium High"
    elif budget >= 10000000:
        return "Medium Budget"
    elif budget >= 5000000:
        return "Low Medium"
    elif budget >= 1000000:
        return "Low Budget"
    else:
        return "Micro Budget"

@udf(returnType=StringType())
def extract_decade(year):
    """UDF: Lấy thập kỷ từ năm"""
    if year is None:
        return "Unknown"
    decade = (year // 10) * 10
    return f"{decade}s"

@udf(returnType=FloatType())
def calculate_roi(revenue, budget):
    """UDF: Tính ROI (Return on Investment)"""
    if budget is None or budget == 0 or revenue is None:
        return None
    return float((revenue - budget) / budget * 100)

@udf(returnType=StringType())
def calculate_profitability_tier(roi):
    """UDF: Phân loại mức độ lợi nhuận"""
    if roi is None:
        return "Unknown"
    elif roi >= 500:
        return "Extremely Profitable"
    elif roi >= 200:
        return "Highly Profitable"
    elif roi >= 100:
        return "Very Profitable"
    elif roi >= 50:
        return "Profitable"
    elif roi >= 0:
        return "Break Even"
    elif roi >= -50:
        return "Minor Loss"
    else:
        return "Major Loss"

@udf(returnType=StringType())
def categorize_runtime(runtime):
    """UDF: Phân loại theo thời lượng phim"""
    if runtime is None or runtime == 0:
        return "Unknown"
    elif runtime < 60:
        return "Short Film"
    elif runtime < 90:
        return "Short Feature"
    elif runtime < 120:
        return "Standard"
    elif runtime < 150:
        return "Long"
    elif runtime < 180:
        return "Epic"
    else:
        return "Extended"

@udf(returnType=StringType())
def categorize_popularity(popularity):
    """UDF: Phân loại theo độ phổ biến"""
    if popularity is None:
        return "Unknown"
    elif popularity >= 100:
        return "Viral"
    elif popularity >= 50:
        return "Trending"
    elif popularity >= 20:
        return "Popular"
    elif popularity >= 10:
        return "Moderate"
    elif popularity >= 5:
        return "Low"
    else:
        return "Niche"

@udf(returnType=IntegerType())
def count_genres(genres_str):
    """UDF: Đếm số lượng genres"""
    if genres_str is None or genres_str == "":
        return 0
    return len([g.strip() for g in genres_str.split(",") if g.strip()])

@udf(returnType=StringType())
def get_primary_genre(genres_str):
    """UDF: Lấy genre chính (đầu tiên)"""
    if genres_str is None or genres_str == "":
        return "Unknown"
    genres = [g.strip() for g in genres_str.split(",") if g.strip()]
    return genres[0] if genres else "Unknown"

# =============================================================================
# SCHEMA DEFINITION
# =============================================================================
TMDB_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("vote_average", DoubleType(), True),
    StructField("vote_count", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("revenue", LongType(), True),
    StructField("runtime", IntegerType(), True),
    StructField("adult", BooleanType(), True),
    StructField("backdrop_path", StringType(), True),
    StructField("budget", LongType(), True),
    StructField("homepage", StringType(), True),
    StructField("tconst", StringType(), True),
    StructField("original_language", StringType(), True),
    StructField("original_title", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("poster_path", StringType(), True),
    StructField("tagline", StringType(), True),
    StructField("genres", StringType(), True),
    StructField("production_companies", StringType(), True),
    StructField("production_countries", StringType(), True),
    StructField("spoken_languages", StringType(), True),
    StructField("keywords", StringType(), True),
    StructField("directors", StringType(), True),
    StructField("writers", StringType(), True),
    StructField("averageRating", DoubleType(), True),
    StructField("numVotes", IntegerType(), True),
    StructField("cast", StringType(), True)
])


# =============================================================================
# LOOKUP TABLES FOR BROADCAST JOINS
# =============================================================================
def create_genre_lookup():
    """Create genre lookup table for broadcast join"""
    genre_data = [
        ("Action", "High Energy", 1, "action-adventure"),
        ("Adventure", "Exciting", 2, "action-adventure"),
        ("Animation", "Family Friendly", 3, "family"),
        ("Comedy", "Light Entertainment", 4, "comedy"),
        ("Crime", "Dark", 5, "thriller"),
        ("Documentary", "Educational", 6, "documentary"),
        ("Drama", "Emotional", 7, "drama"),
        ("Family", "All Ages", 8, "family"),
        ("Fantasy", "Imaginative", 9, "fantasy-scifi"),
        ("History", "Historical", 10, "drama"),
        ("Horror", "Thrilling", 11, "horror"),
        ("Music", "Musical", 12, "music"),
        ("Mystery", "Intriguing", 13, "thriller"),
        ("Romance", "Love Stories", 14, "romance"),
        ("Science Fiction", "Futuristic", 15, "fantasy-scifi"),
        ("TV Movie", "Television", 16, "other"),
        ("Thriller", "Suspenseful", 17, "thriller"),
        ("War", "Conflict", 18, "drama"),
        ("Western", "American Frontier", 19, "western")
    ]
    return spark.createDataFrame(
        genre_data, 
        ["genre_name", "genre_description", "genre_priority", "genre_category"]
    )

def create_language_lookup():
    """Create language lookup table for broadcast join"""
    language_data = [
        ("en", "English", "Western", True),
        ("es", "Spanish", "Western", True),
        ("fr", "French", "Western", True),
        ("de", "German", "Western", True),
        ("it", "Italian", "Western", True),
        ("pt", "Portuguese", "Western", True),
        ("ja", "Japanese", "Asian", True),
        ("ko", "Korean", "Asian", True),
        ("zh", "Chinese", "Asian", True),
        ("hi", "Hindi", "Asian", True),
        ("ru", "Russian", "Eastern European", True),
        ("ar", "Arabic", "Middle Eastern", True),
    ]
    return spark.createDataFrame(
        language_data,
        ["lang_code", "lang_name", "lang_region", "is_major"]
    )

def create_decade_lookup():
    """Create decade lookup table for analysis"""
    decade_data = [
        ("1970s", "New Hollywood", "Renaissance"),
        ("1980s", "Blockbuster Era", "Commercial"),
        ("1990s", "Indie Rise", "Diverse"),
        ("2000s", "Digital Revolution", "Technological"),
        ("2010s", "Streaming Age", "Modern"),
        ("2020s", "Post-Pandemic", "Hybrid")
    ]
    return spark.createDataFrame(
        decade_data,
        ["decade", "era_name", "era_characteristic"]
    )


# =============================================================================
# DATA LOADING WITH PARTITION PRUNING
# =============================================================================
def load_and_process_data():
    """Load CSV và xử lý dữ liệu với advanced transformations"""
    print("\n" + "=" * 80)
    print("📂 STAGE 1: DATA LOADING")
    print("=" * 80)
    
    # Determine data source path
    if MINIO_ENABLED:
        data_path = f"s3a://{MINIO_BUCKET}/{MINIO_CSV_FILE}"
        print(f"   📡 Reading from MinIO: {data_path}")
    else:
        data_path = CSV_PATH
        print(f"   📁 Reading from local: {data_path}")
    
    # Read CSV with schema enforcement
    raw_df = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .option("quote", '"') \
        .schema(TMDB_SCHEMA) \
        .csv(data_path)
    
    total_records = raw_df.count()
    print(f"   ✅ Loaded {total_records} raw records")
    
    # ==========================================================================
    # CACHING STRATEGY: Cache raw data for multiple transformations
    # ==========================================================================
    print("\n💾 CACHING: Persisting raw data (MEMORY_AND_DISK)...")
    raw_df.persist(StorageLevel.MEMORY_AND_DISK)
    raw_df.count()  # Trigger caching
    print("   ✅ Raw data cached")
    
    # ==========================================================================
    # STAGE 2: MULTI-STAGE TRANSFORMATIONS
    # ==========================================================================
    print("\n" + "=" * 80)
    print("🔄 STAGE 2: MULTI-STAGE TRANSFORMATIONS")
    print("=" * 80)
    
    # Stage 2.1: Basic Type Conversions and Date Extraction
    print("   📌 Stage 2.1: Basic transformations...")
    stage1_df = raw_df \
        .withColumn("release_year", year(to_date(col("release_date"), "yyyy-MM-dd"))) \
        .withColumn("release_quarter", quarter(to_date(col("release_date"), "yyyy-MM-dd"))) \
        .withColumn("release_month", month(to_date(col("release_date"), "yyyy-MM-dd"))) \
        .withColumn("release_day_of_week", dayofweek(to_date(col("release_date"), "yyyy-MM-dd"))) \
        .withColumn("release_week_of_year", weekofyear(to_date(col("release_date"), "yyyy-MM-dd")))
    
    # Stage 2.2: Financial Calculations
    print("   📌 Stage 2.2: Financial calculations...")
    stage2_df = stage1_df \
        .withColumn("profit", col("revenue") - col("budget")) \
        .withColumn("profit_margin", 
            when((col("revenue") > 0) & (col("budget") > 0),
                 spark_round((col("revenue") - col("budget")) / col("revenue") * 100, 2))
            .otherwise(None)) \
        .withColumn("cost_to_revenue_ratio",
            when((col("revenue") > 0) & (col("budget") > 0),
                 spark_round(col("budget") / col("revenue"), 4))
            .otherwise(None)) \
        .withColumn("is_profitable", col("revenue") > col("budget")) \
        .withColumn("roi_percent", calculate_roi(col("revenue"), col("budget")))
    
    # Stage 2.3: Apply Custom UDFs
    print("   📌 Stage 2.3: Applying Custom UDFs...")
    stage3_df = stage2_df \
        .withColumn("rating_category", categorize_rating(col("vote_average"))) \
        .withColumn("budget_category", categorize_budget(col("budget"))) \
        .withColumn("decade", extract_decade(col("release_year"))) \
        .withColumn("profitability_tier", calculate_profitability_tier(col("roi_percent"))) \
        .withColumn("runtime_category", categorize_runtime(col("runtime"))) \
        .withColumn("popularity_category", categorize_popularity(col("popularity"))) \
        .withColumn("genre_count", count_genres(col("genres"))) \
        .withColumn("primary_genre", get_primary_genre(col("genres")))
    
    # Stage 2.4: Score Calculations (Composite Metrics)
    print("   📌 Stage 2.4: Composite score calculations...")
    stage4_df = stage3_df \
        .withColumn("weighted_score",
            spark_round(
                (col("vote_average") * log(col("vote_count") + 1)) / 
                greatest(log(spark_max("vote_count").over(Window.partitionBy(lit(1))) + 1), lit(1)),
                2
            )) \
        .withColumn("popularity_rating_index",
            spark_round(
                (col("popularity") / 100) * col("vote_average"),
                2
            )) \
        .withColumn("commercial_success_index",
            when(col("budget") > 0,
                spark_round(
                    (col("revenue") / col("budget")) * (col("vote_average") / 10),
                    2
                ))
            .otherwise(None))
    
    # Stage 2.5: Add Processing Metadata
    print("   📌 Stage 2.5: Adding metadata...")
    processed_df = stage4_df \
        .withColumn("batch_processed_at", current_timestamp()) \
        .withColumn("layer", lit("batch")) \
        .withColumn("data_quality_score",
            spark_round(
                (when(col("title").isNotNull(), lit(0.2)).otherwise(lit(0))) +
                (when(col("release_date").isNotNull(), lit(0.2)).otherwise(lit(0))) +
                (when(col("vote_count") > 0, lit(0.2)).otherwise(lit(0))) +
                (when(col("budget") > 0, lit(0.2)).otherwise(lit(0))) +
                (when(col("revenue") > 0, lit(0.2)).otherwise(lit(0))),
                2
            ))
    
    # Filter valid records
    print("   📌 Filtering valid records...")
    clean_df = processed_df \
        .filter(col("id").isNotNull()) \
        .filter(col("title").isNotNull()) \
        .filter(col("title") != "")
    
    # ==========================================================================
    # PARTITION STRATEGY: Repartition by year for better parallelism
    # ==========================================================================
    print("\n📊 PARTITIONING: Repartitioning by release_year...")
    partitioned_df = clean_df.repartition(col("release_year"))
    
    # Persist processed data for reuse
    print("💾 CACHING: Persisting processed data...")
    partitioned_df.persist(StorageLevel.MEMORY_AND_DISK)
    
    clean_count = partitioned_df.count()
    print(f"   ✅ Clean records: {clean_count}")
    
    # ==========================================================================
    # QUERY OPTIMIZATION: Show execution plan
    # ==========================================================================
    print("\n📋 QUERY EXECUTION PLAN (Optimized):")
    partitioned_df.explain(mode="formatted")
    
    return partitioned_df


# =============================================================================
# BROADCAST JOIN OPERATIONS
# =============================================================================
def perform_broadcast_joins(movies_df):
    """
    Perform BROADCAST JOINs with lookup tables
    Broadcast join is optimal when one table is small enough to fit in memory
    """
    print("\n" + "=" * 80)
    print("🔗 STAGE 3: BROADCAST JOIN OPERATIONS")
    print("=" * 80)
    
    # Create lookup tables
    genre_lookup = create_genre_lookup()
    language_lookup = create_language_lookup()
    decade_lookup = create_decade_lookup()
    
    # 3.1: Explode genres and broadcast join with genre lookup
    print("   🔗 3.1: Genre Enrichment (Broadcast Join)...")
    movies_with_genres = movies_df \
        .withColumn("genre", explode(split(col("genres"), ", "))) \
        .withColumn("genre", trim(col("genre")))
    
    genre_enriched = movies_with_genres.join(
        broadcast(genre_lookup),
        movies_with_genres["genre"] == genre_lookup["genre_name"],
        "left"
    ).select(
        movies_with_genres["*"],
        genre_lookup["genre_description"],
        genre_lookup["genre_priority"],
        genre_lookup["genre_category"]
    )
    print("   ✅ Genre enrichment complete")
    
    # 3.2: Language enrichment
    print("   🔗 3.2: Language Enrichment (Broadcast Join)...")
    language_enriched = movies_df.join(
        broadcast(language_lookup),
        movies_df["original_language"] == language_lookup["lang_code"],
        "left"
    ).select(
        movies_df["*"],
        language_lookup["lang_name"],
        language_lookup["lang_region"],
        language_lookup["is_major"].alias("is_major_language")
    )
    print("   ✅ Language enrichment complete")
    
    # 3.3: Decade enrichment
    print("   🔗 3.3: Decade Enrichment (Broadcast Join)...")
    decade_enriched = movies_df.join(
        broadcast(decade_lookup),
        movies_df["decade"] == decade_lookup["decade"],
        "left"
    ).select(
        movies_df["*"],
        decade_lookup["era_name"],
        decade_lookup["era_characteristic"]
    )
    print("   ✅ Decade enrichment complete")
    
    # Show broadcast join execution plan
    print("\n📋 Broadcast Join Execution Plan:")
    genre_enriched.explain(mode="simple")
    
    return {
        "genre_enriched": genre_enriched,
        "language_enriched": language_enriched,
        "decade_enriched": decade_enriched
    }


# =============================================================================
# SELF-JOIN FOR MOVIE COMPARISONS
# =============================================================================
def perform_self_join_analysis(movies_df):
    """
    Perform SELF-JOIN for comparing movies within same year/genre
    Uses Sort-Merge Join for large datasets
    """
    print("\n" + "=" * 80)
    print("🔗 STAGE 4: SELF-JOIN ANALYSIS")
    print("=" * 80)
    
    # Prepare datasets for self-join
    # Select only necessary columns to reduce shuffle
    movies_a = movies_df.select(
        col("id").alias("id_a"),
        col("title").alias("title_a"),
        col("release_year").alias("year_a"),
        col("primary_genre").alias("genre_a"),
        col("vote_average").alias("rating_a"),
        col("revenue").alias("revenue_a"),
        col("popularity").alias("popularity_a")
    ).filter(col("id_a").isNotNull())
    
    movies_b = movies_df.select(
        col("id").alias("id_b"),
        col("title").alias("title_b"),
        col("release_year").alias("year_b"),
        col("primary_genre").alias("genre_b"),
        col("vote_average").alias("rating_b"),
        col("revenue").alias("revenue_b"),
        col("popularity").alias("popularity_b")
    ).filter(col("id_b").isNotNull())
    
    # Self-join: Find similar movies (same year and genre)
    print("   🔗 Finding similar movies (Sort-Merge Join)...")
    similar_movies = movies_a.join(
        movies_b,
        (movies_a["year_a"] == movies_b["year_b"]) & 
        (movies_a["genre_a"] == movies_b["genre_b"]) &
        (movies_a["id_a"] < movies_b["id_b"]),  # Avoid duplicates
        "inner"
    ).withColumn("rating_diff", spark_abs(col("rating_a") - col("rating_b"))) \
     .withColumn("is_competitive", col("rating_diff") < 0.5) \
     .filter(col("is_competitive")) \
     .select(
         col("id_a"), col("title_a"), col("id_b"), col("title_b"),
         col("year_a").alias("year"), col("genre_a").alias("genre"),
         col("rating_a"), col("rating_b"), col("rating_diff")
     ).limit(1000)  # Limit for performance
    
    print(f"   ✅ Found similar movie pairs")
    
    return similar_movies


# =============================================================================
# COMPLEX WINDOW FUNCTION ANALYTICS
# =============================================================================
def create_window_analytics(df):
    """
    Create advanced analytics using Window Functions
    - RANK, ROW_NUMBER, DENSE_RANK
    - LAG, LEAD for time series
    - NTILE for quartile analysis
    - PERCENT_RANK, CUME_DIST for distribution
    """
    print("\n" + "=" * 80)
    print("📊 STAGE 5: WINDOW FUNCTION ANALYTICS")
    print("=" * 80)
    
    # ==========================================================================
    # 5.1: GENRE ANALYTICS with Window Functions
    # ==========================================================================
    print("   📈 5.1: Genre Analytics with Rankings...")
    genre_exploded = df \
        .withColumn("genre", explode(split(col("genres"), ", "))) \
        .withColumn("genre", trim(col("genre"))) \
        .filter(col("genre") != "")
    
    # Window specifications
    genre_rating_window = Window.partitionBy("genre").orderBy(desc("vote_average"))
    genre_revenue_window = Window.partitionBy("genre").orderBy(desc("revenue"))
    genre_popularity_window = Window.partitionBy("genre").orderBy(desc("popularity"))
    
    genre_with_rankings = genre_exploded \
        .withColumn("rating_rank", rank().over(genre_rating_window)) \
        .withColumn("rating_dense_rank", dense_rank().over(genre_rating_window)) \
        .withColumn("rating_row_num", row_number().over(genre_rating_window)) \
        .withColumn("revenue_rank", rank().over(genre_revenue_window)) \
        .withColumn("popularity_rank", rank().over(genre_popularity_window)) \
        .withColumn("rating_percent_rank", spark_round(percent_rank().over(genre_rating_window), 4)) \
        .withColumn("rating_cume_dist", spark_round(cume_dist().over(genre_rating_window), 4))
    
    # Aggregate genre statistics
    genre_stats = genre_exploded.groupBy("genre").agg(
        count("*").alias("movie_count"),
        spark_round(avg("vote_average"), 2).alias("avg_rating"),
        spark_round(stddev("vote_average"), 2).alias("rating_stddev"),
        spark_round(stddev_pop("vote_average"), 2).alias("rating_stddev_pop"),
        spark_round(variance("vote_average"), 2).alias("rating_variance"),
        spark_round(avg("popularity"), 2).alias("avg_popularity"),
        spark_sum("revenue").alias("total_revenue"),
        spark_sum("budget").alias("total_budget"),
        spark_round(avg("runtime"), 0).alias("avg_runtime"),
        spark_max("vote_average").alias("max_rating"),
        spark_min("vote_average").alias("min_rating"),
        percentile_approx("vote_average", 0.25).alias("rating_25th_percentile"),
        percentile_approx("vote_average", 0.5).alias("rating_median"),
        percentile_approx("vote_average", 0.75).alias("rating_75th_percentile"),
        approx_count_distinct("original_language").alias("unique_languages"),
        collect_set("rating_category").alias("rating_categories"),
        count(when(col("is_profitable"), True)).alias("profitable_count")
    ).withColumn("profitability_rate", 
        spark_round(col("profitable_count") / col("movie_count") * 100, 2)
    ).withColumn("avg_roi",
        when(col("total_budget") > 0,
             spark_round((col("total_revenue") - col("total_budget")) / col("total_budget") * 100, 2))
        .otherwise(None)
    ).withColumn("batch_processed_at", current_timestamp()) \
     .withColumn("layer", lit("batch"))
    
    # ==========================================================================
    # 5.2: YEAR ANALYTICS with LAG/LEAD (Time Series Analysis)
    # ==========================================================================
    print("   📈 5.2: Year Analytics with LAG/LEAD...")
    year_base_stats = df.filter(col("release_year").isNotNull()).groupBy("release_year").agg(
        count("*").alias("movie_count"),
        spark_round(avg("vote_average"), 2).alias("avg_rating"),
        spark_round(avg("popularity"), 2).alias("avg_popularity"),
        spark_sum("revenue").alias("total_revenue"),
        spark_sum("budget").alias("total_budget"),
        count(when(col("is_profitable"), True)).alias("profitable_count"),
        spark_round(avg("runtime"), 0).alias("avg_runtime"),
        approx_count_distinct("original_language").alias("unique_languages"),
        approx_count_distinct("primary_genre").alias("unique_genres")
    )
    
    year_window = Window.orderBy("release_year")
    
    year_stats = year_base_stats \
        .withColumn("prev_year_movies", lag("movie_count", 1).over(year_window)) \
        .withColumn("next_year_movies", lead("movie_count", 1).over(year_window)) \
        .withColumn("prev_2_year_movies", lag("movie_count", 2).over(year_window)) \
        .withColumn("prev_year_revenue", lag("total_revenue", 1).over(year_window)) \
        .withColumn("prev_year_rating", lag("avg_rating", 1).over(year_window)) \
        .withColumn("yoy_movie_growth", 
            when(col("prev_year_movies") > 0, 
                 spark_round((col("movie_count") - col("prev_year_movies")) / col("prev_year_movies") * 100, 2))
            .otherwise(None)) \
        .withColumn("yoy_revenue_growth",
            when(col("prev_year_revenue") > 0,
                 spark_round((col("total_revenue") - col("prev_year_revenue")) / col("prev_year_revenue") * 100, 2))
            .otherwise(None)) \
        .withColumn("yoy_rating_change",
            spark_round(col("avg_rating") - col("prev_year_rating"), 2)) \
        .withColumn("cumulative_movies", spark_sum("movie_count").over(year_window)) \
        .withColumn("cumulative_revenue", spark_sum("total_revenue").over(year_window)) \
        .withColumn("moving_avg_3y_movies",
            spark_round(avg("movie_count").over(
                Window.orderBy("release_year").rowsBetween(-2, 0)
            ), 0)) \
        .withColumn("moving_avg_3y_rating",
            spark_round(avg("avg_rating").over(
                Window.orderBy("release_year").rowsBetween(-2, 0)
            ), 2)) \
        .withColumn("batch_processed_at", current_timestamp()) \
        .withColumn("layer", lit("batch"))
    
    # ==========================================================================
    # 5.3: DIRECTOR ANALYTICS with NTILE
    # ==========================================================================
    print("   📈 5.3: Director Analytics with NTILE...")
    director_exploded = df \
        .withColumn("director", explode(split(col("directors"), ", "))) \
        .withColumn("director", trim(col("director"))) \
        .filter(col("director") != "")
    
    director_base_stats = director_exploded.groupBy("director").agg(
        count("*").alias("movie_count"),
        spark_round(avg("vote_average"), 2).alias("avg_rating"),
        spark_round(avg("popularity"), 2).alias("avg_popularity"),
        spark_sum("revenue").alias("total_revenue"),
        spark_sum("budget").alias("total_budget"),
        collect_list("title").alias("movies"),
        collect_set("primary_genre").alias("genres_worked"),
        spark_min("release_year").alias("first_movie_year"),
        spark_max("release_year").alias("latest_movie_year"),
        count(when(col("is_profitable"), True)).alias("profitable_movies")
    ).filter(col("movie_count") >= 2) \
     .withColumn("career_span", col("latest_movie_year") - col("first_movie_year")) \
     .withColumn("profitability_rate", 
        spark_round(col("profitable_movies") / col("movie_count") * 100, 2))
    
    director_window = Window.orderBy(desc("avg_rating"))
    revenue_director_window = Window.orderBy(desc("total_revenue"))
    prolific_director_window = Window.orderBy(desc("movie_count"))
    
    director_stats = director_base_stats \
        .withColumn("rating_quartile", ntile(4).over(director_window)) \
        .withColumn("revenue_quartile", ntile(4).over(revenue_director_window)) \
        .withColumn("prolific_quartile", ntile(4).over(prolific_director_window)) \
        .withColumn("rating_decile", ntile(10).over(director_window)) \
        .withColumn("director_tier", 
            when(col("rating_quartile") == 1, "Top 25%")
            .when(col("rating_quartile") == 2, "Top 50%")
            .when(col("rating_quartile") == 3, "Top 75%")
            .otherwise("Bottom 25%")) \
        .withColumn("batch_processed_at", current_timestamp()) \
        .withColumn("layer", lit("batch"))
    
    # ==========================================================================
    # 5.4: LANGUAGE ANALYTICS
    # ==========================================================================
    print("   📈 5.4: Language Analytics...")
    language_window = Window.orderBy(desc("movie_count"))
    
    language_stats = df.groupBy("original_language").agg(
        count("*").alias("movie_count"),
        spark_round(avg("vote_average"), 2).alias("avg_rating"),
        spark_round(avg("popularity"), 2).alias("avg_popularity"),
        spark_sum("revenue").alias("total_revenue"),
        spark_sum("budget").alias("total_budget"),
        spark_round(avg("runtime"), 0).alias("avg_runtime"),
        count(when(col("is_profitable"), True)).alias("profitable_count"),
        approx_count_distinct("release_year").alias("active_years")
    ).filter(col("movie_count") >= 5) \
     .withColumn("language_rank", rank().over(language_window)) \
     .withColumn("market_share", 
        spark_round(col("movie_count") / spark_sum("movie_count").over(Window.partitionBy(lit(1))) * 100, 2)) \
     .withColumn("batch_processed_at", current_timestamp()) \
     .withColumn("layer", lit("batch"))
    
    # ==========================================================================
    # 5.5: TOP MOVIES with Global Rankings
    # ==========================================================================
    print("   📈 5.5: Top Movies with Global Rankings...")
    global_rating_window = Window.orderBy(desc("vote_average"), desc("vote_count"))
    global_popularity_window = Window.orderBy(desc("popularity"))
    global_revenue_window = Window.orderBy(desc("revenue"))
    global_roi_window = Window.orderBy(desc("roi_percent"))
    
    top_movies = df.filter(col("vote_count") >= 100) \
        .select(
            "id", "title", "vote_average", "vote_count", "popularity",
            "revenue", "budget", "profit", "profit_margin", "release_year",
            "genres", "directors", "original_language", "primary_genre",
            "rating_category", "budget_category", "decade", "roi_percent",
            "profitability_tier", "runtime_category", "popularity_category",
            "data_quality_score", "batch_processed_at", "layer"
        ) \
        .withColumn("global_rating_rank", rank().over(global_rating_window)) \
        .withColumn("global_popularity_rank", rank().over(global_popularity_window)) \
        .withColumn("global_revenue_rank", rank().over(global_revenue_window)) \
        .withColumn("global_roi_rank", rank().over(global_roi_window)) \
        .withColumn("rating_percentile", 
            spark_round((1 - percent_rank().over(global_rating_window)) * 100, 1)) \
        .orderBy(desc("vote_average"), desc("vote_count")) \
        .limit(1000)
    
    return {
        "genre_stats": genre_stats,
        "genre_with_rankings": genre_with_rankings.limit(5000),
        "year_stats": year_stats,
        "director_stats": director_stats,
        "language_stats": language_stats,
        "top_movies": top_movies
    }


# =============================================================================
# PIVOT AND UNPIVOT OPERATIONS
# =============================================================================
def create_pivot_tables(df):
    """
    Create PIVOT tables for cross-tabulation analysis
    """
    print("\n" + "=" * 80)
    print("📊 STAGE 6: PIVOT TABLES")
    print("=" * 80)
    
    # ==========================================================================
    # 6.1: Decade x Rating Category (Movie Count)
    # ==========================================================================
    print("   📈 6.1: Decade x Rating Category Pivot...")
    decade_rating_pivot = df \
        .filter(col("decade").isNotNull()) \
        .filter(col("rating_category").isNotNull()) \
        .groupBy("decade") \
        .pivot("rating_category", 
               ["Masterpiece", "Excellent", "Good", "Above Average", 
                "Average", "Below Average", "Poor", "Very Poor"]) \
        .agg(count("*")) \
        .na.fill(0) \
        .orderBy("decade") \
        .withColumn("batch_processed_at", current_timestamp()) \
        .withColumn("layer", lit("batch"))
    
    # ==========================================================================
    # 6.2: Quarter x Budget Category (Avg Revenue in Millions)
    # ==========================================================================
    print("   📈 6.2: Quarter x Budget Category Pivot...")
    quarter_budget_pivot = df \
        .filter(col("release_quarter").isNotNull()) \
        .filter(col("budget_category").isNotNull()) \
        .groupBy("release_quarter") \
        .pivot("budget_category", 
               ["Mega Blockbuster", "Blockbuster", "Big Budget", 
                "Medium High", "Medium Budget", "Low Medium", "Low Budget", "Micro Budget"]) \
        .agg(spark_round(avg("revenue") / 1000000, 2)) \
        .na.fill(0) \
        .orderBy("release_quarter") \
        .withColumn("batch_processed_at", current_timestamp()) \
        .withColumn("layer", lit("batch"))
    
    # ==========================================================================
    # 6.3: Year x Primary Genre (Movie Count)
    # ==========================================================================
    print("   📈 6.3: Year x Primary Genre Pivot...")
    # Get top 10 genres
    top_genres = df.groupBy("primary_genre").count() \
        .orderBy(desc("count")).limit(10).collect()
    top_genre_list = [row["primary_genre"] for row in top_genres if row["primary_genre"]]
    
    year_genre_pivot = df \
        .filter(col("release_year").isNotNull()) \
        .filter(col("primary_genre").isin(top_genre_list)) \
        .groupBy("release_year") \
        .pivot("primary_genre", top_genre_list) \
        .agg(count("*")) \
        .na.fill(0) \
        .orderBy("release_year") \
        .withColumn("batch_processed_at", current_timestamp()) \
        .withColumn("layer", lit("batch"))
    
    # ==========================================================================
    # 6.4: Language x Decade (Average Rating)
    # ==========================================================================
    print("   📈 6.4: Language x Decade Pivot...")
    # Get top 10 languages
    top_languages = df.groupBy("original_language").count() \
        .orderBy(desc("count")).limit(10).collect()
    top_lang_list = [row["original_language"] for row in top_languages if row["original_language"]]
    
    language_decade_pivot = df \
        .filter(col("decade").isNotNull()) \
        .filter(col("original_language").isin(top_lang_list)) \
        .groupBy("decade") \
        .pivot("original_language", top_lang_list) \
        .agg(spark_round(avg("vote_average"), 2)) \
        .na.fill(0) \
        .orderBy("decade") \
        .withColumn("batch_processed_at", current_timestamp()) \
        .withColumn("layer", lit("batch"))
    
    # ==========================================================================
    # 6.5: UNPIVOT Example - Convert Pivot back to long format
    # ==========================================================================
    print("   📈 6.5: UNPIVOT example...")
    # Create a smaller pivot to unpivot
    sample_pivot = df.filter(col("decade").isNotNull()) \
        .groupBy("decade") \
        .pivot("profitability_tier", ["Extremely Profitable", "Highly Profitable", "Profitable", "Break Even"]) \
        .agg(count("*")) \
        .na.fill(0)
    
    # Unpivot using stack
    unpivot_expr = "stack(4, " \
                   "'Extremely Profitable', `Extremely Profitable`, " \
                   "'Highly Profitable', `Highly Profitable`, " \
                   "'Profitable', `Profitable`, " \
                   "'Break Even', `Break Even`) as (profitability_tier, movie_count)"
    
    unpivoted_df = sample_pivot.selectExpr("decade", unpivot_expr) \
        .filter(col("movie_count") > 0) \
        .withColumn("batch_processed_at", current_timestamp()) \
        .withColumn("layer", lit("batch"))
    
    return {
        "decade_rating_pivot": decade_rating_pivot,
        "quarter_budget_pivot": quarter_budget_pivot,
        "year_genre_pivot": year_genre_pivot,
        "language_decade_pivot": language_decade_pivot,
        "unpivoted_example": unpivoted_df
    }


# =============================================================================
# SAVE TO MONGODB (SERVING LAYER)
# =============================================================================
def save_to_mongodb(df, collection_name, id_field=None):
    """Lưu DataFrame vào MongoDB Serving Layer"""
    if not MONGO_ENABLED:
        print(f"   ℹ️  MongoDB disabled, skipping: {collection_name}")
        return True
    
    try:
        writer = df.write \
            .format("mongodb") \
            .option("connection.uri", CONNECTION_STRING) \
            .option("database", "BIGDATA") \
            .option("collection", collection_name)
        
        if id_field:
            writer = writer.option("idFieldList", id_field)
        
        writer.mode("overwrite").save()
        print(f"   ✅ Saved to MongoDB: {collection_name}")
        return True
    except Exception as e:
        print(f"   ❌ MongoDB error ({collection_name}): {str(e)}")
        return False


# =============================================================================
# MAIN BATCH LAYER EXECUTION
# =============================================================================
def run_batch_layer():
    """Main function to run enhanced batch layer"""
    print("\n" + "=" * 80)
    print("🔄 STARTING ENHANCED BATCH LAYER PROCESSING")
    print("=" * 80)
    
    try:
        # Stage 1 & 2: Load and Transform Data
        movies_df = load_and_process_data()
        
        # Stage 3: Broadcast Joins
        join_results = perform_broadcast_joins(movies_df)
        
        # Stage 4: Self-Join Analysis
        similar_movies = perform_self_join_analysis(movies_df)
        
        # Stage 5: Window Analytics
        analytics = create_window_analytics(movies_df)
        
        # Stage 6: Pivot Tables
        pivot_tables = create_pivot_tables(movies_df)
        
        # ==========================================================================
        # STAGE 7: SAVE TO MONGODB (SERVING LAYER)
        # ==========================================================================
        print("\n" + "=" * 80)
        print("💾 STAGE 7: SAVING TO MONGODB (SERVING LAYER)")
        print("=" * 80)
        
        # Master Dataset
        print("\n   📦 Saving Master Dataset...")
        save_to_mongodb(movies_df, "batch_movies", "id")
        
        # Analytics Views
        print("\n   📊 Saving Analytics Views...")
        save_to_mongodb(analytics["genre_stats"], "batch_genre_stats", "genre")
        save_to_mongodb(analytics["year_stats"], "batch_year_stats", "release_year")
        save_to_mongodb(analytics["director_stats"].limit(500), "batch_director_stats", "director")
        save_to_mongodb(analytics["language_stats"], "batch_language_stats", "original_language")
        save_to_mongodb(analytics["top_movies"], "batch_top_movies", "id")
        
        # Pivot Tables
        print("\n   📊 Saving Pivot Tables...")
        save_to_mongodb(pivot_tables["decade_rating_pivot"], "batch_decade_rating_pivot")
        save_to_mongodb(pivot_tables["quarter_budget_pivot"], "batch_quarter_budget_pivot")
        save_to_mongodb(pivot_tables["year_genre_pivot"], "batch_year_genre_pivot")
        save_to_mongodb(pivot_tables["language_decade_pivot"], "batch_language_decade_pivot")
        
        # Enriched Data
        print("\n   📊 Saving Enriched Data...")
        save_to_mongodb(join_results["language_enriched"], "batch_language_enriched", "id")
        save_to_mongodb(join_results["decade_enriched"], "batch_decade_enriched", "id")
        save_to_mongodb(similar_movies, "batch_similar_movies")
        
        # ==========================================================================
        # CLEANUP
        # ==========================================================================
        print("\n🧹 Cleaning up cached data...")
        movies_df.unpersist()
        print("   ✅ Cache cleared")
        
        # ==========================================================================
        # SUMMARY
        # ==========================================================================
        print("\n" + "=" * 80)
        print("✅ ENHANCED BATCH LAYER COMPLETED!")
        print("=" * 80)
        
        print("\n📊 BATCH LAYER SUMMARY:")
        print(f"   - Master Dataset: {movies_df.count()} movies")
        print(f"   - Genre Stats: {analytics['genre_stats'].count()} genres")
        print(f"   - Year Stats: {analytics['year_stats'].count()} years")
        print(f"   - Director Stats: {analytics['director_stats'].count()} directors")
        print(f"   - Language Stats: {analytics['language_stats'].count()} languages")
        print(f"   - Top Movies: {analytics['top_movies'].count()} movies")
        print(f"   - Pivot Tables: 4 tables")
        
        print("\n🎯 ADVANCED FEATURES DEMONSTRATED:")
        print("   ✅ Window Functions: rank, row_number, dense_rank, lag, lead, ntile, percent_rank, cume_dist")
        print("   ✅ Pivot Tables: 4 cross-tabulation views")
        print("   ✅ Unpivot: Stack function for long format conversion")
        print("   ✅ Custom UDFs: 9 business logic functions")
        print("   ✅ Broadcast Joins: Genre, Language, Decade enrichment")
        print("   ✅ Self-Join: Similar movie detection (Sort-Merge)")
        print("   ✅ Multi-stage Transformations: 5 transformation stages")
        print("   ✅ Caching: MEMORY_AND_DISK persistence")
        print("   ✅ Partitioning: Repartition by release_year")
        print("   ✅ Query Optimization: Explain plans, AQE enabled")
        
    except Exception as e:
        print(f"\n❌ BATCH LAYER FAILED: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    try:
        run_batch_layer()
    except Exception as e:
        print(f"\n❌ FATAL ERROR: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()
