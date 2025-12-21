#!/usr/bin/env python3
"""
Data Ingestion Layer - Lambda Architecture
Thu thập dữ liệu từ TheMovieDB và lưu vào:
1. Kafka (Message Queue) - Dữ liệu "nóng" cho Speed Layer
2. MinIO (Object Storage) - Master Dataset cho Batch Layer

Kiến trúc:
- Data Source: Crawler từ themoviedb
- Data Ingestion: Kafka + MinIO
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, to_json, struct, 
    concat_ws, coalesce, when, year, to_date,
    explode, size, array_join
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, LongType, ArrayType, FloatType
)
from crawler import MovieDB
from schema import MOVIE_SCHEMA
import os
import sys
import time
import json
from datetime import datetime
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import io
import csv

load_dotenv()

# Environment variables
KAFKA_BROKER1 = os.environ.get("KAFKA_BROKER1", "localhost:9092")
MOVIE_TOPIC = os.environ.get("MOVIE_TOPIC", "movie")

# MinIO Configuration
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "password123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "datalake")
MINIO_ENABLED = os.environ.get("MINIO_ENABLED", "true").lower() == "true"

# Ingestion Configuration
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
YEARS_TO_FETCH = os.environ.get("YEARS_TO_FETCH", "2024,2023,2022,2021,2020")
CONTINUOUS_MODE = os.environ.get("CONTINUOUS_MODE", "true").lower() == "true"
FETCH_INTERVAL = int(os.environ.get("FETCH_INTERVAL", "30"))  # seconds between fetches

# Spark packages
packages = [
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1',
    'org.apache.hadoop:hadoop-aws:3.3.4',
    'com.amazonaws:aws-java-sdk-bundle:1.12.500'
]

print("=" * 70)
print("📥 DATA INGESTION LAYER - LAMBDA ARCHITECTURE")
print("=" * 70)
print(f"📡 Kafka: {KAFKA_BROKER1}")
print(f"📺 Topic: {MOVIE_TOPIC}")
print(f"🗄️  MinIO: {MINIO_ENDPOINT} ({'enabled' if MINIO_ENABLED else 'disabled'})")
if MINIO_ENABLED:
    print(f"   - Bucket: {MINIO_BUCKET}")
print(f"📦 Batch Size: {BATCH_SIZE}")
print(f"🔄 Continuous Mode: {CONTINUOUS_MODE}")
print("=" * 70)


class MinIOClient:
    """Client để lưu dữ liệu vào MinIO (Object Storage)"""
    
    def __init__(self, endpoint, access_key, secret_key, bucket):
        # Remove http:// or https:// from endpoint for boto3
        endpoint_url = endpoint
        if not endpoint.startswith('http'):
            endpoint_url = f"http://{endpoint}"
        
        self.s3 = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        self.bucket = bucket
        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self):
        """Tạo bucket nếu chưa tồn tại"""
        try:
            self.s3.head_bucket(Bucket=self.bucket)
            print(f"   ✅ Bucket '{self.bucket}' exists")
        except:
            try:
                self.s3.create_bucket(Bucket=self.bucket)
                print(f"   ✅ Created bucket '{self.bucket}'")
            except Exception as e:
                print(f"   ⚠️  Bucket creation error: {e}")
    
    def save_json(self, data, filename):
        """Lưu dữ liệu JSON vào MinIO"""
        try:
            json_str = json.dumps(data, indent=2, ensure_ascii=False)
            self.s3.put_object(
                Bucket=self.bucket,
                Key=filename,
                Body=json_str.encode('utf-8'),
                ContentType='application/json'
            )
            return True
        except Exception as e:
            print(f"   ❌ MinIO save error: {e}")
            return False
    
    def save_csv(self, data, filename):
        """Lưu dữ liệu CSV vào MinIO"""
        try:
            if not data:
                return False
            
            # Flatten nested structures
            flattened_data = []
            for movie in data:
                flat_movie = self._flatten_movie(movie)
                flattened_data.append(flat_movie)
            
            # Create CSV in memory
            output = io.StringIO()
            if flattened_data:
                writer = csv.DictWriter(output, fieldnames=flattened_data[0].keys())
                writer.writeheader()
                writer.writerows(flattened_data)
            
            csv_content = output.getvalue()
            
            self.s3.put_object(
                Bucket=self.bucket,
                Key=filename,
                Body=csv_content.encode('utf-8'),
                ContentType='text/csv'
            )
            return True
        except Exception as e:
            print(f"   ❌ MinIO CSV save error: {e}")
            return False
    
    def append_to_master_csv(self, data, filename="tmdb.csv"):
        """Append dữ liệu vào Master Dataset CSV"""
        try:
            if not data:
                return False
            
            # Check if file exists
            existing_data = []
            existing_ids = set()
            try:
                response = self.s3.get_object(Bucket=self.bucket, Key=filename)
                csv_content = response['Body'].read().decode('utf-8')
                reader = csv.DictReader(io.StringIO(csv_content))
                for row in reader:
                    existing_data.append(row)
                    existing_ids.add(row.get('id'))
            except:
                # File doesn't exist yet
                pass
            
            # Flatten and add new data (avoid duplicates)
            new_count = 0
            for movie in data:
                movie_id = str(movie.get('id', ''))
                if movie_id and movie_id not in existing_ids:
                    flat_movie = self._flatten_movie(movie)
                    existing_data.append(flat_movie)
                    existing_ids.add(movie_id)
                    new_count += 1
            
            if new_count == 0:
                print(f"   ℹ️  No new movies to add")
                return True
            
            # Write back to MinIO
            output = io.StringIO()
            if existing_data:
                fieldnames = list(existing_data[0].keys())
                writer = csv.DictWriter(output, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(existing_data)
            
            csv_content = output.getvalue()
            
            self.s3.put_object(
                Bucket=self.bucket,
                Key=filename,
                Body=csv_content.encode('utf-8'),
                ContentType='text/csv'
            )
            print(f"   ✅ Appended {new_count} movies to {filename}")
            return True
        except Exception as e:
            print(f"   ❌ MinIO append error: {e}")
            return False
    
    def _flatten_movie(self, movie):
        """Flatten nested movie structure for CSV storage"""
        flat = {
            'id': movie.get('id', ''),
            'title': movie.get('title', ''),
            'vote_average': movie.get('vote_average', 0),
            'vote_count': movie.get('vote_count', 0),
            'status': movie.get('status', ''),
            'release_date': movie.get('release_date', ''),
            'revenue': movie.get('revenue', 0),
            'runtime': movie.get('runtime', 0),
            'adult': movie.get('adult', False),
            'backdrop_path': movie.get('backdrop_path', ''),
            'budget': movie.get('budget', 0),
            'homepage': movie.get('homepage', ''),
            'tconst': movie.get('imdb_id', ''),
            'original_language': movie.get('original_language', ''),
            'original_title': movie.get('original_title', ''),
            'overview': movie.get('overview', ''),
            'popularity': movie.get('popularity', 0),
            'poster_path': movie.get('poster_path', ''),
            'tagline': movie.get('tagline', ''),
        }
        
        # Flatten genres
        genres = movie.get('genres', [])
        if genres:
            flat['genres'] = ', '.join([g.get('name', '') for g in genres if g])
        else:
            flat['genres'] = ''
        
        # Flatten production companies
        companies = movie.get('production_companies', [])
        if companies:
            flat['production_companies'] = ', '.join([c.get('name', '') for c in companies if c])
        else:
            flat['production_companies'] = ''
        
        # Flatten production countries
        countries = movie.get('production_countries', [])
        if countries:
            flat['production_countries'] = ', '.join([c.get('name', '') for c in countries if c])
        else:
            flat['production_countries'] = ''
        
        # Flatten spoken languages
        languages = movie.get('spoken_languages', [])
        if languages:
            flat['spoken_languages'] = ', '.join([l.get('english_name', l.get('name', '')) for l in languages if l])
        else:
            flat['spoken_languages'] = ''
        
        return flat


def create_spark_session():
    """Tạo Spark Session với cấu hình cho Kafka và MinIO"""
    driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", "1g")
    
    builder = SparkSession.builder \
        .appName("DataIngestionLayer-LambdaArchitecture") \
        .master(os.environ.get("MASTER", "local[*]")) \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint/ingestion") \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.executor.memory", driver_memory) \
        .config("spark.driver.maxResultSize", "512m")
    
    # MinIO/S3 configuration
    if MINIO_ENABLED:
        builder = builder \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def send_to_kafka(spark, movies):
    """Gửi dữ liệu movie vào Kafka topic"""
    if not movies:
        return 0
    
    try:
        df = spark.createDataFrame(movies, schema=MOVIE_SCHEMA)
        
        # Add ingestion metadata
        df_with_metadata = df \
            .withColumn("ingested_at", current_timestamp()) \
            .withColumn("source", lit("tmdb_api"))
        
        # Send to Kafka
        df_with_metadata \
            .selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER1) \
            .option("topic", MOVIE_TOPIC) \
            .mode("append") \
            .save()
        
        return df.count()
    except Exception as e:
        print(f"   ❌ Kafka error: {e}")
        return 0


def run_ingestion():
    """Main ingestion loop - Thu thập dữ liệu từ TMDB"""
    print("\n" + "=" * 70)
    print("🚀 STARTING DATA INGESTION")
    print("=" * 70)
    
    # Initialize Spark
    spark = create_spark_session()
    
    # Initialize MinIO client
    minio_client = None
    if MINIO_ENABLED:
        try:
            minio_client = MinIOClient(
                MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
                MINIO_ACCESS_KEY,
                MINIO_SECRET_KEY,
                MINIO_BUCKET
            )
            print("\n✅ MinIO client initialized")
        except Exception as e:
            print(f"\n⚠️  MinIO client error: {e}")
    
    # Initialize crawler
    movie_db = MovieDB(max_workers=4)
    print("✅ Movie crawler initialized")
    
    # Parse years to fetch
    years = [int(y.strip()) for y in YEARS_TO_FETCH.split(',') if y.strip()]
    if not years:
        years = list(range(datetime.now().year, 1974, -1))
    
    print(f"\n📅 Years to fetch: {years[:5]}... (total: {len(years)})")
    
    year_index = 0
    page_in_year = 1
    total_ingested = 0
    batch_count = 0
    
    while True:
        try:
            year = years[year_index % len(years)]
            
            print(f"\n📅 Year: {year} | Page: {page_in_year}")
            start_time = time.time()
            
            movies, total_pages = movie_db.get_movies_by_year(year=year, page=page_in_year)
            fetch_time = time.time() - start_time
            
            if not movies:
                print(f"   ✓ Year {year} complete (pages: {page_in_year-1})")
                year_index += 1
                page_in_year = 1
                
                if year_index >= len(years):
                    print(f"\n✓ All years fetched! Total: {total_ingested} movies")
                    if CONTINUOUS_MODE:
                        print(f"   Restarting in 60s...")
                        year_index = 0
                        time.sleep(60)
                    else:
                        break
                continue
            
            batch_count += 1
            
            # ==========================================================================
            # 1. SEND TO KAFKA (Hot Path - cho Speed Layer)
            # ==========================================================================
            kafka_count = send_to_kafka(spark, movies)
            if kafka_count > 0:
                print(f"   ✅ Kafka: Sent {kafka_count} movies")
            
            # ==========================================================================
            # 2. SAVE TO MINIO (Cold Path - cho Batch Layer)
            # ==========================================================================
            if minio_client:
                # Save raw JSON for archival
                json_filename = f"raw/movies/{year}/page_{page_in_year}.json"
                minio_client.save_json(movies, json_filename)
                
                # Append to master CSV dataset
                minio_client.append_to_master_csv(movies, "tmdb.csv")
            
            total_ingested += len(movies)
            print(f"   📊 Batch #{batch_count}: {len(movies)} movies in {fetch_time:.2f}s")
            print(f"   📈 Total ingested: {total_ingested}")
            
            # Navigate pages
            if page_in_year < min(total_pages, 500):  # TMDB limits to 500 pages
                page_in_year += 1
            else:
                year_index += 1
                page_in_year = 1
                
                if year_index >= len(years):
                    print(f"\n✓ All years fetched! Total: {total_ingested} movies")
                    if CONTINUOUS_MODE:
                        print(f"   Restarting in 60s...")
                        year_index = 0
                        time.sleep(60)
                    else:
                        break
            
            # Rate limiting
            time.sleep(FETCH_INTERVAL / 10)  # Smaller delay between pages
            
        except KeyboardInterrupt:
            print("\n\n⚠️  Interrupted by user")
            break
        except Exception as e:
            print(f"   ❌ Error: {e}")
            time.sleep(10)
    
    print("\n" + "=" * 70)
    print("📊 INGESTION SUMMARY")
    print("=" * 70)
    print(f"   Total movies ingested: {total_ingested}")
    print(f"   Total batches: {batch_count}")
    print(f"   Kafka topic: {MOVIE_TOPIC}")
    if MINIO_ENABLED:
        print(f"   MinIO bucket: {MINIO_BUCKET}")
    print("=" * 70)
    
    spark.stop()


if __name__ == "__main__":
    run_ingestion()
