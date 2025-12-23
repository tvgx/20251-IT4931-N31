#!/usr/bin/env python3
"""
Simple Data Ingestion Layer - Lambda Architecture (No Spark)
Thu thập dữ liệu từ TheMovieDB và lưu vào:
1. Kafka (Message Queue) - Dữ liệu "nóng" cho Speed Layer
2. MinIO (Object Storage) - Master Dataset cho Batch Layer

Version này không dùng Spark để tiết kiệm tài nguyên.
"""

from kafka import KafkaProducer
from crawler import MovieDB
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
MAX_PAGES_PER_YEAR = int(os.environ.get("MAX_PAGES_PER_YEAR", "100"))  # TMDB max is 500 pages - default 5 for testing

print("=" * 70)
print("📥 DATA INGESTION LAYER - LAMBDA ARCHITECTURE (Lightweight)")
print("=" * 70)
print(f"📡 Kafka: {KAFKA_BROKER1}")
print(f"📺 Topic: {MOVIE_TOPIC}")
print(f"🗄️  MinIO: {MINIO_ENDPOINT} ({'enabled' if MINIO_ENABLED else 'disabled'})")
if MINIO_ENABLED:
    print(f"   - Bucket: {MINIO_BUCKET}")
print(f"📦 Batch Size: {BATCH_SIZE}")
print(f"🔄 Continuous Mode: {CONTINUOUS_MODE}")
print(f"📄 Max Pages Per Year: {MAX_PAGES_PER_YEAR}")
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
    
    def append_to_master_csv(self, data, filename="raw/tmdb.csv"):
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
                print(f"   ℹ️  No new movies to add to CSV")
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
            print(f"   ✅ Appended {new_count} movies to {filename} (total: {len(existing_data)})")
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


class KafkaMovieProducer:
    """Kafka Producer để gửi dữ liệu movie"""
    
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(','),
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1,
            batch_size=16384,
            linger_ms=10,
            buffer_memory=33554432
        )
        print("✅ Kafka producer initialized")
    
    def send_movies(self, topic, movies):
        """Gửi danh sách movies vào Kafka"""
        sent_count = 0
        for movie in movies:
            try:
                # Add ingestion metadata
                movie['ingested_at'] = datetime.utcnow().isoformat()
                movie['source'] = 'tmdb_api'
                
                self.producer.send(
                    topic,
                    key=movie.get('id'),
                    value=movie
                )
                sent_count += 1
            except Exception as e:
                print(f"   ⚠️  Error sending movie {movie.get('id')}: {e}")
        
        self.producer.flush()
        return sent_count
    
    def close(self):
        self.producer.close()


def run_ingestion():
    """Main ingestion loop - Thu thập dữ liệu từ TMDB"""
    print("\n" + "=" * 70)
    print("🚀 STARTING DATA INGESTION")
    print("=" * 70)
    
    # Initialize Kafka producer
    try:
        kafka_producer = KafkaMovieProducer(KAFKA_BROKER1)
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        print("   Retrying in 10 seconds...")
        time.sleep(10)
        try:
            kafka_producer = KafkaMovieProducer(KAFKA_BROKER1)
        except Exception as e2:
            print(f"❌ Kafka connection failed again: {e2}")
            sys.exit(1)
    
    # Initialize MinIO client
    minio_client = None
    if MINIO_ENABLED:
        try:
            minio_client = MinIOClient(
                MINIO_ENDPOINT,
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
    print(f"\n📅 Years to fetch: {years}")
    
    # Statistics
    total_movies_kafka = 0
    total_movies_minio = 0
    iteration = 0
    
    while True:
        iteration += 1
        print(f"\n{'='*70}")
        print(f"🔄 ITERATION {iteration} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*70}")
        
        iteration_batch = []  # Batch for THIS iteration (all years)
        
        for year in years:
            print(f"\n📆 Processing year: {year}")
            year_batch = []  # Batch for THIS year
            
            # Fetch movies for this year
            page = 1
            max_pages = MAX_PAGES_PER_YEAR  # TMDB API allows max 500 pages
            
            while page <= max_pages:
                print(f"   📄 Year: {year} | Page: {page}")
                
                try:
                    # Dùng get_movies_by_year thay vì discover_movie_list
                    movies, total_pages = movie_db.get_movies_by_year(year, page)
                    
                    if not movies:
                        print(f"   ℹ️  No more movies for year {year}")
                        break
                    
                    print(f"   📦 Fetched {len(movies)} movies (Total pages: {total_pages})")
                    
                    # Movies đã có chi tiết từ get_movies_by_year
                    if movies:
                        # Send to Kafka
                        kafka_sent = kafka_producer.send_movies(MOVIE_TOPIC, movies)
                        total_movies_kafka += kafka_sent
                        print(f"   📡 Kafka: Sent {kafka_sent} movies")
                        
                        # Add to batches
                        year_batch.extend(movies)
                        iteration_batch.extend(movies)
                    
                    page += 1
                    
                    # Rate limiting
                    time.sleep(0.5)
                    
                except Exception as e:
                    print(f"   ❌ Error fetching movies: {e}")
                    import traceback
                    traceback.print_exc()
                    break
            
            # Save year batch to MinIO after completing the year
            if minio_client and year_batch:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                json_filename = f"raw/movies_{year}_{timestamp}.json"
                minio_client.save_json(year_batch, json_filename)
                print(f"\n💾 MinIO: Saved {len(year_batch)} movies from {year} to {json_filename}")
                
                # Append to master CSV
                minio_client.append_to_master_csv(year_batch)
                total_movies_minio += len(year_batch)
        
        # Print statistics
        print(f"\n📊 STATISTICS:")
        print(f"   - Total movies sent to Kafka: {total_movies_kafka}")
        if minio_client:
            print(f"   - Total movies saved to MinIO: {total_movies_minio}")
        
        if not CONTINUOUS_MODE:
            print(f"\n✅ Single iteration mode - Stopping")
            break
        
        # Wait before next iteration
        print(f"\n⏳ Waiting {FETCH_INTERVAL} seconds before next iteration...")
        time.sleep(FETCH_INTERVAL)
    
    # Cleanup
    kafka_producer.close()
    print("\n" + "=" * 70)
    print("✅ DATA INGESTION COMPLETED")
    print("=" * 70)


if __name__ == "__main__":
    try:
        run_ingestion()
    except KeyboardInterrupt:
        print("\n\n⚠️  Ingestion stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
