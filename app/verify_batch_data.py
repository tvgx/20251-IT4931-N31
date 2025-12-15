#!/usr/bin/env python3
"""
Verify Batch Data - Kiểm tra dữ liệu đã được load vào MongoDB và Elasticsearch
"""

import os
import sys
import json
from dotenv import load_dotenv

load_dotenv()

def check_mongodb():
    """Kiểm tra dữ liệu trong MongoDB"""
    try:
        from pymongo import MongoClient
        
        connection_string = os.environ.get("CONNECTION_STRING", "mongodb://mycluster-mongos.bigdata.svc.cluster.local:27017")
        
        print("\n📦 Kiểm tra MongoDB...")
        print(f"   Connection: {connection_string}")
        
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        db = client['BIGDATA']
        collection = db['batch_movie']
        
        # Đếm documents
        count = collection.count_documents({})
        print(f"   ✅ Documents in batch_movie: {count}")
        
        if count > 0:
            # Lấy mẫu
            sample = collection.find_one()
            print(f"\n   📄 Sample document:")
            print(f"      ID: {sample.get('id')}")
            print(f"      Title: {sample.get('title')}")
            print(f"      Vote Average: {sample.get('vote_average')}")
            print(f"      Release Year: {sample.get('release_year')}")
            
            # Thống kê
            top_movies = list(collection.find({}, {"title": 1, "vote_average": 1}).sort("vote_average", -1).limit(5))
            print(f"\n   🏆 Top 5 movies by vote average:")
            for i, movie in enumerate(top_movies, 1):
                print(f"      {i}. {movie.get('title')} ({movie.get('vote_average')})")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        return False

def check_elasticsearch():
    """Kiểm tra dữ liệu trong Elasticsearch"""
    try:
        from elasticsearch import Elasticsearch
        
        es_nodes = os.environ.get("ES_NODES", "elasticsearch-es-http.bigdata.svc.cluster.local")
        
        print("\n🔍 Kiểm tra Elasticsearch...")
        print(f"   Nodes: {es_nodes}:9200")
        
        es = Elasticsearch([f"http://{es_nodes}:9200"], timeout=10)
        
        # Kiểm tra kết nối
        if not es.ping():
            print(f"   ❌ Cannot connect to Elasticsearch")
            return False
        
        print(f"   ✅ Connected to Elasticsearch")
        
        # Kiểm tra index
        indices = es.cat.indices(format="json")
        batch_index = [idx for idx in indices if "batch" in idx['index']]
        
        if not batch_index:
            print(f"   ⚠️  No batch index found")
            return False
        
        # Đếm documents
        index_name = batch_index[0]['index']
        count = es.count(index=index_name)['count']
        print(f"   ✅ Documents in {index_name}: {count}")
        
        if count > 0:
            # Lấy mẫu
            results = es.search(index=index_name, size=1)
            if results['hits']['hits']:
                sample = results['hits']['hits'][0]['_source']
                print(f"\n   📄 Sample document:")
                print(f"      ID: {sample.get('id')}")
                print(f"      Title: {sample.get('title')}")
                print(f"      Vote Average: {sample.get('vote_average')}")
                print(f"      Release Date: {sample.get('release_date')}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        print(f"   💡 Tip: Thử chạy 'kubectl port-forward svc/elasticsearch-es-http 9200:9200 -n bigdata'")
        return False

def check_kafka():
    """Kiểm tra Kafka topic"""
    try:
        from kafka import KafkaConsumer
        from kafka.errors import KafkaError
        
        kafka_broker = os.environ.get("KAFKA_BROKER1", "kafka-cluster-kafka-bootstrap.bigdata.svc.cluster.local:9092")
        batch_topic = os.environ.get("BATCH_TOPIC", "batch-movies")
        
        print("\n☕ Kiểm tra Kafka...")
        print(f"   Broker: {kafka_broker}")
        print(f"   Topic: {batch_topic}")
        
        consumer = KafkaConsumer(
            batch_topic,
            bootstrap_servers=[kafka_broker],
            consumer_timeout_ms=5000,
            auto_offset_reset='earliest'
        )
        
        messages = list(consumer)
        print(f"   ✅ Messages in topic: {len(messages)}")
        
        if messages:
            print(f"\n   📄 Sample message:")
            sample_msg = json.loads(messages[0].value.decode('utf-8'))
            print(f"      ID: {sample_msg.get('id')}")
            print(f"      Title: {sample_msg.get('title')}")
        
        consumer.close()
        return True
        
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        return False

def main():
    print("=" * 50)
    print("🔍 Batch Data Verification")
    print("=" * 50)
    
    results = {
        "MongoDB": check_mongodb(),
        "Elasticsearch": check_elasticsearch(),
        "Kafka": check_kafka()
    }
    
    print("\n" + "=" * 50)
    print("📊 Summary:")
    print("=" * 50)
    
    for service, status in results.items():
        status_str = "✅ OK" if status else "❌ FAILED"
        print(f"{service:20} {status_str}")
    
    print("\n" + "=" * 50)
    
    if all(results.values()):
        print("✅ Tất cả dịch vụ hoạt động bình thường!")
        sys.exit(0)
    else:
        print("⚠️  Một số dịch vụ có lỗi, vui lòng kiểm tra logs")
        sys.exit(1)

if __name__ == "__main__":
    main()
