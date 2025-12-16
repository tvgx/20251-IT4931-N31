#!/usr/bin/env python3
"""
Verify Batch Data - Kiểm tra dữ liệu Batch Layer (Movies & Actors)
"""

import os
import sys
import json
from dotenv import load_dotenv

load_dotenv()

def check_mongodb():
    """Kiểm tra dữ liệu trong MongoDB (batch_movie & batch_actor)"""
    try:
        from pymongo import MongoClient
        
        connection_string = os.environ.get("CONNECTION_STRING", "mongodb://mycluster-mongos.bigdata.svc.cluster.local:27017")
        
        print("\n📦 Kiểm tra MongoDB...")
        print(f"   Connection: {connection_string}")
        
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        db = client['BIGDATA']
        
        # Check batch_movie
        movie_col = db['batch_movie']
        movie_count = movie_col.count_documents({})
        print(f"   ✅ batch_movie count: {movie_count}")
        
        # Check batch_actor
        actor_col = db['batch_actor']
        actor_count = actor_col.count_documents({})
        print(f"   ✅ batch_actor count: {actor_count}")
        
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
        batch_indices = [idx['index'] for idx in indices if "batch" in idx['index']]
        print(f"   Found indices: {batch_indices}")
        
        # Check batch-movie
        if 'batch-movie' in batch_indices:
            count = es.count(index='batch-movie')['count']
            print(f"   ✅ batch-movie count: {count}")
        else:
            print(f"   ⚠️  batch-movie index missing")

        # Check batch-actor
        if 'batch-actor' in batch_indices:
            count = es.count(index='batch-actor')['count']
            print(f"   ✅ batch-actor count: {count}")
        else:
            print(f"   ⚠️  batch-actor index missing")
            
        return True
        
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        return False

def main():
    print("=" * 50)
    print("🔍 Batch Data Verification")
    print("=" * 50)
    
    mongo_ok = check_mongodb()
    es_ok = check_elasticsearch()
    
    print("\n" + "=" * 50)
    if mongo_ok and es_ok:
        print("✅ Validation Completed (Check counts above)")
    else:
        print("⚠️  Validation found issues")

if __name__ == "__main__":
    main()
