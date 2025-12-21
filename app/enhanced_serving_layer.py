#!/usr/bin/env python3
"""
Enhanced Serving Layer - Lambda Architecture
Kết hợp kết quả từ Batch Layer và Speed Layer để phục vụ queries

=============================================================================
FEATURES:
=============================================================================
1. Merge Batch + Speed Views với ưu tiên dữ liệu real-time
2. RESTful API cho dashboard và applications
3. Caching với TTL cho performance
4. Aggregation endpoints cho analytics
5. Health check và monitoring
6. Pagination và filtering support
=============================================================================
"""

from pymongo import MongoClient
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from functools import wraps
from datetime import datetime, timedelta
import os
import sys
import json
import time
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# ENVIRONMENT CONFIGURATION
# =============================================================================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING", "mongodb://localhost:27017")
SERVING_PORT = int(os.environ.get("SERVING_PORT", "5000"))
CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", "60"))
DEBUG_MODE = os.environ.get("DEBUG_MODE", "false").lower() == "true"

print("=" * 80)
print("🎯 ENHANCED SERVING LAYER - LAMBDA ARCHITECTURE")
print("=" * 80)
print(f"📦 MongoDB: {CONNECTION_STRING}")
print(f"🌐 Serving Port: {SERVING_PORT}")
print(f"⏱️  Cache TTL: {CACHE_TTL_SECONDS}s")
print(f"🔧 Debug Mode: {DEBUG_MODE}")
print("=" * 80)

# =============================================================================
# FLASK APP INITIALIZATION
# =============================================================================
app = Flask(__name__)
CORS(app)  # Enable CORS for Metabase and other dashboards

# MongoDB connection
mongo_client = MongoClient(CONNECTION_STRING)
db = mongo_client['BIGDATA']

# Simple in-memory cache
cache = {}
cache_timestamps = {}


# =============================================================================
# CACHING DECORATOR
# =============================================================================
def cached(ttl_seconds=None):
    """Simple TTL cache decorator"""
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            ttl = ttl_seconds or CACHE_TTL_SECONDS
            cache_key = f"{f.__name__}:{str(args)}:{str(sorted(kwargs.items()))}"
            
            # Check cache
            if cache_key in cache:
                timestamp = cache_timestamps.get(cache_key, 0)
                if time.time() - timestamp < ttl:
                    return cache[cache_key]
            
            # Execute and cache
            result = f(*args, **kwargs)
            cache[cache_key] = result
            cache_timestamps[cache_key] = time.time()
            return result
        return wrapper
    return decorator


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def merge_batch_speed(batch_data, speed_data, key_field):
    """
    Merge data từ Batch Layer và Speed Layer
    Speed data có độ ưu tiên cao hơn cho các bản ghi mới hơn
    """
    merged = {}
    
    # Add batch data first
    for item in batch_data:
        key = str(item.get(key_field))
        if key:
            merged[key] = item.copy()
            merged[key]['source'] = 'batch'
            merged[key]['_id'] = str(item.get('_id', ''))
    
    # Override/add with speed data (more recent)
    for item in speed_data:
        key = str(item.get(key_field))
        if key:
            if key in merged:
                # Merge: keep batch data but update with speed data
                for k, v in item.items():
                    if v is not None:
                        merged[key][k] = v
                merged[key]['source'] = 'merged'
            else:
                merged[key] = item.copy()
                merged[key]['source'] = 'speed'
            merged[key]['_id'] = str(item.get('_id', ''))
    
    result = list(merged.values())
    # Remove MongoDB ObjectId
    for item in result:
        if '_id' in item:
            del item['_id']
    
    return result


def merge_aggregations(batch_agg, speed_agg, key_field, numeric_fields=None, weighted_fields=None):
    """
    Merge aggregations từ Batch và Speed layers
    - numeric_fields: Cộng dồn (sum)
    - weighted_fields: Tính trung bình có trọng số
    """
    numeric_fields = numeric_fields or []
    weighted_fields = weighted_fields or []
    merged = {}
    
    # Add batch aggregations
    for item in batch_agg:
        key = str(item.get(key_field))
        if key:
            merged[key] = item.copy()
            merged[key]['batch_count'] = item.get('movie_count', 0)
            merged[key]['_id'] = str(item.get('_id', ''))
    
    # Add speed aggregations (incremental updates)
    for item in speed_agg:
        key = str(item.get(key_field))
        if key:
            if key in merged:
                batch_count = merged[key].get('batch_count', 0)
                speed_count = item.get('movie_count', 0)
                
                # Combine counts
                merged[key]['movie_count'] = batch_count + speed_count
                
                # Weighted average for specified fields
                for field in weighted_fields:
                    if batch_count > 0 and speed_count > 0:
                        batch_val = merged[key].get(field, 0) or 0
                        speed_val = item.get(field, 0) or 0
                        total = batch_count + speed_count
                        merged[key][field] = round(
                            (batch_val * batch_count + speed_val * speed_count) / total, 2
                        )
                
                # Sum for numeric fields
                for field in numeric_fields:
                    if field in item:
                        current = merged[key].get(field, 0) or 0
                        new_val = item.get(field, 0) or 0
                        merged[key][field] = current + new_val
                
                merged[key]['source'] = 'merged'
                merged[key]['speed_count'] = speed_count
            else:
                merged[key] = item.copy()
                merged[key]['source'] = 'speed'
            merged[key]['_id'] = str(item.get('_id', ''))
    
    result = list(merged.values())
    # Remove MongoDB ObjectId and internal fields
    for item in result:
        if '_id' in item:
            del item['_id']
    
    return result


def format_response(data, meta=None):
    """Standard response format"""
    response = {
        "success": True,
        "data": data,
        "timestamp": datetime.now().isoformat(),
        "layer": "serving"
    }
    if meta:
        response["meta"] = meta
    return response


def paginate(data, page=1, per_page=20):
    """Paginate a list"""
    start = (page - 1) * per_page
    end = start + per_page
    return {
        "items": data[start:end],
        "total": len(data),
        "page": page,
        "per_page": per_page,
        "total_pages": (len(data) + per_page - 1) // per_page
    }


# =============================================================================
# API ENDPOINTS
# =============================================================================

@app.route('/')
def home():
    """API Home - Documentation"""
    return jsonify({
        "service": "Lambda Architecture - Enhanced Serving Layer",
        "version": "2.0",
        "architecture": {
            "batch_layer": "MinIO → Spark Batch → MongoDB (batch_* collections)",
            "speed_layer": "Kafka → Spark Streaming → MongoDB (speed_* collections)",
            "serving_layer": "MongoDB → REST API (merged views)"
        },
        "endpoints": {
            "movies": {
                "/api/movies": "GET - Get movies (merged batch + speed)",
                "/api/movies/search": "GET - Search movies (?q=query)",
                "/api/movies/<id>": "GET - Get movie by ID"
            },
            "statistics": {
                "/api/stats/genres": "GET - Genre statistics",
                "/api/stats/years": "GET - Year statistics with YoY analysis",
                "/api/stats/languages": "GET - Language statistics",
                "/api/stats/directors": "GET - Director statistics with rankings"
            },
            "rankings": {
                "/api/top/movies": "GET - Top rated movies",
                "/api/top/profitable": "GET - Most profitable movies",
                "/api/top/popular": "GET - Most popular movies"
            },
            "analytics": {
                "/api/analytics/decade-rating": "GET - Decade x Rating pivot",
                "/api/analytics/quarter-budget": "GET - Quarter x Budget pivot",
                "/api/analytics/year-genre": "GET - Year x Genre pivot",
                "/api/analytics/trends": "GET - Trend analysis"
            },
            "monitoring": {
                "/api/lambda/status": "GET - Lambda architecture status",
                "/api/health": "GET - Health check",
                "/api/metrics": "GET - System metrics"
            }
        },
        "query_params": {
            "limit": "Number of items per page (default: 20)",
            "page": "Page number for pagination (default: 1)",
            "sort_by": "Field to sort by",
            "order": "Sort order: asc/desc (default: desc)"
        }
    })


@app.route('/api/health')
def health():
    """Health check endpoint"""
    status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "components": {}
    }
    
    # Check MongoDB
    try:
        mongo_client.admin.command('ping')
        status["components"]["mongodb"] = {
            "status": "connected",
            "database": "BIGDATA"
        }
    except Exception as e:
        status["components"]["mongodb"] = {
            "status": "error",
            "error": str(e)
        }
        status["status"] = "degraded"
    
    return jsonify(status)


@app.route('/api/metrics')
def metrics():
    """System metrics endpoint"""
    try:
        collections_info = {}
        for name in db.list_collection_names():
            collections_info[name] = {
                "count": db[name].count_documents({})
            }
        
        return jsonify({
            "timestamp": datetime.now().isoformat(),
            "database": "BIGDATA",
            "collections": collections_info,
            "cache": {
                "items": len(cache),
                "ttl_seconds": CACHE_TTL_SECONDS
            }
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =============================================================================
# MOVIES ENDPOINTS
# =============================================================================

@app.route('/api/movies')
def get_movies():
    """
    Get movies merged from Batch and Speed layers
    Query params: limit, page, sort_by, order, year, genre, language
    """
    limit = int(request.args.get('limit', 20))
    page = int(request.args.get('page', 1))
    sort_by = request.args.get('sort_by', 'popularity')
    order = -1 if request.args.get('order', 'desc') == 'desc' else 1
    year = request.args.get('year')
    genre = request.args.get('genre')
    language = request.args.get('language')
    
    try:
        # Build filter
        query_filter = {}
        if year:
            query_filter['release_year'] = int(year)
        if genre:
            query_filter['genres'] = {'$regex': genre, '$options': 'i'}
        if language:
            query_filter['original_language'] = language
        
        # Get from batch layer
        batch_movies = list(db['batch_movies'].find(
            query_filter, {'_id': 0}
        ).sort(sort_by, order).limit(limit * 2))
        
        # Get from speed layer (recent data)
        speed_movies = list(db['speed_movies'].find(
            query_filter, {'_id': 0}
        ).sort('processed_at', -1).limit(limit))
        
        # Merge results
        merged = merge_batch_speed(batch_movies, speed_movies, 'id')
        
        # Sort merged results
        merged.sort(key=lambda x: x.get(sort_by, 0) or 0, reverse=(order == -1))
        
        # Paginate
        paginated = paginate(merged, page, limit)
        
        return jsonify(format_response(
            paginated["items"],
            {
                "pagination": {
                    "page": paginated["page"],
                    "per_page": paginated["per_page"],
                    "total": paginated["total"],
                    "total_pages": paginated["total_pages"]
                },
                "sources": {
                    "batch_count": len(batch_movies),
                    "speed_count": len(speed_movies)
                },
                "filters": {
                    "year": year,
                    "genre": genre,
                    "language": language
                }
            }
        ))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/movies/search')
def search_movies():
    """
    Search movies using text matching
    Query params: q (query), limit, fields
    """
    query = request.args.get('q', '')
    limit = int(request.args.get('limit', 20))
    
    if not query:
        return jsonify({"success": False, "error": "Query parameter 'q' is required"}), 400
    
    try:
        # Search in batch and speed collections
        search_filter = {"$or": [
            {"title": {"$regex": query, "$options": "i"}},
            {"overview": {"$regex": query, "$options": "i"}},
            {"genres": {"$regex": query, "$options": "i"}},
            {"directors": {"$regex": query, "$options": "i"}}
        ]}
        
        batch_movies = list(db['batch_movies'].find(
            search_filter, {'_id': 0}
        ).limit(limit))
        
        speed_movies = list(db['speed_movies'].find(
            {"$or": [
                {"title": {"$regex": query, "$options": "i"}},
                {"genres_flat": {"$regex": query, "$options": "i"}}
            ]}, {'_id': 0}
        ).limit(limit))
        
        # Merge results
        merged = merge_batch_speed(batch_movies, speed_movies, 'id')
        
        return jsonify(format_response(
            merged[:limit],
            {
                "query": query,
                "total_results": len(merged),
                "sources": {
                    "batch_hits": len(batch_movies),
                    "speed_hits": len(speed_movies)
                }
            }
        ))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/movies/<movie_id>')
def get_movie(movie_id):
    """Get single movie by ID (merged from batch + speed)"""
    try:
        # Try batch first
        batch_movie = db['batch_movies'].find_one({'id': int(movie_id)}, {'_id': 0})
        
        # Try speed layer
        speed_movie = db['speed_movies'].find_one({'id': movie_id}, {'_id': 0})
        
        if batch_movie and speed_movie:
            result = {**batch_movie, **speed_movie, 'source': 'merged'}
        elif speed_movie:
            result = {**speed_movie, 'source': 'speed'}
        elif batch_movie:
            result = {**batch_movie, 'source': 'batch'}
        else:
            return jsonify({"success": False, "error": "Movie not found"}), 404
        
        return jsonify(format_response(result))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# =============================================================================
# STATISTICS ENDPOINTS
# =============================================================================

@app.route('/api/stats/genres')
@cached(ttl_seconds=60)
def get_genre_stats():
    """Get genre statistics (merged batch + speed)"""
    try:
        batch_stats = list(db['batch_genre_stats'].find({}, {'_id': 0}))
        speed_stats = list(db['speed_genre_stats'].find({}, {'_id': 0}))
        
        merged = merge_aggregations(
            batch_stats, speed_stats, 'genre',
            numeric_fields=['total_revenue', 'total_budget', 'profitable_count'],
            weighted_fields=['avg_rating', 'avg_popularity']
        )
        
        # Sort by movie count
        merged.sort(key=lambda x: x.get('movie_count', 0), reverse=True)
        
        return jsonify(format_response(
            merged,
            {
                "total_genres": len(merged),
                "sources": {
                    "batch_genres": len(batch_stats),
                    "speed_genres": len(speed_stats)
                }
            }
        ))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/stats/years')
@cached(ttl_seconds=60)
def get_year_stats():
    """Get year statistics with YoY analysis (merged batch + speed)"""
    try:
        batch_stats = list(db['batch_year_stats'].find({}, {'_id': 0}))
        speed_stats = list(db['speed_year_stats'].find({}, {'_id': 0}))
        
        merged = merge_aggregations(
            batch_stats, speed_stats, 'release_year',
            numeric_fields=['total_revenue', 'total_budget', 'profitable_count'],
            weighted_fields=['avg_rating', 'avg_popularity']
        )
        
        # Sort by year (descending)
        merged.sort(key=lambda x: x.get('release_year', 0), reverse=True)
        
        return jsonify(format_response(
            merged,
            {
                "total_years": len(merged),
                "year_range": {
                    "min": min([x.get('release_year', 9999) for x in merged]) if merged else None,
                    "max": max([x.get('release_year', 0) for x in merged]) if merged else None
                },
                "sources": {
                    "batch_years": len(batch_stats),
                    "speed_years": len(speed_stats)
                }
            }
        ))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/stats/languages')
@cached(ttl_seconds=60)
def get_language_stats():
    """Get language statistics (merged batch + speed)"""
    try:
        batch_stats = list(db['batch_language_stats'].find({}, {'_id': 0}))
        speed_stats = list(db['speed_language_stats'].find({}, {'_id': 0}))
        
        merged = merge_aggregations(
            batch_stats, speed_stats, 'original_language',
            numeric_fields=['total_revenue'],
            weighted_fields=['avg_rating', 'avg_popularity']
        )
        
        # Sort by movie count
        merged.sort(key=lambda x: x.get('movie_count', 0), reverse=True)
        
        return jsonify(format_response(
            merged,
            {
                "total_languages": len(merged),
                "sources": {
                    "batch_languages": len(batch_stats),
                    "speed_languages": len(speed_stats)
                }
            }
        ))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/stats/directors')
@cached(ttl_seconds=120)
def get_director_stats():
    """Get director statistics with rankings from batch layer"""
    limit = int(request.args.get('limit', 50))
    tier = request.args.get('tier')  # Filter by tier
    
    try:
        query_filter = {}
        if tier:
            query_filter['director_tier'] = tier
        
        batch_stats = list(db['batch_director_stats'].find(
            query_filter, {'_id': 0}
        ).sort('movie_count', -1).limit(limit))
        
        return jsonify(format_response(
            batch_stats,
            {
                "total_directors": len(batch_stats),
                "note": "Director stats from batch layer (historical data)",
                "available_tiers": ["Top 25%", "Top 50%", "Top 75%", "Bottom 25%"]
            }
        ))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# =============================================================================
# RANKINGS ENDPOINTS
# =============================================================================

@app.route('/api/top/movies')
@cached(ttl_seconds=60)
def get_top_movies():
    """Get top movies (merged batch + speed)"""
    limit = int(request.args.get('limit', 20))
    sort_by = request.args.get('sort_by', 'vote_average')
    
    try:
        batch_top = list(db['batch_top_movies'].find(
            {}, {'_id': 0}
        ).sort(sort_by, -1).limit(limit))
        
        speed_top = list(db['speed_top_movies'].find(
            {}, {'_id': 0}
        ).sort('popularity', -1).limit(limit))
        
        merged = merge_batch_speed(batch_top, speed_top, 'id')
        merged.sort(key=lambda x: x.get(sort_by, 0) or 0, reverse=True)
        
        return jsonify(format_response(
            merged[:limit],
            {
                "sort_by": sort_by,
                "sources": {
                    "batch_count": len(batch_top),
                    "speed_count": len(speed_top)
                }
            }
        ))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/top/profitable')
@cached(ttl_seconds=120)
def get_top_profitable():
    """Get most profitable movies"""
    limit = int(request.args.get('limit', 20))
    
    try:
        # From batch layer (has profit calculations)
        profitable = list(db['batch_top_movies'].find(
            {'is_profitable': True}, {'_id': 0}
        ).sort('roi_percent', -1).limit(limit))
        
        return jsonify(format_response(
            profitable,
            {"note": "Sorted by ROI percentage"}
        ))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/top/popular')
@cached(ttl_seconds=60)
def get_top_popular():
    """Get most popular movies (real-time from speed layer)"""
    limit = int(request.args.get('limit', 20))
    
    try:
        # Prioritize speed layer for real-time popularity
        speed_popular = list(db['speed_top_movies'].find(
            {}, {'_id': 0}
        ).sort('popularity', -1).limit(limit))
        
        if len(speed_popular) < limit:
            batch_popular = list(db['batch_top_movies'].find(
                {}, {'_id': 0}
            ).sort('popularity', -1).limit(limit))
            
            merged = merge_batch_speed(batch_popular, speed_popular, 'id')
            merged.sort(key=lambda x: x.get('popularity', 0) or 0, reverse=True)
            result = merged[:limit]
        else:
            result = speed_popular
        
        return jsonify(format_response(
            result,
            {"source": "real-time (speed layer prioritized)"}
        ))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# =============================================================================
# ANALYTICS ENDPOINTS (PIVOT TABLES)
# =============================================================================

@app.route('/api/analytics/decade-rating')
@cached(ttl_seconds=300)
def get_decade_rating_pivot():
    """Get Decade x Rating Category pivot table"""
    try:
        pivot_data = list(db['batch_decade_rating_pivot'].find({}, {'_id': 0}))
        
        return jsonify(format_response(
            pivot_data,
            {
                "type": "pivot_table",
                "dimensions": ["decade", "rating_category"],
                "metric": "movie_count"
            }
        ))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/analytics/quarter-budget')
@cached(ttl_seconds=300)
def get_quarter_budget_pivot():
    """Get Quarter x Budget Category pivot table"""
    try:
        pivot_data = list(db['batch_quarter_budget_pivot'].find({}, {'_id': 0}))
        
        return jsonify(format_response(
            pivot_data,
            {
                "type": "pivot_table",
                "dimensions": ["release_quarter", "budget_category"],
                "metric": "avg_revenue_millions"
            }
        ))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/analytics/year-genre')
@cached(ttl_seconds=300)
def get_year_genre_pivot():
    """Get Year x Genre pivot table"""
    try:
        pivot_data = list(db['batch_year_genre_pivot'].find({}, {'_id': 0}))
        
        return jsonify(format_response(
            pivot_data,
            {
                "type": "pivot_table",
                "dimensions": ["release_year", "primary_genre"],
                "metric": "movie_count"
            }
        ))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/analytics/trends')
@cached(ttl_seconds=60)
def get_trends():
    """Get trend analysis from year statistics"""
    try:
        # Get year stats with YoY analysis
        year_stats = list(db['batch_year_stats'].find(
            {'yoy_movie_growth': {'$exists': True}}, 
            {'_id': 0}
        ).sort('release_year', -1).limit(20))
        
        # Calculate overall trends
        if year_stats:
            avg_growth = sum([x.get('yoy_movie_growth', 0) or 0 for x in year_stats]) / len(year_stats)
            avg_rating_change = sum([x.get('yoy_rating_change', 0) or 0 for x in year_stats]) / len(year_stats)
        else:
            avg_growth = 0
            avg_rating_change = 0
        
        return jsonify(format_response(
            {
                "year_trends": year_stats,
                "summary": {
                    "avg_yoy_growth": round(avg_growth, 2),
                    "avg_rating_change": round(avg_rating_change, 2),
                    "years_analyzed": len(year_stats)
                }
            }
        ))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# =============================================================================
# LAMBDA ARCHITECTURE STATUS
# =============================================================================

@app.route('/api/lambda/status')
def lambda_status():
    """Get Lambda Architecture status with detailed information"""
    try:
        status = {
            "architecture": "Lambda",
            "layers": {},
            "timestamp": datetime.now().isoformat()
        }
        
        # Batch Layer Status
        batch_movies_count = db['batch_movies'].count_documents({})
        batch_latest = db['batch_movies'].find_one(
            {}, sort=[('batch_processed_at', -1)]
        )
        
        batch_collections = [
            "batch_movies", "batch_genre_stats", "batch_year_stats",
            "batch_director_stats", "batch_language_stats", "batch_top_movies",
            "batch_decade_rating_pivot", "batch_quarter_budget_pivot",
            "batch_year_genre_pivot", "batch_language_decade_pivot"
        ]
        
        batch_stats = {}
        for coll in batch_collections:
            if coll in db.list_collection_names():
                batch_stats[coll] = db[coll].count_documents({})
        
        status["layers"]["batch"] = {
            "status": "active" if batch_movies_count > 0 else "empty",
            "movie_count": batch_movies_count,
            "last_processed": str(batch_latest.get('batch_processed_at')) if batch_latest else None,
            "collections": batch_stats,
            "data_source": "MinIO (Master Dataset)"
        }
        
        # Speed Layer Status
        speed_movies_count = db['speed_movies'].count_documents({})
        speed_latest = db['speed_movies'].find_one(
            {}, sort=[('processed_at', -1)]
        )
        
        speed_collections = [
            "speed_movies", "speed_genre_stats", "speed_year_stats",
            "speed_language_stats", "speed_top_movies",
            "speed_popularity_distribution", "speed_rating_distribution"
        ]
        
        speed_stats = {}
        for coll in speed_collections:
            if coll in db.list_collection_names():
                speed_stats[coll] = db[coll].count_documents({})
        
        # Get latest epoch
        latest_epoch = None
        if speed_latest:
            latest_epoch = speed_latest.get('epoch_id')
        
        status["layers"]["speed"] = {
            "status": "active" if speed_movies_count > 0 else "waiting",
            "movie_count": speed_movies_count,
            "last_processed": str(speed_latest.get('processed_at')) if speed_latest else None,
            "latest_epoch": latest_epoch,
            "collections": speed_stats,
            "data_source": "Kafka (Real-time Stream)"
        }
        
        # Serving Layer Status
        total_collections = len(db.list_collection_names())
        
        status["layers"]["serving"] = {
            "status": "active",
            "mode": "merge_batch_speed",
            "total_collections": total_collections,
            "cache_enabled": True,
            "cache_ttl_seconds": CACHE_TTL_SECONDS,
            "endpoints": {
                "movies": 3,
                "statistics": 4,
                "rankings": 3,
                "analytics": 4,
                "monitoring": 3
            }
        }
        
        return jsonify(status)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/lambda/collections')
def list_collections():
    """List all collections in the database"""
    try:
        collections = {}
        for name in db.list_collection_names():
            collections[name] = {
                "count": db[name].count_documents({}),
                "layer": "batch" if name.startswith("batch_") else "speed" if name.startswith("speed_") else "other"
            }
        
        return jsonify(format_response(collections))
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# =============================================================================
# METABASE-FRIENDLY ENDPOINTS (Raw JSON for visualization)
# =============================================================================

@app.route('/api/raw/genres')
def raw_genres():
    """Raw genre data for Metabase"""
    try:
        data = list(db['batch_genre_stats'].find({}, {'_id': 0}))
        return Response(
            json.dumps(data),
            mimetype='application/json'
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/raw/years')
def raw_years():
    """Raw year data for Metabase"""
    try:
        data = list(db['batch_year_stats'].find({}, {'_id': 0}).sort('release_year', 1))
        return Response(
            json.dumps(data),
            mimetype='application/json'
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/raw/movies')
def raw_movies():
    """Raw movies data for Metabase (limited)"""
    limit = int(request.args.get('limit', 1000))
    try:
        data = list(db['batch_movies'].find({}, {'_id': 0}).limit(limit))
        return Response(
            json.dumps(data),
            mimetype='application/json'
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =============================================================================
# ERROR HANDLERS
# =============================================================================

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "success": False,
        "error": "Endpoint not found",
        "message": "Please check the API documentation at /"
    }), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        "success": False,
        "error": "Internal server error",
        "message": str(error)
    }), 500


# =============================================================================
# MAIN
# =============================================================================

def run_serving_layer():
    """Main function to run serving layer"""
    print("\n" + "=" * 80)
    print("🚀 STARTING ENHANCED SERVING LAYER")
    print("=" * 80)
    print(f"\n🌐 API available at: http://0.0.0.0:{SERVING_PORT}")
    print("\n📚 API Documentation: http://0.0.0.0:{}/".format(SERVING_PORT))
    print("\n" + "=" * 80)
    
    app.run(host='0.0.0.0', port=SERVING_PORT, debug=DEBUG_MODE, threaded=True)


if __name__ == "__main__":
    run_serving_layer()
