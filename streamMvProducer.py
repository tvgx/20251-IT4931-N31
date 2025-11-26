import requests
import json
import time
import redis
from kafka import KafkaProducer

# --- Cấu hình TMDb ---
# LƯU Ý: Thay thế 'YOUR_API_KEY' bằng API Key thật của bạn
TMDB_API_KEY = "84ce6d76ae217b1b2136214640c07f43"
TMDB_BASE_URL = "https://api.themoviedb.org/3"
POPULAR_MOVIES_ENDPOINT = f"{TMDB_BASE_URL}/movie/popular"

# --- Cấu hình Kafka & Redis ---
KAFKA_BROKER = "localhost:9092" 
KAFKA_TOPIC = "tmdb_movies_raw"
REDIS_HOST = "localhost"  # Sử dụng localhost vì port 6379 đã được mapping từ Docker
REDIS_PORT = 6379
REDIS_KEY_SET = "processed_movie_ids" # Redis set để lưu ID phim đã xử lý
POLLING_INTERVAL_SECONDS = 600 # Polling mỗi 10 phút (600 giây)

def get_popular_movies(page=1):
    """Gửi yêu cầu đến TMDb API và trả về dữ liệu phim."""
    params = {
        'api_key': TMDB_API_KEY,
        'language': 'en-US',
        'page': page
    }
    try:
        response = requests.get(POPULAR_MOVIES_ENDPOINT, params=params, timeout=10)
        response.raise_for_status()
        return response.json().get('results', [])
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Lỗi khi gọi TMDb API: {e}")
        return []

def run_streaming_producer():
    """Thiết lập Producer và thực hiện Polling liên tục."""
    
    # 1. Khởi tạo Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # 2. Khởi tạo Redis Client
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        # Kiểm tra kết nối Redis
        r.ping()
        print(f"✅ Kết nối Redis thành công tại {REDIS_HOST}:{REDIS_PORT}")
    except Exception as e:
        print(f"❌ KHÔNG THỂ KẾT NỐI ĐẾN REDIS. Vui lòng kiểm tra container 'kafka_redis'. Lỗi: {e}")
        return # Dừng nếu không kết nối được Redis

    print(f"Bắt đầu chế độ Streaming (Polling mỗi {POLLING_INTERVAL_SECONDS}s) cho Topic: {KAFKA_TOPIC}")

    while True:
        try:
            newly_processed_count = 0
            
            # Lấy dữ liệu 5 trang đầu tiên (có thể tùy chỉnh)
            for page in range(1, 6):
                movies = get_popular_movies(page=page)
                
                for movie in movies:
                    movie_id = str(movie['id'])
                    
                    # 3. Kiểm tra trùng lặp bằng Redis (chỉ gửi phim MỚI hoặc đã bị xóa khỏi top)
                    # Nếu movie_id CHƯA có trong Redis Set
                    if not r.sismember(REDIS_KEY_SET, movie_id):
                        
                        # Gửi thông điệp vào Kafka
                        producer.send(
                            KAFKA_TOPIC, 
                            key=movie_id.encode('utf-8'), 
                            value=movie
                        )
                        
                        # Thêm ID vào Redis Set
                        r.sadd(REDIS_KEY_SET, movie_id)
                        
                        print(f"[NEW] -> Gửi phim ID {movie_id} - {movie['title']}")
                        newly_processed_count += 1
                    # else:
                        # print(f"[SKIP] Phim ID {movie_id} đã được xử lý.") # Tùy chọn in ra phim cũ
            
            producer.flush()
            print(f"--- HOÀN TẤT VÒNG LẶP. Đã gửi {newly_processed_count} bản ghi MỚI vào Kafka. ---")

        except Exception as e:
            print(f"❌ LỖI TRONG VÒNG LẶP CHÍNH: {e}")
        
        # Chờ đợi trước khi bắt đầu chu kỳ Polling tiếp theo
        print(f"Đang chờ {POLLING_INTERVAL_SECONDS} giây cho lần Polling tiếp theo...")
        time.sleep(POLLING_INTERVAL_SECONDS)

if __name__ == "__main__":
    run_streaming_producer()