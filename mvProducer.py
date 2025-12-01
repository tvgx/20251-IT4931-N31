import requests
import json
from kafka import KafkaProducer

# --- Cấu hình TMDb ---
TMDB_API_KEY = "84ce6d76ae217b1b2136214640c07f43"
TMDB_BASE_URL = "https://api.themoviedb.org/3"
POPULAR_MOVIES_ENDPOINT = f"{TMDB_BASE_URL}/movie/popular"

# --- Cấu hình Kafka ---
KAFKA_BROKER = "localhost:9092"  # Địa chỉ của Kafka Broker
KAFKA_TOPIC = "tmdb_movies_raw"

def get_popular_movies(page=1):
    """Gửi yêu cầu đến TMDb API và trả về dữ liệu phim."""
    params = {
        'api_key': TMDB_API_KEY,
        'language': 'en-US',
        'page': page
    }
    response = requests.get(POPULAR_MOVIES_ENDPOINT, params=params)
    response.raise_for_status() # Báo lỗi nếu status code là lỗi
    return response.json()['results']

def produce_tmdb_data():
    """Thiết lập Producer và gửi dữ liệu phim vào Kafka."""
    
    # Khởi tạo Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        # Hàm serializing giá trị: chuyển dictionary/JSON thành bytes
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f"Bắt đầu trích xuất dữ liệu từ TMDb và gửi đến Kafka Topic: {KAFKA_TOPIC}")

    try:
        # Lấy dữ liệu 5 trang đầu tiên
        for page in range(1, 6):
            movies = get_popular_movies(page=page)
            print(f"--- Đã lấy {len(movies)} phim từ Trang {page} ---")

            for movie in movies:
                # Gửi thông điệp vào Kafka. Key là ID phim để đảm bảo thứ tự (tùy chọn)
                producer.send(
                    KAFKA_TOPIC, 
                    key=str(movie['id']).encode('utf-8'), 
                    value=movie
                )
                print(f"Đã gửi phim ID {movie['id']} - {movie['title']}")

        # Đảm bảo tất cả thông điệp đã được gửi đi
        producer.flush()
        print("Hoàn thành gửi dữ liệu TMDb vào Kafka.")

    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi gọi TMDb API: {e}")
    except Exception as e:
        print(f"Lỗi Kafka hoặc lỗi chung: {e}")

if __name__ == "__main__":
    produce_tmdb_data()