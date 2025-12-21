import requests
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-cluster-kafka-bootstrap.bigdata.svc.cluster.local:9092")
MONGO_HOST = os.getenv("MONGO_HOST", "mycluster-mongos.bigdata.svc.cluster.local:27017")

class MovieDB:
    def __init__(self, max_workers=5) -> None:
        self.url = "https://api.themoviedb.org/3"
        self.headers = {
            "accept": "application/json",
            "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiI3M2ZkOTg0MmMzMTg2ZDY2OWMxNmEwNmIzYTdjODA2OCIsInN1YiI6IjY1M2IyMzFlMTA5Y2QwMDEyY2ZlMWU2NCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.5nkctErbJj6FNm__3T8rX1iFXSFw5Qasd3JOXqGOKYU"
        }
        self.session = self._create_session()
        self.max_workers = max_workers
        self.genres_cache = None

    def _create_session(self):
        """Create session with connection pooling và retry strategy"""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        session.mount("https://", adapter)
        return session

    def get_actors(self, page=1):
        url_actors = self.url + f"/person/popular?page={page}"
        response = self.session.get(url_actors, headers=self.headers)
        return response.json()['results']

    def get_genres(self):
        """Cache genres để tránh multiple requests"""
        if self.genres_cache is not None:
            return self.genres_cache
        
        url_genres = self.url + f"/genre/movie/list?language=en"
        response = self.session.get(url_genres, headers=self.headers)
        self.genres_cache = response.json()["genres"]
        return self.genres_cache

    def _fetch_movie_detail(self, movie_id):
        """Fetch movie detail với retry logic"""
        url_detail = self.url + f"/movie/{movie_id}"
        query_detail = {"language": "en-US"}
        try:
            time.sleep(0.2)  # Rate limit: 0.2s delay between requests
            response = self.session.get(url_detail, headers=self.headers, params=query_detail, timeout=20)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error fetching movie {movie_id}: {e}")
            return None

    def get_movies(self, primary_release_date_gt="1975-04-30", page=1):
        """Fetch movies với parallel detail fetching"""
        url_movies = self.url + "/discover/movie?"
        querystring = {
            "include_adult": "true",
            "include_video": "false",
            "language": "en-US",
            "page": page,
            "primary_release_date.gte": primary_release_date_gt,
            "vote_count.gte": 0,
        }
        
        response = self.session.get(url_movies, headers=self.headers, params=querystring, timeout=10)
        movies_list = response.json()["results"]
        
        if not movies_list:
            return []
        
        # Parallel fetch movie details
        res = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_movie = {
                executor.submit(self._fetch_movie_detail, movie['id']): movie 
                for movie in movies_list
            }
            
            for future in as_completed(future_to_movie):
                try:
                    movie_detail = future.result()
                    if movie_detail:
                        res.append(movie_detail)
                except Exception as e:
                    print(f"Error: {e}")
        
        return res

    def get_movies_by_year(self, year, page=1):
        """Fetch movies for specific year (DIVIDE & CONQUER strategy)
        
        Vượt qua limit 10k phim bằng cách chia theo năm.
        Mỗi năm thường có < 10k phim nên sẽ không bị cắt.
        """
        url_movies = self.url + "/discover/movie?"
        
        # YYYY-01-01 đến YYYY-12-31
        date_gte = f"{year}-01-01"
        date_lte = f"{year}-12-31"
        
        querystring = {
            "include_adult": "true",
            "include_video": "false",
            "language": "en-US",
            "page": page,
            "primary_release_date.gte": date_gte,
            "primary_release_date.lte": date_lte,
            "vote_count.gte": 0,
            "sort_by": "popularity.desc"
        }
        
        try:
            response = self.session.get(url_movies, headers=self.headers, params=querystring, timeout=10)
            response.raise_for_status()
            response_json = response.json()
        except Exception as e:
            print(f"Error fetching year {year} page {page}: {e}")
            return [], 0
        
        movies_list = response_json.get("results", [])
        total_pages = response_json.get("total_pages", 1)
        
        if not movies_list:
            return [], total_pages
        
        # Parallel fetch movie details
        res = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_movie = {
                executor.submit(self._fetch_movie_detail, movie['id']): movie 
                for movie in movies_list
            }
            
            for future in as_completed(future_to_movie):
                try:
                    movie_detail = future.result()
                    if movie_detail:
                        res.append(movie_detail)
                except Exception as e:
                    print(f"Error fetching movie: {e}")
        
        return res, total_pages


if __name__ == "__main__":
    movies = MovieDB()
    mv_json = movies.get_movies(page=501)
    print(json.dumps( mv_json, indent=2))
    # for mv in mv_json:
    #     # print(mv.get('ratingsSummary'))
    #     print(json.dumps(mv, indent=2))