# Lambda Architecture - Movie Analytics

Dự án này triển khai hệ thống Lambda Architecture để phân tích dữ liệu phim, sử dụng Kafka, MongoDB, Spark, MinIO và Metabase.

## Kiến trúc hệ thống

- **Batch Layer**: Xử lý dữ liệu lịch sử với Spark
- **Speed Layer**: Xử lý dữ liệu real-time với Kafka và MongoDB
- **Serving Layer**: API phục vụ dữ liệu với Flask và MongoDB
- **Data Ingestion**: Crawl dữ liệu từ TMDB API và gửi vào Kafka

## Yêu cầu hệ thống

- Python 3.8+
- Docker Desktop
- Minikube hoặc Kubernetes cluster
- kubectl

## Cài đặt và chạy

### 1. Cài đặt dependencies

```bash
# Tạo virtual environment (khuyến nghị)
python -m venv venv
venv\Scripts\activate  # Trên Windows

# Cài đặt Python packages
pip install -r requirements.txt
```

### 2. Khởi động Kubernetes cluster và cài đặt các dịch vụ
  
```bash
# Khởi động Minikube (trên Windows)
minikube start --driver=docker --cpus=8 --memory=7168mb --disk-size=40gb
```
```bash
# Kiểm tra status
minikube status
kubectl cluster-info
```
```bash
#Cài Helm repo
helm repo add strimzi https://strimzi.io/charts/
helm repo add percona https://percona.github.io/percona-helm-charts/
helm repo add elastic https://helm.elastic.co
helm repo update
```
```bash
#Chạy trên một terminal khác và giữ nguyên terminal đó, chú ý thay bằng đường dẫn tới thư mục app của mình
minikube mount < Path-to-your-app >:/mounted_code
# Ví dụ: minikube mount E:\Bigdata\Test\IT4931-bigdata-Movie-processing\app:/mounted_code
```
```bash
# Bật environment để Docker CLI trỏ vào Docker engine của Minikube
minikube docker-env | Invoke-Expression
```
```bash
#Build image trực tiếp trong cluster 
minikube image build -t spark-bigdata:latest -f k8s/04-spark/Dockerfile .
```

### 3. Triển khai các dịch vụ trên Kubernetes
   Copy và chạy từng khối 
```bash
# Tạo namespace
kubectl apply -f k8s/00-namespace.yaml
```
```bash
# Kafka - Strimzi Operator
helm install kafka strimzi/strimzi-kafka-operator -n bigdata --version 0.49.1 --wait
kubectl apply -f k8s/01-kafka/kafka-cluster.yaml
kubectl wait kafka/kafka-cluster --for=condition=Ready --timeout=600s -n bigdata
```
```bash
# MongoDB - Percona Operator
helm install psmdb percona/psmdb-operator -n bigdata --wait
kubectl apply -f k8s/02-mongo/secret.yaml
kubectl apply -f k8s/02-mongo/psmdb-cluster.yaml
kubectl wait psmdb/mycluster --for=condition=Ready --timeout=900s -n bigdata
```
```bash
# Spark producers & consumers
kubectl apply -f k8s/04-spark/deployments/
```
```bash
# MinIO
kubectl apply -f k8s/05-minio/minio-standalone.yaml
```
```bash
# Metabase
kubectl apply -f k8s/06-metabase/metabase.yaml -n bigdata
```
```bash
#Enable sharding MongoDB 
kubectl exec -it mycluster-mongos-0 -n bigdata -- mongosh admin -u clusterAdmin -p 123456 --eval "sh.enableSharding('BIGDATA'); sh.shardCollection('BIGDATA.movie', { id: 'hashed' }); sh.shardCollection('BIGDATA.actor', { id: 'hashed' });"
```
```bash
kubectl exec -it mycluster-mongos-0 -n bigdata -- mongosh admin -u userAdmin -p 123456 --eval "db.createUser({ user: 'spark-user', pwd: 'spark-pass', roles: [{ role: 'readWrite', db: 'BIGDATA' }], mechanisms: ['SCRAM-SHA-1', 'SCRAM-SHA-256'] })"
```

### 4. Kiểm tra trạng thái các pods

```bash
kubectl get pods -n bigdata
kubectl get services -n bigdata
```

### 5. Chạy ứng dụng Python

Mở các terminal riêng biệt và chạy các component:

```bash
# Terminal 1: Data Ingestion Producer
kubectl port-forward svc/mycluster-mongos 27018:27017 -n bigdata
# Truy cập: mongodb://localhost:27018
```
```bash
# Terminal 2: MinIO Console
kubectl port-forward svc/minio 9001:9001 -n bigdata
# Truy cập: http://localhost:9001
```
```bash

# Terminal 3: Serving Layer API
kubectl port-forward svc/serving-layer 5000:5000 -n bigdata
# Truy cập: http://localhost:5000

```
```bash
# Terminal 4: Metabase Dashboard
kubectl port-forward svc/metabase 3000:3000 -n bigdata
# Truy cập: http://localhost:3000
```
```bash
# Terminal 5: Kafka (nếu cần)
kubectl port-forward svc/kafka-cluster-kafka-bootstrap 9092:9092 -n bigdata
```
```bash
#Truy cập Kibana
minikube service kibana-kb-http -n bigdata --url
#Lệnh trên sẽ in ra URL kiểu http://192.168.49.2:xxxxx → mở browser 
```

### 6. Truy cập các dịch vụ

- **Metabase**: `minikube service metabase -n bigdata`
- **MinIO Console**: `minikube service minio -n bigdata`
- **Kafka**: `kafka-cluster-kafka-bootstrap.bigdata.svc.cluster.local:9092`
- **MongoDB**: `mycluster-mongos.bigdata.svc.cluster.local:27017`
- **Kibana**: `minikube service kibana-kb-http -n bigdata --url`

## Cấu hình môi trường

Tạo file `.env` trong thư mục gốc:

```env
KAFKA_BROKER1=kafka-cluster-kafka-bootstrap.bigdata.svc.cluster.local:9092
MASTER=local[*]
CONNECTION_STRING=mongodb://mycluster-mongos.bigdata.svc.cluster.local:27017

MOVIE_TOPIC=movie
BATCH_TOPIC=batch-movies
CSV_PATH=/data/tmdb.csv
REPLICATION=3

BATCH_INTERVAL_HOURS=24
SERVING_PORT=5000
```

## Troubleshooting

### Kiểm tra logs

```bash
# Logs của một pod cụ thể
kubectl logs <pod-name> -n bigdata

# Logs của tất cả pods trong deployment
kubectl logs -l app=<app-label> -n bigdata
```

### Restart dịch vụ

```bash
kubectl rollout restart deployment <deployment-name> -n bigdata
```

### Xóa và tái tạo cluster

```bash
# Dùng trong trường hợp muốn giải phóng toàn bộ dung lượng được sử dụng
minikube stop
minikube delete --all --purge
wsl --shutdown
```

## Phát triển local

Để chạy local mà không dùng Kubernetes:

1. Cài đặt Kafka, MongoDB, MinIO local
2. Cập nhật biến môi trường trong `.env` thành localhost
3. Chạy các script Python trực tiếp

## Cấu trúc dự án

```
├── app/                    # Source code Python
│   ├── crawler.py         # Crawl dữ liệu từ TMDB
│   ├── movie_producer.py  # Producer gửi dữ liệu vào Kafka
│   ├── movie_consumer.py  # Consumer xử lý dữ liệu
│   ├── enhanced_batch_layer.py    # Batch processing với Spark
│   ├── enhanced_speed_layer.py    # Real-time processing
│   ├── enhanced_serving_layer.py  # API serving layer
│   └── schema.py          # Data schemas
├── k8s/                   # Kubernetes manifests
│   ├── 00-namespace.yaml
│   ├── 01-kafka/
│   ├── 02-mongo/
│   ├── 04-spark/
│   ├── 05-minio/
│   └── 06-metabase/
├── requirements.txt       # Python dependencies
└── README.md
```