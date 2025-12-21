FROM apache/spark:3.5.1

# Chuyển sang user root để có quyền cài đặt và tạo symbolic link
USER root

# Cập nhật hệ thống, cài đặt git và dọn dẹp cache
# Lưu ý: Các Image Spark cơ sở thường đã có Python 3
RUN apt-get update && \
    apt-get install -y git curl && \
    rm -rf /var/lib/apt/lists/*

# Tạo symbolic link "python" trỏ đến "python3"
# Điều này giúp Pod Kubernetes có thể tìm thấy lệnh "python"
RUN ln -s /usr/bin/python3 /usr/bin/python

# Cài các package cần cho dự án - Lambda Architecture
# Đảm bảo cài đặt bằng pip3 (hoặc pip) nhưng pip3 an toàn hơn
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    pymongo==4.6.1 \
    kafka-python==2.0.2 \
    python-dotenv==1.0.1 \
    requests==2.31.0 \
    flask==3.0.0 \
    flask-cors==4.0.0 \
    boto3==1.34.0 \
    tenacity==8.2.3

# Tạo thư mục cho dữ liệu batch
RUN mkdir -p /data


# Copy code Lambda Architecture
COPY app/ /opt/app/

WORKDIR /opt/app

# Environment variables cho Lambda Architecture
ENV PYSPARK_PYTHON=python3
ENV CSV_PATH=/data/tmdb.csv
ENV BATCH_INTERVAL_HOURS=24
ENV SERVING_PORT=5000

# Expose port cho Serving Layer API
EXPOSE 5000

# Quay lại user mặc định của Spark (hoặc tiếp tục là root, tùy thuộc vào nhu cầu bảo mật)
# Nếu không chỉ định, Spark sẽ chạy dưới user mặc định.
# USER spark

# Default command: Run Lambda Orchestrator
CMD ["python", "lambda_orchestrator.py", "--all"]