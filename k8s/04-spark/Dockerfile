FROM apache/spark:3.5.1

# Chuyển sang user root để có quyền cài đặt và tạo symbolic link
USER root

# Cập nhật hệ thống, cài đặt git và dọn dẹp cache
# Lưu ý: Các Image Spark cơ sở thường đã có Python 3
RUN apt-get update && \
    apt-get install -y git && \
    rm -rf /var/lib/apt/lists/*

# Tạo symbolic link "python" trỏ đến "python3"
# Điều này giúp Pod Kubernetes có thể tìm thấy lệnh "python"
RUN ln -s /usr/bin/python3 /usr/bin/python

# Cài các package cần cho dự án
# Đảm bảo cài đặt bằng pip3 (hoặc pip) nhưng pip3 an toàn hơn
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    pymongo==4.6.1 \
    kafka-python==2.0.2 \
    python-dotenv==1.0.1 \
    requests==2.31.0

# Copy code đã sửa env var
COPY app/ /opt/app/

WORKDIR /opt/app

# PYSPARK_PYTHON đã được xác định, nhưng nó không ảnh hưởng đến lệnh 'python' trong command
ENV PYSPARK_PYTHON=python3

# Quay lại user mặc định của Spark (hoặc tiếp tục là root, tùy thuộc vào nhu cầu bảo mật)
# Nếu không chỉ định, Spark sẽ chạy dưới user mặc định.
# USER spark