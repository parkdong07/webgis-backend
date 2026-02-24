# ใช้ Python เวอร์ชันที่มี GDAL ติดตั้งมาแล้ว (สำคัญมาก!)
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.6.3

# ติดตั้ง Python และ pip
RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*

# ตั้งค่า Workspace
WORKDIR /app

# Copy requirements และติดตั้ง
COPY requirements.txt .
# ลบ GDAL/Fiona ออกจาก requirements เพื่อใช้ตัวที่มากับ Base Image (ป้องกัน Error)
RUN sed -i '/GDAL/d' requirements.txt && \
    sed -i '/Fiona/d' requirements.txt && \
    pip3 install --no-cache-dir -r requirements.txt

# Copy โค้ดทั้งหมด
COPY . .

# เปิด Port 3000
EXPOSE 3000

# คำสั่งรัน Server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "3000"]