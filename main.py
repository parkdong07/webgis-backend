import os
import shutil
import tempfile
import zipfile
import json
from typing import List, Optional

import uvicorn
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import geopandas as gpd
from sqlalchemy import create_engine
import asyncpg
from dotenv import load_dotenv

# --- 1. CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv()

# ดึงค่าจาก Environment Variable (Render จะส่งมาให้)
RAW_DB_URL = os.environ.get("DATABASE_URL")

if RAW_DB_URL:
    # ✅ MODE: CLOUD (RENDER)
    # แก้ไข scheme จาก postgres:// เป็น postgresql:// เพื่อให้ SQLAlchemy และ asyncpg ทำงานได้
    if RAW_DB_URL.startswith("postgres://"):
        RAW_DB_URL = RAW_DB_URL.replace("postgres://", "postgresql://", 1)
    
    DATABASE_URL_SYNC = RAW_DB_URL
    print(f"🚀 MODE: Cloud Database Connected")
else:
    # 🏠 MODE: LOCAL (LOCALHOST)
    # ใช้ค่าในเครื่องคุณ (สำรองไว้กรณีไม่ได้รันบน Render)
    DATABASE_URL_SYNC = "postgresql://postgres:4721040073@localhost:5432/webgis_db"
    print("🏠 MODE: Local Database (Localhost)")

# --- 2. APP SETUP ---
app = FastAPI(title="WebGIS Backend")

# ตั้งค่า CORS ให้ Frontend คุยกับ Backend ได้ไม่ติดบล็อก
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# SQLAlchemy Engine (ใช้สำหรับการเขียนข้อมูล/Upload ผ่าน GeoPandas)
engine = create_engine(DATABASE_URL_SYNC)

# Helper: Async DB Connection (ใช้สำหรับการดึงข้อมูลมาแสดงผลบนแผนที่)
async def get_db_connection():
    try:
        # asyncpg ต้องการ URL ที่ขึ้นต้นด้วย postgresql:// เท่านั้น (ห้ามมี +asyncpg)
        return await asyncpg.connect(DATABASE_URL_SYNC)
    except Exception as e:
        print(f"❌ DB Connection Failed: {e}")
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

# --- 3. API ROUTES ---

@app.get("/api/test-db")
async def test_db():
    """เช็กว่าต่อ Database ติดไหม"""
    try:
        conn = await get_db_connection()
        version = await conn.fetchval("SELECT version()")
        await conn.close()
        return {"status": "success", "db_version": version}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

@app.get("/api/layers")
async def get_layers():
    """ดึงรายชื่อตารางที่มีข้อมูลแผนที่ (PostGIS)"""
    conn = await get_db_connection()
    try:
        query = "SELECT f_table_name, type FROM geometry_columns WHERE f_table_schema = 'public'"
        rows = await conn.fetch(query)
        return [{"name": row['f_table_name'], "type": row['type']} for row in rows]
    finally:
        await conn.close()

@app.get("/api/layers/{table}/geojson")
async def get_layer_geojson(table: str):
    """แปลงข้อมูลในตารางให้เป็น GeoJSON เพื่อแสดงบน Leaflet"""
    conn = await get_db_connection()
    try:
        query = f"""
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', COALESCE(json_agg(ST_AsGeoJSON(t.*)::json), '[]')
            ) FROM "{table}" AS t
        """
        result = await conn.fetchval(query)
        return json.loads(result) if result else {"type": "FeatureCollection", "features": []}
    finally:
        await conn.close()

@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    """รับไฟล์ Shapefile/Zip แล้วบันทึกลง Database"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        file_path = os.path.join(tmpdirname, file.filename)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        try:
            read_path = file_path
            if file.filename.endswith(".zip"):
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(tmpdirname)
                shp_files = [f for f in os.listdir(tmpdirname) if f.endswith(".shp")]
                read_path = os.path.join(tmpdirname, shp_files[0])

            gdf = gpd.read_file(read_path)
            table_name = os.path.splitext(file.filename)[0].replace(" ", "_").lower()
            if gdf.crs is not None:
                gdf = gdf.to_crs(epsg=4326)
                            
            gdf.to_postgis(name=table_name, con=engine, if_exists='replace', index=False)
            return {"message": f"Successfully imported: {table_name}"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

# --- 4. STATIC FILES & RUN ---
# รับใช้ไฟล์หน้าเว็บ index.html
if os.path.exists(os.path.join(BASE_DIR, "index.html")):
    app.mount("/", StaticFiles(directory=BASE_DIR, html=True), name="static")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=3000)
