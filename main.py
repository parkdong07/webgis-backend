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
from sqlalchemy import create_engine, text
import asyncpg
from dotenv import load_dotenv

# --- 1. CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv()

# ดึงค่า DATABASE_URL จาก Render
DATABASE_URL = os.environ.get("DATABASE_URL")

if DATABASE_URL:
    # ✅ MODE: CLOUD (RENDER)
    # แก้ไข postgres:// ให้เป็น postgresql://
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    
    DATABASE_URL_SYNC = DATABASE_URL
    DATABASE_URL_ASYNC = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")
    print("🚀 MODE: Cloud Database (Render)")
else:
    # 🏠 MODE: LOCAL (LOCALHOST)
    # ใส่รหัสผ่านและชื่อ DB ของเครื่องคุณที่นี่
    DB_USER = "postgres"
    DB_PASS = "4721040073"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    DB_NAME = "webgis_db"
    
    DATABASE_URL_SYNC = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    DATABASE_URL_ASYNC = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    print("🏠 MODE: Local Database (Localhost)")

# --- 2. APP SETUP ---
app = FastAPI(title="WebGIS Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# SQLAlchemy Engine สำหรับ GeoPandas
engine = create_engine(DATABASE_URL_SYNC)

# Pydantic Models
class QueryRequest(BaseModel):
    sql: str

class BufferRequest(BaseModel):
    geojson: dict
    distance: float

class AddFieldRequest(BaseModel):
    fieldName: str
    fieldType: str

# Helper: Async DB Connection (ใช้ DATABASE_URL_ASYNC ตัวเดียวจบ)
async def get_db_connection():
    try:
        # ใช้ SYNC URL (ที่ไม่มี +asyncpg) แต่ส่งให้ asyncpg.connect ทำงาน
        # มันจะเข้าใจได้ทันทีครับ
        return await asyncpg.connect(DATABASE_URL_SYNC) 
    except Exception as e:
        print(f"❌ DB Connection Failed: {e}")
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

# --- 3. DEBUG ROUTES ---
@app.get("/api/test-db")
async def test_db():
    try:
        conn = await get_db_connection()
        version = await conn.fetchval("SELECT version()")
        postgis = await conn.fetchval("SELECT PostGIS_Full_Version()")
        await conn.close()
        return {
            "status": "success", 
            "db_version": version, 
            "postgis_version": postgis,
            "message": "✅ เชื่อมต่อ Database สำเร็จ!"
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

# --- 4. MAIN API ROUTES ---

@app.get("/api/layers")
async def get_layers():
    conn = await get_db_connection()
    try:
        query = """
            SELECT f_table_name as table_name, type as geometry_type 
            FROM geometry_columns 
            WHERE f_table_schema = 'public'
        """
        rows = await conn.fetch(query)
        return [{"name": row['table_name'], "type": row['geometry_type']} for row in rows]
    finally:
        await conn.close()

@app.get("/api/layers/{table}/geojson")
async def get_layer_geojson(table: str):
    conn = await get_db_connection()
    try:
        query = f"""
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', COALESCE(json_agg(ST_AsGeoJSON(t.*)::json), '[]')
            )
            FROM "{table}" AS t
        """
        result = await conn.fetchval(query)
        return json.loads(result) if result else {"type": "FeatureCollection", "features": []}
    finally:
        await conn.close()

@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
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
                if not shp_files:
                    raise HTTPException(400, "Zip file must contain a .shp file")
                read_path = os.path.join(tmpdirname, shp_files[0])

            gdf = gpd.read_file(read_path)
            table_name = os.path.splitext(file.filename)[0].replace(" ", "_").lower()
            
            if gdf.crs is not None:
                gdf = gdf.to_crs(epsg=4326)
            
            gdf.to_postgis(name=table_name, con=engine, if_exists='replace', index=False)
            return {"message": f"Successfully imported layer: {table_name}", "count": len(gdf)}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/buffer")
async def create_buffer(req: BufferRequest):
    conn = await get_db_connection()
    try:
        geojson_str = json.dumps(req.geojson)
        query = """
        WITH input_geom AS (
            SELECT ST_SetSRID(ST_GeomFromGeoJSON($1), 4326) as geom
        )
        SELECT ST_AsGeoJSON(ST_Transform(ST_Buffer(ST_Transform((SELECT geom FROM input_geom), 3857), $2), 4326)) as result
        """
        result = await conn.fetchval(query, geojson_str, req.distance)
        return {"type": "Feature", "geometry": json.loads(result)}
    finally:
        await conn.close()

@app.delete("/api/layers/{table}")
async def delete_layer(table: str):
    conn = await get_db_connection()
    try:
        await conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        return {"status": "deleted"}
    finally:
        await conn.close()

# --- 5. STATIC FILES ---
if os.path.exists(os.path.join(BASE_DIR, "index.html")):
    app.mount("/", StaticFiles(directory=BASE_DIR, html=True), name="static")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=3000, reload=True)

