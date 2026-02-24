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

# --- 1. CONFIGURATION (ตั้งค่าตรงนี้ให้ชัวร์ที่สุด) ---
# บังคับใช้ path ของไฟล์ปัจจุบัน เพื่อป้องกันปัญหาหา index.html ไม่เจอ
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# --- Config (ฉบับรองรับ Cloud Render) ---
# --- Config ---
load_dotenv()

# รับค่าจาก Cloud (Render)
# --- แก้ไขส่วน Config ใน main.py ---
import os
from dotenv import load_dotenv

load_dotenv()

# ดึงค่าจาก Environment Variable ของ Render
# ใช้ os.environ.get จะชัวร์กว่าในบางกรณีบน Cloud
db_url = os.environ.get("DATABASE_URL")

if db_url:
    # ✅ ถ้าเจอค่า (แสดงว่าอยู่บน Render)
    # แก้ไขรูปแบบ URL ให้ SQLAlchemy ใช้งานได้
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
    
    DATABASE_URL_SYNC = db_url
    DATABASE_URL_ASYNC = db_url.replace("postgresql://", "postgresql+asyncpg://")
    print("🚀 MODE: Cloud Database (Render)")
else:
    # 🏠 ถ้าไม่เจอค่า (แสดงว่ารันในเครื่องตัวเอง)
    # ใส่ค่า localhost เดิมของคุณ
    DATABASE_URL_SYNC = "postgresql://postgres:4721040073@localhost:5432/webgis_db"
    DATABASE_URL_ASYNC = "postgresql+asyncpg://postgres:4721040073@localhost:5432/webgis_db"
    print("🏠 MODE: Local Database (Localhost)")

# กรณี Cloud บางเจ้าให้มาเป็น URL ยาวๆ บรรทัดเดียว (DATABASE_URL)
DATABASE_URL_ENV = os.getenv("DATABASE_URL") 

if DATABASE_URL_ENV:
    # แก้ปัญหา URL ของ Render ที่ขึ้นต้นด้วย postgres:// ให้เป็น postgresql:// (เพื่อให้ SQLAlchemy อ่านออก)
    if DATABASE_URL_ENV.startswith("postgres://"):
        DATABASE_URL_ENV = DATABASE_URL_ENV.replace("postgres://", "postgresql://", 1)
    DATABASE_URL_SYNC = DATABASE_URL_ENV
    DATABASE_URL_ASYNC = DATABASE_URL_ENV.replace("postgresql://", "postgresql+asyncpg://")
else:
    # ถ้าไม่มี URL ยาวๆ (รันในเครื่อง) ให้ต่อเองเหมือนเดิม
    DATABASE_URL_ASYNC = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    DATABASE_URL_SYNC = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- 2. APP SETUP ---
app = FastAPI(title="WebGIS Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# SQLAlchemy Engine (Sync)
try:
    engine = create_engine(DATABASE_URL_SYNC)
except Exception as e:
    print(f"⚠️ Warning: SQLAlchemy Engine creation failed: {e}")

# Pydantic Models
class QueryRequest(BaseModel):
    sql: str

class BufferRequest(BaseModel):
    geojson: dict
    distance: float

class AddFieldRequest(BaseModel):
    fieldName: str
    fieldType: str

# Helper: Async DB Connection
async def get_db_connection():
    try:
        return await asyncpg.connect(user=DB_USER, password=DB_PASS, database=DB_NAME, host=DB_HOST, port=DB_PORT)
    except Exception as e:
        print(f"❌ DB Connection Failed: {e}")
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

# --- 3. DEBUG ROUTES (เพิ่มมาใหม่เพื่อเช็กสถานะ) ---
@app.on_event("startup")
async def startup_msg():
    print("\n" + "="*50)
    print(f"🚀 Server Started!")
    print(f"📂 Root Path: {BASE_DIR}")
    print(f"🔌 Connecting to DB at: {DB_HOST}:{DB_PORT} as user '{DB_USER}'")
    print("="*50 + "\n")

@app.get("/api/test-db")
async def test_db():
    """API สำหรับทดสอบการเชื่อมต่อ Database โดยเฉพาะ"""
    try:
        conn = await get_db_connection()
        version = await conn.fetchval("SELECT version()")
        postgis = await conn.fetchval("SELECT PostGIS_Version()")
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
        # เช็กว่ามีตาราง geometry_columns หรือไม่ (กัน error กรณี PostGIS ยังไม่สมบูรณ์)
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
        # Validate table name strictly to prevent injection
        valid = await conn.fetchval("SELECT to_regclass($1::text)", table)
        if not valid:
            raise HTTPException(status_code=404, detail="Layer not found")

        query = f"""
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', COALESCE(json_agg(ST_AsGeoJSON(t.*)::json), '[]')
            )
            FROM "{table}" AS t
        """
        result = await conn.fetchval(query)
        # ถ้า result เป็น None (ตารางว่าง) ให้ส่ง FeatureCollection เปล่าๆ กลับไป
        return json.loads(result) if result else {"type": "FeatureCollection", "features": []}
    finally:
        await conn.close()

@app.post("/api/upload")
async def upload_file(file: UploadFile = File(...)):
    # ใช้ BASE_DIR เพื่อความชัวร์เรื่อง path
    with tempfile.TemporaryDirectory() as tmpdirname:
        file_path = os.path.join(tmpdirname, file.filename)
        
        # Save uploaded file
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        try:
            read_path = file_path
            # Handle Zip
            if file.filename.endswith(".zip"):
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(tmpdirname)
                shp_files = [f for f in os.listdir(tmpdirname) if f.endswith(".shp")]
                if not shp_files:
                    raise HTTPException(400, "Zip file must contain a .shp file")
                read_path = os.path.join(tmpdirname, shp_files[0])

            # Read & Upload to DB
            gdf = gpd.read_file(read_path)
            table_name = os.path.splitext(file.filename)[0].replace(" ", "_").lower()
            
            if gdf.crs is not None:
                gdf = gdf.to_crs(epsg=4326)
            
            gdf.to_postgis(name=table_name, con=engine, if_exists='replace', index=False)
            return {"message": f"Successfully imported layer: {table_name}", "count": len(gdf)}

        except Exception as e:
            print(f"Upload Error: {e}")
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/query")
async def run_custom_query(req: QueryRequest):
    conn = await get_db_connection()
    try:
        # Wrap query for GeoJSON output if it's a SELECT
        if req.sql.strip().upper().startswith("SELECT"):
            wrapped_sql = f"""
                WITH q AS ({req.sql})
                SELECT json_build_object(
                    'type', 'FeatureCollection',
                    'features', COALESCE(json_agg(ST_AsGeoJSON(q.*)::json), '[]')
                ) FROM q
            """
            result = await conn.fetchval(wrapped_sql)
            return json.loads(result) if result else {"type": "FeatureCollection", "features": []}
        else:
            status = await conn.execute(req.sql)
            return {"status": status}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    finally:
        await conn.close()

@app.post("/api/buffer")
async def create_buffer(req: BufferRequest):
    conn = await get_db_connection()
    try:
        # Convert dict to JSON string explicitly
        geojson_str = json.dumps(req.geojson)
        
        query = """
        WITH input_geom AS (
            SELECT ST_SetSRID(ST_GeomFromGeoJSON($1), 4326) as geom
        ),
        utm_srid AS (
            SELECT CASE 
                WHEN ST_Y(ST_Centroid(geom)) > 0 THEN 32600 + floor((ST_X(ST_Centroid(geom)) + 186) / 6)::int
                ELSE 32700 + floor((ST_X(ST_Centroid(geom)) + 186) / 6)::int
            END as srid FROM input_geom
        )
        SELECT ST_AsGeoJSON(
            ST_Transform(
                ST_Buffer(
                    ST_Transform((SELECT geom FROM input_geom), (SELECT srid FROM utm_srid)), 
                    $2
                ), 
                4326
            )
        ) as result
        """
        result = await conn.fetchval(query, geojson_str, req.distance)
        return {"type": "Feature", "geometry": json.loads(result)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await conn.close()

@app.get("/api/export/{table}/shapefile")
def export_shapefile(table: str):
    try:
        sql = f'SELECT * FROM "{table}"'
        gdf = gpd.read_postgis(sql, con=engine, geom_col='geom')
        
        with tempfile.TemporaryDirectory() as tmpdirname:
            filename = f"{table}_shapefile"
            shp_path = os.path.join(tmpdirname, filename)
            gdf.to_file(shp_path, driver='ESRI Shapefile')
            
            zip_path = os.path.join(tmpdirname, filename)
            shutil.make_archive(zip_path, 'zip', tmpdirname, filename)
            
            return FileResponse(
                path=f"{zip_path}.zip", 
                filename=f"{filename}.zip", 
                media_type='application/zip'
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/layers/{table}/addfield")
async def add_field(table: str, req: AddFieldRequest):
    conn = await get_db_connection()
    try:
        type_map = {'text': 'VARCHAR(255)', 'float': 'FLOAT', 'int': 'INTEGER', 'date': 'DATE'}
        sql_type = type_map.get(req.fieldType, 'TEXT')
        await conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{req.fieldName}" {sql_type}')
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
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

# --- 5. STATIC FILES (ต้องอยู่ท้ายสุด) ---
# ตรวจสอบว่ามี index.html จริงไหม
if os.path.exists(os.path.join(BASE_DIR, "index.html")):
    app.mount("/", StaticFiles(directory=BASE_DIR, html=True), name="static")
else:
    print("⚠️ Warning: index.html not found in current directory!")

if __name__ == "__main__":

    uvicorn.run("main:app", host="0.0.0.0", port=3000, reload=True)


