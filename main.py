from fastapi import FastAPI, HTTPException, status # <-- PASTI FastAPI ADA DI SINI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

# Muat variabel lingkungan dari .env file
load_dotenv()

# CRITICAL IMPORTS FOR DATABASE & REDIS CHECK
from db_config import POSTGRES_CONFIG, REDIS_CONFIG
import psycopg2
import redis 

# Import Router (Absolut - Sudah LULUS troubleshooting path)
from routers.import_router import router as import_router
from routers.video_router import router as video_router
from routers.anomaly_router import router as anomaly_router
from routers.export_router import router as export_router
from routers.summary_router import router as summary_router


# --- 1. INISIALISASI APLIKASI (DEKLARASI 'app' - CRITICAL FIX) ---
app = FastAPI(
    title="SOFTWARE DATAVISTA - Data API Engine",
    description="API Gateway for CSV Import and Video Job Queue Management.",
    version="1.0.0"
)

# --- 2. KONFIGURASI CORS ---
origins = [
    "http://localhost:8000",  # Laravel Development Host
    "http://localhost",
    "http://127.0.0.1",
    "http://api.domainanda.com", # Production domain
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- 3. HEALTH CHECK (Verifikasi Layanan) ---
@app.get("/", tags=["Health Check"])
async def root():
    """Endpoint dasar untuk memastikan service berjalan."""
    return {"service": app.title, "status": "Running"}

@app.get("/health", tags=["Health Check"])
async def check_database_status():
    """Cek koneksi ke PostgreSQL dan Redis (Menggunakan konfigurasi Docker)."""
    
    status_db = "FAILED"
    status_redis = "FAILED"
    
    # 1. Cek Koneksi PostgreSQL
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        conn.close()
        status_db = "OK"
    except psycopg2.Error:
        status_db = f"FAILED: Cannot connect to {POSTGRES_CONFIG['host']}"

    # 2. Cek Koneksi Redis
    try:
        r = redis.Redis(host=REDIS_CONFIG['host'], port=REDIS_CONFIG['port'])
        r.ping()
        status_redis = "OK"
    except Exception:
        status_redis = f"FAILED: Cannot connect to {REDIS_CONFIG['host']}"

    if status_db == "OK" and status_redis == "OK":
        status_code = status.HTTP_200_OK
    else:
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        
    return JSONResponse(
        status_code=status_code,
        content={
            "app_status": "Healthy" if status_code == status.HTTP_200_OK else "Degraded",
            "database": status_db,
            "redis_broker": status_redis
        }
    )


# --- 4. INTEGRASI ROUTERS UTAMA ---

app.include_router(import_router)
app.include_router(anomaly_router)
app.include_router(video_router)
app.include_router(export_router)
app.include_router(summary_router)
