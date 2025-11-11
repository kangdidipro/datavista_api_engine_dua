import os
import logging

# --- KONFIGURASI DOCKER INTERNAL (Sesuai docker-compose.yml) ---

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "datavista_postgres"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "user": os.getenv("POSTGRES_USER", "datavista_api_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "DatavistaAPI@2025"),
    "database": os.getenv("POSTGRES_DB", "datavista_db")
}
logging.warning(f"[DIAGNOSTIC] POSTGRES_CONFIG: {POSTGRES_CONFIG}")

REDIS_CONFIG = {
    # HOST: Gunakan nama service Docker untuk koneksi internal
    "host": os.getenv("REDIS_HOST", "datavista_redis"), 
    "port": os.getenv("REDIS_PORT", 6379),
    "db": 0,
}
logging.warning(f"[DIAGNOSTIC] REDIS_CONFIG: {REDIS_CONFIG}")

# --- KONFIGURASI HOST LOKAL UNTUK INISIALISASI (Akses via Port 5433) ---
HOST_INIT_CONFIG = {
    "host": "localhost",
    # PORT HOST yang terhubung ke Container PostgreSQL
    "port": "5433", 
    "user": "datavista_api_user", 
    "password": "DatavistaAPI@2025", 
    "database": "postgres" # Koneksi awal ke database default
}
