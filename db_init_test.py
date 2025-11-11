import psycopg2
from psycopg2 import sql
import os

# KONFIGURASI HOST LOKAL UNTUK INISIALISASI
HOST_CONFIG = {
    "host": "localhost",
    "port": "5433", 
    # GUNAKAN KREDENSIAL USER API
    "user": "datavista_api_user", 
    "password": "DatavistaAPI@2025", 
    "database": "postgres"
}

TARGET_DB = 'datavista_db'
TARGET_USER = 'datavista_api_user'
TARGET_PASSWORD = 'DatavistaAPI@2025'

def initialize_db():
    """Membuat database dan user jika belum ada."""
    conn = None
    try:
        # 1. Koneksi ke database default (postgres) sebagai superuser
        conn = psycopg2.connect(
            host=HOST_CONFIG['host'],
            port=HOST_CONFIG['port'],
            user=HOST_CONFIG['user'],
            password=HOST_CONFIG['password'],
            database=HOST_CONFIG['database']
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # 2. Cek dan Buat DATABASE (datavista_db)
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname='{TARGET_DB}'")
        if not cursor.fetchone():
            print(f"Database {TARGET_DB} not found. Creating...")
            cursor.execute(f"CREATE DATABASE {TARGET_DB}")
        else:
            print(f"Database {TARGET_DB} already exists.")

        # 3. Cek dan Buat/Update USER (datavista_api_user)
        cursor.execute(f"SELECT 1 FROM pg_roles WHERE rolname='{TARGET_USER}'")
        if not cursor.fetchone():
            print(f"User {TARGET_USER} not found. Creating...")
            cursor.execute(f"CREATE USER {TARGET_USER} WITH PASSWORD '{TARGET_PASSWORD}'")
        else:
            print(f"User {TARGET_USER} found. Ensuring correct password...")
            cursor.execute(f"ALTER USER {TARGET_USER} WITH PASSWORD '{TARGET_PASSWORD}'")

        # 4. Berikan Hak Akses ke Database Proyek
        cursor.execute(f"GRANT ALL PRIVILEGES ON DATABASE {TARGET_DB} TO {TARGET_USER}")
        print("Database initialization successful!")
        return True

    except psycopg2.Error as e:
        print(f"FATAL DB CONFIG ERROR: {e}")
        print("Please ensure PostgreSQL is running and port 5433 is open.")
        return False
    finally:
        if conn:
            conn.close()

if initialize_db():
    print("Initialization complete. Proceeding to final test.")

# NOTE: Anda juga harus menjalankan create_initial_tables(conn) di sini 
# untuk membuat skema log.
