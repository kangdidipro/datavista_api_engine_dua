import sys
import time
from database import get_db_connection, create_initial_tables 

# KONFIGURASI HOST LOKAL UNTUK INISIALISASI (Akses via Port 5433)
HOST_INIT_CONFIG = {
    "host": "localhost",
    "port": "5433", 
    "user": "datavista_api_user", 
    "password": "DatavistaAPI@2025", 
    "database": "datavista_db" 
}

def run_schema_creation():
    """Menjalankan pembuatan tabel dengan retry logic yang aman."""
    print("Starting schema creation...")

    try:
        # Panggil get_db_connection dengan config host yang benar
        with get_db_connection(config=HOST_INIT_CONFIG) as conn: 
            create_initial_tables(conn)
            print("SUCCESS: Database schemas created/verified.")
    except Exception as e:
        print(f"FATAL SQL ERROR: {e}")
        sys.exit(1)

if __name__ == '__main__':
    run_schema_creation()
