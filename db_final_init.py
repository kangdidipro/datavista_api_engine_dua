import sys
import time
from db_config import POSTGRES_CONFIG, REDIS_CONFIG, HOST_INIT_CONFIG


# Import logic dari file yang sudah ada
from database import get_db_connection, create_initial_tables 

# --- INISIALISASI ---
def run_db_initialization():
    # Loop ini akan memberi waktu bagi PostgreSQL Docker untuk start penuh
    print("Starting manual database schema initialization...")
    for i in range(1, 10): 
        try:
            with get_db_connection(config=HOST_INIT_CONFIG) as conn:
                create_initial_tables(conn)
                print("SUCCESS: Database schemas created/verified.")
                return
        except Exception as e:
            # Menangkap error jika service belum sepenuhnya siap
            print(f"Attempt {i}/9 failed. Retrying in 5s. Error: {e}")
            time.sleep(5)

    print("FATAL: Failed to initialize database after multiple attempts.")
    sys.exit(1)

if __name__ == '__main__':
    run_db_initialization()
