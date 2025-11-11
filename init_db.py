import database
import psycopg2
import time
import os
import logging
import sys
import subprocess

# Impor model/session (tetap diperlukan agar Alembic dapat mendeteksi model)
try:
    import models.models
    from models.base import SessionLocal, engine
    from sqlalchemy import text
    import seed # Import the seed module (Asumsi ini berisi fungsi run_seed atau seed_initial_data)
except ImportError as e:
    logging.error(f"Error importing modules: {e}. Check your PYTHONPATH and dependencies.")
    sys.exit(1)


# --- Konfigurasi ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MAX_RETRIES = 20
RETRY_DELAY = 5 # seconds (Total wait: 100 seconds)

# Variabel Lingkungan untuk Kredensial Aplikasi
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres_db')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'datavista_db')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'datavista_api_user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'DatavistaAPI@2025')

# Kredensial Superuser (Harus Disediakan di Lingkungan Docker)
POSTGRES_ADMIN_USER = os.getenv('POSTGRES_ADMIN_USER', 'postgres') # Asumsi Superuser
POSTGRES_ADMIN_PASSWORD = os.getenv('POSTGRES_ADMIN_PASSWORD') # HARUS diset

if not POSTGRES_ADMIN_PASSWORD:
    logging.error("POSTGRES_ADMIN_PASSWORD must be set for database creation/destruction.")
    sys.exit(1)


# --- Fungsi Koneksi Cek (Menggunakan Kredensial Biasa) ---

def wait_for_db():
    logging.info("--- 1. Cek Konektivitas Server PostgreSQL ---")
    for i in range(MAX_RETRIES):
        try:
            logging.info(f"Attempting connection to server (attempt {i+1}/{MAX_RETRIES})...")
            # Coba konek ke DB 'postgres' default, menggunakan kredensial user biasa
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database="postgres",
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            conn.close()
            logging.info("Successfully connected to PostgreSQL server.")
            return True
        except psycopg2.OperationalError as e:
            logging.warning(f"Server connection failed: {e}. Retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)

    logging.error("Could not connect to PostgreSQL server after multiple retries. Exiting.")
    return False

# --- Fungsi Drop dan Recreate Database (MENGGUNAKAN SUPERUSER) ---

# def drop_and_recreate_db():
#     logging.info("--- 2. Drop dan Recreate Database Target ---")
#     try:
#         # ✅ PERBAIKAN: Koneksi menggunakan KREDENSIAL SUPERUSER
#         admin_conn = psycopg2.connect(
#             host=POSTGRES_HOST,
#             database="postgres",
#             user=POSTGRES_ADMIN_USER,
#             password=POSTGRES_ADMIN_PASSWORD
#         )
#         admin_conn.autocommit = True
#         admin_cursor = admin_conn.cursor()

#         # Terminate semua koneksi ke DB target
#         terminate_query = f"""
#         SELECT pg_terminate_backend(pg_stat_activity.pid)
#         FROM pg_stat_activity
#         WHERE pg_stat_activity.datname = '{POSTGRES_DB}'
#           AND pid <> pg_backend_pid();
#         """
#         admin_cursor.execute(terminate_query)
#         logging.info(f"Terminated active connections to database {POSTGRES_DB}.")

#         # Drop dan Recreate database
#         admin_cursor.execute(f"DROP DATABASE IF EXISTS {POSTGRES_DB};")
#         logging.info(f"Dropped database {POSTGRES_DB}.")
#         admin_cursor.execute(f"CREATE DATABASE {POSTGRES_DB} OWNER {POSTGRES_USER};") # Set OWNER ke pengguna aplikasi
#         logging.info(f"Created database {POSTGRES_DB} with owner {POSTGRES_USER}.")

#         admin_cursor.close()
#         admin_conn.close()
#         logging.info("Database dropped and recreated successfully.")
#         return True
#     except Exception as e:
#         # Menambahkan pesan untuk memandu user mengecek kredensial superuser jika gagal
#         logging.error(f"Error dropping/recreating database. CHECK POSTGRES_ADMIN_USER/PASSWORD: {e}", exc_info=True)
#         return False

# --- Fungsi Migrasi Alembic (Menggunakan Kredensial Biasa) ---

def run_alembic_migrations():
    logging.info("--- 3. Applying Alembic Migrations ---")
    try:
        # Gunakan subprocess.run untuk kontrol yang lebih baik
        result = subprocess.run(
            ["alembic", "-x", "log_config=true", "upgrade", "head"],
            capture_output=True,
            text=True,
            check=True # Akan raise exception jika return code bukan 0
        )
        logging.info("Alembic migrations applied successfully.")
        
        # Log output ke file
        with open("alembic_upgrade.log", "w") as f:
            f.write(result.stdout)
            f.write("\n--- STDERR ---\n")
            f.write(result.stderr)
        
        logging.info("Alembic output logged to alembic_upgrade.log.")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Alembic migration FAILED with error code {e.returncode}.")
        logging.error(f"STDOUT:\n{e.stdout}")
        logging.error(f"STDERR:\n{e.stderr}")
        return False

# --- Fungsi Seed Data ---
def seed_initial_data():
    logging.info("--- 4. Seeding Initial Data ---")
    try:
        session = SessionLocal()
        # Menggunakan nama fungsi yang generik, asumsikan seed.py punya fungsi ini
        # Jika fungsi Anda bernama 'run_seed', ganti baris di bawah ini.
        if hasattr(seed, 'seed_initial_data'):
             seed.seed_initial_data(session)
        elif hasattr(seed, 'run_seed'):
             seed.run_seed(session)
        else:
             logging.warning("Seed module does not contain 'seed_initial_data' or 'run_seed' function.")
             return True # Tidak error jika seeding di skip
             
        session.close()
        logging.info("Database seeding complete.")
        return True
    except Exception as e:
        logging.error(f"Error during data seeding: {e}", exc_info=True)
        return False


# --- Main Execution ---

if __name__ == "__main__":
    if not wait_for_db():
        sys.exit(1)

    # if not drop_and_recreate_db():
    #     sys.exit(1)

    # if not run_alembic_migrations():
    #     sys.exit(1)

    # PENTING: Tunggu sebentar setelah migrasi, sebelum mencoba koneksi aplikasi biasa
    time.sleep(10) 

    if not seed_initial_data(): # Mengganti nama run_seed() ke seed_initial_data() untuk konsistensi
        sys.exit(1)


    logging.info("✅ Database Initialization Completed Successfully.")

    # Keep container alive
    try:
        os.system("tail -f /dev/null")
    except KeyboardInterrupt:
        logging.info("Exiting container.")
        sys.exit(0)