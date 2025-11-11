import logging
from db_config import POSTGRES_CONFIG
from contextlib import contextmanager
from typing import List, Tuple
from sqlalchemy import create_engine, URL
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from fastapi import HTTPException 
import psycopg2
from psycopg2 import sql, extras
import pandas as pd
import sys

# --- SQLAlchemy Setup ---
SQLALCHEMY_DATABASE_URL = URL.create(
    "postgresql",
    username=POSTGRES_CONFIG['user'],
    password=POSTGRES_CONFIG['password'],
    host=POSTGRES_CONFIG['host'],
    port=POSTGRES_CONFIG['port'],
    database=POSTGRES_CONFIG['database']
)
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- KONFIGURASI NAMA TABEL ---
TRANSACTION_TABLE = "csv_import_log"
SUMMARY_TABLE = "csv_summary_master_daily"

# --- 1. KONEKSI UTILITY (Context Manager) ---

@contextmanager
def get_db_connection(config=None): # <--- TAMBAHKAN ARGUMEN INI
    """
    Menyediakan koneksi Psycopg2 menggunakan context manager.
    Koneksi akan ditutup secara otomatis saat keluar dari blok 'with'.
    """
    conn = None

    # Koreksi: Set conn_config secara eksplisit di awal scope
    conn_config = config if config else POSTGRES_CONFIG

    # DIAGNOSTIC LOG: Cetak konfigurasi yang digunakan
    logging.warning("--- [DIAGNOSTIC] Attempting DB Connection ---")
    for key, value in conn_config.items():
        if key != "password":
            logging.warning(f"[DIAGNOSTIC] Using config: {key} = {value}")
    logging.warning("------------------------------------------")
    
    from fastapi import HTTPException
    
    try:
        conn = psycopg2.connect(**conn_config) # <--- GUNAKAN conn_config
        yield conn
    except psycopg2.Error as e:
        logging.error(f"Database connection error: {e}", exc_info=True)
        # HTTPException diimpor di dalam fungsi untuk menghindari conflict saat startup
        raise HTTPException(status_code=503, detail="Layanan Database tidak tersedia.")
    finally:
        if conn:
            conn.close()

# --- 2. LOGIC BULK INSERT TRANSAKSI CSV (Psycopg2) ---

def bulk_insert_transactions(data_tuples: List[Tuple], summary_id: int):
    """
    Melakukan bulk insert data transaksi ke PostgreSQL.
    Menggunakan teknik Psycopg2 execute_values untuk efisiensi.
    """
    # Kolom ini harus sesuai dengan urutan kolom dalam CSV dan model Anda (20 kolom + 1 FK)
    cols_for_insert = [
        'transaction_id_asersi', 'tanggal', 'jam', 'mor', 'provinsi', 
        'kota_kabupaten', 'no_spbu', 'no_nozzle', 'no_dispenser', 
        'produk', 'volume_liter', 'penjualan_rupiah', 'operator', 
        'mode_transaksi', 'plat_nomor', 'nik', 'sektor_non_kendaraan', 
        'jumlah_roda_kendaraan', 'kuota', 'warna_plat', 'daily_summary_id',
        'batch_original_duplicate_count'
    ]
    
    # Menambahkan summary_id (FK) ke setiap baris
    data_with_fk = [item + (summary_id,) for item in data_tuples]
    
    # Query SQL: INSERT ... ON CONFLICT (transaction_id_asersi) DO UPDATE
    insert_query = sql.SQL("""
        INSERT INTO {} ({}) 
        VALUES %s
        ON CONFLICT (transaction_id_asersi) DO UPDATE SET import_attempt_count = {}.import_attempt_count + 1, daily_summary_id = EXCLUDED.daily_summary_id, batch_original_duplicate_count = EXCLUDED.batch_original_duplicate_count
    """).format(
        sql.Identifier(TRANSACTION_TABLE),
        sql.SQL(', ').join(map(sql.Identifier, cols_for_insert)),
        sql.Identifier(TRANSACTION_TABLE)
    )

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                logging.warning(f"[DIAGNOSTIC] Attempting bulk insert for {len(data_with_fk)} rows.")
                extras.execute_values(
                    cursor,
                    insert_query,
                    data_with_fk,
                    page_size=10000
                )
                conn.commit()
                logging.warning(f"[DIAGNOSTIC] Bulk insert committed. Rows affected: {cursor.rowcount}")
                return cursor.rowcount
    except Exception as e:
        logging.error(f"Bulk insert failed: {e}", exc_info=True)
        raise e

def create_summary_entry(
    import_datetime,
    import_duration,
    file_name,
    title,
    total_records_inserted,
    total_records_read, # New parameter
    total_volume,
    total_penjualan,
    total_operator,
    produk_jbt,
    produk_jbkt,
    total_volume_liter,
    total_penjualan_rupiah,
    total_mode_transaksi,
    total_plat_nomor,
    total_nik,
    total_sektor_non_kendaraan,
    total_jumlah_roda_kendaraan_4,
    total_jumlah_roda_kendaraan_6,
    total_kuota,
    total_warna_plat_kuning,
    total_warna_plat_hitam,
    total_warna_plat_merah,
    total_warna_plat_putih,
    total_mor,
    total_provinsi,
    total_kota_kabupaten,
    total_no_spbu,
    numeric_totals # New parameter
):
    """Membuat entry baru di tabel summary dan mengembalikan summary_id."""
    insert_query = sql.SQL("""
        INSERT INTO {} (
            import_datetime, import_duration, file_name, title, total_records_inserted, total_records_read,
            total_volume, total_penjualan, total_operator, produk_jbt, produk_jbkt,
            total_volume_liter, total_penjualan_rupiah, total_mode_transaksi,
            total_plat_nomor, total_nik, total_sektor_non_kendaraan,
            total_jumlah_roda_kendaraan_4, total_jumlah_roda_kendaraan_6, total_kuota,
            total_warna_plat_kuning, total_warna_plat_hitam, total_warna_plat_merah,
            total_warna_plat_putih, total_mor, total_provinsi, total_kota_kabupaten,
            total_no_spbu, numeric_totals
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING summary_id
    """).format(sql.Identifier(SUMMARY_TABLE))
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(insert_query, (
                    import_datetime, import_duration, file_name, title, total_records_inserted, total_records_read,
                    total_volume, total_penjualan, total_operator, produk_jbt, produk_jbkt,
                    total_volume_liter, total_penjualan_rupiah, total_mode_transaksi,
                    total_plat_nomor, total_nik, total_sektor_non_kendaraan,
                    total_jumlah_roda_kendaraan_4, total_jumlah_roda_kendaraan_6, total_kuota,
                    total_warna_plat_kuning, total_warna_plat_hitam, total_warna_plat_merah,
                    total_warna_plat_putih, total_mor, total_provinsi, total_kota_kabupaten,
                    total_no_spbu, numeric_totals
                ))
                summary_id = cursor.fetchone()[0]
                conn.commit()
                logging.warning(f"[DIAGNOSTIC] Successfully created summary entry with ID: {summary_id}")

                # Diagnostic: Verify if the summary_id exists immediately after commit
                verify_query = sql.SQL("SELECT COUNT(*) FROM {} WHERE summary_id = %s").format(sql.Identifier(SUMMARY_TABLE))
                cursor.execute(verify_query, (summary_id,))
                if cursor.fetchone()[0] == 1:
                    logging.warning(f"[DIAGNOSTIC] Verified: summary_id {summary_id} exists in {SUMMARY_TABLE}.")
                else:
                    logging.error(f"[DIAGNOSTIC] ERROR: summary_id {summary_id} DOES NOT EXIST in {SUMMARY_TABLE} after commit!")

                return summary_id
    except Exception as e:
        logging.error(f"Failed to create summary entry: {e}", exc_info=True)
        raise e

def update_summary_total_records(summary_id: int, total_records_inserted: int):
    """
    Memperbarui total_records_inserted untuk entry summary yang sudah ada.
    """
    logging.warning(f"--- [DIAGNOSTIC] Calling update_summary_total_records for summary_id: {summary_id} with total_records_inserted: {total_records_inserted} ---")
    update_query = sql.SQL("""
        UPDATE {} SET total_records_inserted = %s WHERE summary_id = %s
    """).format(sql.Identifier(SUMMARY_TABLE))

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(update_query, (total_records_inserted, summary_id))
                logging.warning(f"--- [DIAGNOSTIC] Update query executed. Rows affected: {cursor.rowcount} ---")
                conn.commit()
                logging.warning(f"--- [DIAGNOSTIC] Update committed for summary_id: {summary_id} ---")
    except Exception as e:
        logging.error(f"Failed to update summary total records: {e}", exc_info=True)
        raise e

def count_transactions_for_summary(summary_id: int) -> int:
    """
    Menghitung jumlah total transaksi yang terkait dengan daily_summary_id tertentu.
    """
    count_query = sql.SQL("""
        SELECT COUNT(*) FROM {} WHERE daily_summary_id = %s
    """).format(sql.Identifier(TRANSACTION_TABLE))

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(count_query, (summary_id,))
                count = cursor.fetchone()[0]
                logging.warning(f"--- [DIAGNOSTIC] count_transactions_for_summary for summary_id {summary_id} returned: {count} ---")
                return count
    except Exception as e:
        logging.error(f"Failed to count transactions for summary_id {summary_id}: {e}", exc_info=True)
        raise e

def insert_mor_if_not_exists(mor_id: int, mor: str):
    """
    Memasukkan mor_id dan mor ke tabel_mor jika belum ada.
    """
    logging.warning(f"[DIAGNOSTIC] Attempting to insert MOR: mor_id={mor_id}, mor='{mor}'")
    check_query = sql.SQL("SELECT mor_id FROM tabel_mor WHERE mor_id = %s")
    insert_query = sql.SQL("INSERT INTO tabel_mor (mor_id, mor, created_at, updated_at) VALUES (%s, %s, NOW(), NOW()) ON CONFLICT (mor_id) DO NOTHING")

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(check_query, (mor_id,))
                existing_mor = cursor.fetchone()
                if existing_mor is None:
                    logging.warning(f"[DIAGNOSTIC] MOR {mor_id} not found. Inserting...")
                    cursor.execute(insert_query, (mor_id, mor))
                    conn.commit()
                    logging.warning(f"[DIAGNOSTIC] Successfully inserted new MOR: {mor_id} - {mor}")
                else:
                    logging.warning(f"[DIAGNOSTIC] MOR {mor_id} - {mor} already exists. Skipping insert.")
    except Exception as e:
        logging.error(f"Failed to insert MOR {mor_id} - {mor}: {e}", exc_info=True)
        raise e

# --- 3. LOGIC PEMBUATAN TABEL AWAL (Untuk digunakan di Docker Entrypoint) ---
def create_initial_tables(conn):
    """Membuat tabel log transaksi dan summary jika belum ada."""

    # Jalankan kueri
    try:    
    
        # NOTE: Pastikan semua kolom dari cols_for_insert didefinisikan di sini
        cursor = conn.cursor()

        # CREATE_TRANSACTION_LOG = sql.SQL("""
        #     CREATE TABLE {} (
        #         transaction_id_asersi VARCHAR(50) NOT NULL PRIMARY KEY,
        #         tanggal DATE NOT NULL,
        #         jam TIME WITHOUT TIME ZONE NOT NULL,
        #         mor INTEGER,
        #         provinsi VARCHAR(50),
        #         kota_kabupaten VARCHAR(50),
        #         no_spbu VARCHAR(20),
        #         no_nozzle VARCHAR(20),
        #         no_dispenser VARCHAR(50),
        #         produk VARCHAR(50),
        #         volume_liter NUMERIC(10,3),
        #         penjualan_rupiah VARCHAR(50),
        #         operator VARCHAR(50),
        #         mode_transaksi VARCHAR(50),
        #         plat_nomor VARCHAR(20),
        #         nik VARCHAR(30),
        #         sektor_non_kendaraan VARCHAR(50),
        #         jumlah_roda_kendaraan VARCHAR(50),
        #         kuota NUMERIC(10,1),
        #         warna_plat VARCHAR(20),
        #         daily_summary_id INTEGER,
        #         import_attempt_count INTEGER DEFAULT 0,
        #         batch_original_duplicate_count INTEGER DEFAULT 0,
        #         CONSTRAINT fk_daily_summary FOREIGN KEY (daily_summary_id)
        #             REFERENCES {} (summary_id) ON DELETE CASCADE
        #     );
        # """).format(sql.Identifier(TRANSACTION_TABLE), sql.Identifier(SUMMARY_TABLE))

        # CREATE_SUMMARY_MASTER = sql.SQL("""
        #     CREATE TABLE {} (
        #         summary_id SERIAL PRIMARY KEY,
        #         import_datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        #         import_duration NUMERIC(20,3),
        #         file_name VARCHAR(50),
        #         title VARCHAR(50),
        #         total_records_inserted INTEGER,
        #         total_records_read INTEGER,
        #         total_volume NUMERIC(20,3),
        #         total_penjualan VARCHAR(50),
        #         total_operator NUMERIC(20,3),
        #         produk_jbt VARCHAR(50),
        #         produk_jbkt VARCHAR(50),
        #         total_volume_liter NUMERIC(10,3),
        #         total_penjualan_rupiah VARCHAR(50),
        #         total_mode_transaksi VARCHAR(50),
        #         total_plat_nomor VARCHAR(20),
        #         total_nik VARCHAR(30),
        #         total_sektor_non_kendaraan VARCHAR(50),
        #         total_jumlah_roda_kendaraan_4 VARCHAR(50),
        #         total_jumlah_roda_kendaraan_6 VARCHAR(50),
        #         total_kuota NUMERIC(10,1),
        #         total_warna_plat_kuning VARCHAR(20),
        #         total_warna_plat_hitam VARCHAR(20),
        #         total_warna_plat_merah VARCHAR(20),
        #         total_warna_plat_putih VARCHAR(20),
        #         total_mor NUMERIC(20,3),
        #         total_provinsi NUMERIC(20,3),
        #         total_kota_kabupaten NUMERIC(20,3),
        #         total_no_spbu NUMERIC(20,3),
        #         numeric_totals JSONB
        #     );
        # """).format(sql.Identifier(SUMMARY_TABLE))

        # # Jalankan kueri
        # cursor.execute(CREATE_SUMMARY_MASTER)
        # cursor.execute(CREATE_TRANSACTION_LOG)
        # conn.commit()
        # logging.warning("SCHEMAS CREATED SUCCESSFULLY") # Tanda keberhasilan

    except psycopg2.Error as e:
        # DEBUG KRITIS: Tampilkan error SQL sebenarnya
        logging.error(f"\n[FATAL SQL SYNTAX ERROR]\n{e.pgerror}\n", exc_info=True)
        conn.rollback() # Batalkan transaksi jika ada error
        raise e # Re-raise error untuk menghentikan proses

    cursor.close()    
