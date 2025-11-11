import logging
import json
import time
import os
import pandas as pd
import psycopg2
from psycopg2 import sql
import redis
import requests
import zipfile
import io

# --- Konfigurasi ---
# Pastikan variabel-variabel ini sesuai dengan lingkungan Anda
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
EXPORT_QUEUE = 'export-queue'
LARAVEL_STORAGE_PATH = '/home/bphmigas/datavista_app/storage/app/public/exports'

# Impor konfigurasi database dari file yang sudah ada
try:
    from db_config import POSTGRES_CONFIG
except ImportError:
    logging.error("Gagal mengimpor db_config. Pastikan file db_config.py ada dan benar.")
    exit(1)

# --- Pengaturan Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Fungsi Helper ---

def get_db_connection():
    """Membuat koneksi database psycopg2."""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        return conn
    except psycopg2.Error as e:
        logging.error(f"Gagal terhubung ke database: {e}")
        return None

def send_callback(url, status, file_path=None, error_message=None):
    """Mengirim status kembali ke Laravel."""
    payload = {
        'status': status,
        'file_path': file_path,
        'error_message': error_message
    }
    try:
        response = requests.patch(url, json=payload, timeout=10)
        response.raise_for_status()
        logging.info(f"Callback berhasil dikirim ke {url} dengan status {status}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Gagal mengirim callback ke {url}: {e}")

def build_query(base_query, filters):
    """Membangun klausa WHERE secara dinamis dari filter."""
    where_clauses = []
    params = []
    
    if not filters:
        return base_query, params

    for group in filters:
        group_clauses = []
        for condition in group.get('conditions', []):
            field = condition.get('field')
            operator = condition.get('operator')
            value = condition.get('value')

            if field and operator and value is not None:
                # Tambahkan validasi sederhana untuk nama kolom
                if field in ['tanggal', 'jam', 'mor', 'provinsi', 'kota_kabupaten', 'no_spbu', 'no_nozzle', 'no_dispenser', 'produk', 'volume_liter', 'penjualan_rupiah', 'operator', 'mode_transaksi', 'plat_nomor', 'nik', 'sektor_non_kendaraan', 'jumlah_roda_kendaraan', 'kuota', 'warna_plat']:
                    if 'LIKE' in operator:
                        group_clauses.append(sql.SQL("{} {} %s").format(sql.Identifier(field), sql.SQL(operator)))
                        params.append(f"%{value}%")
                    else:
                        group_clauses.append(sql.SQL("{} {} %s").format(sql.Identifier(field), sql.SQL(operator)))
                        params.append(value)
        
        if group_clauses:
            # Gabungkan kondisi dalam satu grup dengan 'OR'
            where_clauses.append(sql.SQL("({})").format(sql.SQL(" OR ").join(group_clauses)))

    if where_clauses:
        # Gabungkan grup dengan 'AND'
        base_query += sql.SQL(" AND ") + sql.SQL(" AND ").join(where_clauses)
        
    return base_query, params

# --- Fungsi Pemrosesan Utama ---

def process_export_job(job_data):
    """Fungsi utama untuk memproses satu pekerjaan ekspor."""
    job_id = job_data['job_id']
    callback_url = job_data['callback_url']
    logging.info(f"Memulai pekerjaan ekspor {job_id}...")

    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            raise ConnectionError("Tidak dapat membuat koneksi database.")
        
        cursor = conn.cursor()

        # 1. Ambil data Profile (dari csv_summary_master_daily)
        summary_query = sql.SQL("SELECT * FROM csv_summary_master_daily WHERE summary_id = %s")
        df_profile = pd.read_sql_query(summary_query.as_string(cursor), conn, params=[job_data['summary_id']])
        
        # 2. Ambil data Data (dari csv_import_log)
        base_data_query = sql.SQL("SELECT * FROM csv_import_log WHERE daily_summary_id = %s")
        base_params = [job_data['summary_id']]

        if job_data['source'] == 'filtered':
            final_data_query, filter_params = build_query(base_data_query, job_data.get('filters'))
            params = base_params + filter_params
        else: # 'all'
            final_data_query = base_data_query
            params = base_params
        
        df_data = pd.read_sql_query(final_data_query.as_string(cursor), conn, params=params)
        
        logging.info(f"[{job_id}] Data berhasil diambil. Profile: 1 baris, Data: {len(df_data)} baris.")

        # 3. Buat file ekspor
        os.makedirs(LARAVEL_STORAGE_PATH, exist_ok=True)
        relative_path = f"exports/{job_data['file_name']}"
        full_path = os.path.join('/home/bphmigas/datavista_app/storage/app/public/', relative_path)


        if job_data['format'] == 'xlsx':
            with pd.ExcelWriter(full_path, engine='openpyxl') as writer:
                df_profile.to_excel(writer, sheet_name='profile', index=False)
                df_data.to_excel(writer, sheet_name='data', index=False)
            logging.info(f"[{job_id}] File Excel berhasil dibuat di {full_path}")

        elif job_data['format'] == 'csv':
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                # Buat nama file CSV dinamis
                base_name = os.path.splitext(job_data['file_name'])[0]
                profile_csv_name = f"profile_{base_name}.csv"
                data_csv_name = f"data_{base_name}.csv"

                zip_file.writestr(profile_csv_name, df_profile.to_csv(index=False, sep=';', decimal=','))
                zip_file.writestr(data_csv_name, df_data.to_csv(index=False, sep=';', decimal=','))
            
            with open(full_path, 'wb') as f:
                f.write(zip_buffer.getvalue())
            logging.info(f"[{job_id}] File ZIP (CSV) berhasil dibuat di {full_path}")

        # 4. Kirim callback sukses
        send_callback(callback_url, 'COMPLETED', file_path=relative_path)

    except Exception as e:
        logging.error(f"Pekerjaan ekspor {job_id} gagal: {e}", exc_info=True)
        send_callback(callback_url, 'FAILED', error_message=str(e))
    finally:
        if conn:
            conn.close()

# --- Main Loop Worker ---

def main():
    """Loop utama worker untuk mendengarkan antrian Redis."""
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        r.ping()
        logging.info(f"Berhasil terhubung ke Redis dan mendengarkan antrian '{EXPORT_QUEUE}'...")
    except redis.exceptions.ConnectionError as e:
        logging.error(f"Gagal terhubung ke Redis di {REDIS_HOST}:{REDIS_PORT}. Error: {e}")
        return

    while True:
        try:
            # blpop adalah blocking pop, akan menunggu hingga ada item di antrian
            _, job_json = r.blpop(EXPORT_QUEUE)
            logging.info("Pekerjaan baru diterima!")
            job_data = json.loads(job_json)
            process_export_job(job_data)
        except redis.exceptions.ConnectionError as e:
            logging.error(f"Koneksi Redis terputus, mencoba menghubungkan kembali... Error: {e}")
            time.sleep(5)
        except json.JSONDecodeError as e:
            logging.error(f"Gagal mem-parsing data pekerjaan (JSON tidak valid): {e}")
        except Exception as e:
            logging.error(f"Terjadi error tak terduga di loop utama: {e}", exc_info=True)
            time.sleep(5) # Beri jeda sebelum mencoba lagi

if __name__ == "__main__":
    main()
