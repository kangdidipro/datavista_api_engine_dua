from __future__ import annotations # Enable Postponed Evaluation of Annotations
import logging
from db_config import POSTGRES_CONFIG
from contextlib import contextmanager
from typing import List, Tuple
from sqlalchemy import create_engine, URL
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session # Import Session
import psycopg2
from psycopg2 import sql, extras # Import sql and extras

from app.schemas import AnomalyTemplateMasterCreate, TransactionAnomalyCriteriaCreate, SpecialAnomalyCriteriaCreate, AccumulatedAnomalyCriteriaCreate, VideoAiParameterCreate
from app.models import AnomalyTemplateMaster, TransactionAnomalyCriteria, SpecialAnomalyCriteria, AccumulatedAnomalyCriteria, VideoAiParameter, TemplateCriteriaVolume, TemplateCriteriaSpecial, TemplateCriteriaVideo, TemplateCriteriaAccumulated
from crud import anomaly_crud
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text

from app.db_base import Base # Import Base from the new file

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
            logging.error(f"[DEBUG] POSTGRES_CONFIG used by psycopg2.connect: {conn_config}")
        
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
    # Kolom ini harus sesuai dengan urutan kolom dalam CSV dan model Anda (20 kolom + 3 tambahan)
    cols_for_insert = [
        'transaction_id_asersi', 'tanggal', 'jam', 'mor', 'provinsi', 
        'kota_kabupaten', 'no_spbu', 'no_nozzle', 'no_dispenser', 
        'produk', 'volume_liter', 'penjualan_rupiah', 'operator', 
        'mode_transaksi', 'plat_nomor', 'nik', 'sektor_non_kendaraan', 
        'jumlah_roda_kendaraan', 'kuota', 'warna_plat', 
        'daily_summary_id',          # FK
        'import_attempt_count',      # Default 1
        'batch_original_duplicate_count' # Default 0
    ]
    
    # Menambahkan daily_summary_id (FK), import_attempt_count (default 1), dan batch_original_duplicate_count (default 0) ke setiap baris
    data_with_defaults_and_fk = [item + (summary_id, 1, 0) for item in data_tuples] 
    logging.warning(f"[DIAGNOSTIC] bulk_insert_transactions using summary_id: {summary_id}")
    
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
                logging.warning(f"[DIAGNOSTIC] Attempting bulk insert for {len(data_with_defaults_and_fk)} rows.")
                logging.error(f"[DEBUG] Data to insert (first 5 rows): {data_with_defaults_and_fk[:5]}")
                extras.execute_values(
                    cursor,
                    insert_query,
                    data_with_defaults_and_fk,
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
    total_records_read,
    file_type: str, # New parameter
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
    sektor_non_kendaraan,
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
    column_names = [
        'import_datetime', 'import_duration', 'file_name', 'title', 'total_records_inserted', 'total_records_read', 'file_type',
        'total_volume', 'total_penjualan', 'total_operator', 'produk_jbt', 'produk_jbkt',
        'total_volume_liter', 'total_penjualan_rupiah', 'total_mode_transaksi',
        'total_plat_nomor', 'total_nik', 'sektor_non_kendaraan',
        'total_jumlah_roda_kendaraan_4', 'total_jumlah_roda_kendaraan_6', 'total_kuota',
        'total_warna_plat_kuning', 'total_warna_plat_hitam', 'total_warna_plat_merah',
        'total_warna_plat_putih', 'total_mor', 'total_provinsi', 'total_kota_kabupaten',
        'total_no_spbu', 'numeric_totals'
    ]
    insert_query = sql.SQL("""
        INSERT INTO {} ({})
        VALUES ({})
        RETURNING summary_id
    """).format(
        sql.Identifier(SUMMARY_TABLE),
        sql.SQL(', ').join(map(sql.Identifier, column_names)),
        sql.SQL(', ').join(sql.Placeholder() * len(column_names))
    )
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                logging.error(f"[DEBUG] Executing query: {insert_query.as_string(conn)}")
                logging.error(f"[DEBUG] With parameters: {import_datetime, import_duration, file_name, title, total_records_inserted, total_records_read, file_type, total_volume, total_penjualan, total_operator, produk_jbt, produk_jbkt, total_volume_liter, total_penjualan_rupiah, total_mode_transaksi, total_plat_nomor, total_nik, sektor_non_kendaraan, total_jumlah_roda_kendaraan_4, total_jumlah_roda_kendaraan_6, total_kuota, total_warna_plat_kuning, total_warna_plat_hitam, total_warna_plat_merah, total_warna_plat_putih, total_mor, total_provinsi, total_kota_kabupaten, total_no_spbu, numeric_totals}")
                cursor.execute(insert_query, (
                    import_datetime, import_duration, file_name, title, total_records_inserted, total_records_read, file_type,
                    total_volume, total_penjualan, total_operator, produk_jbt, produk_jbkt,
                    total_volume_liter, total_penjualan_rupiah, total_mode_transaksi,
                    total_plat_nomor, total_nik, sektor_non_kendaraan,
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
    logging.warning(f"--- [DIAGNOSTIC] count_transactions_for_summary querying for summary_id: {summary_id} ---")

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                conn.rollback() # Clear any pending transaction state
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

def get_spbu_details_by_no_spbu(no_spbu: str) -> Tuple[int | None, str | None, str | None]:
    """
    Mengambil detail MOR, provinsi, dan kota/kabupaten dari tabel master SPBU
    berdasarkan nomor SPBU.
    Mengembalikan tuple (mor, provinsi, kota_kabupaten) atau (None, None, None) jika tidak ditemukan.
    """
    logging.warning(f"[DIAGNOSTIC] Looking up SPBU details for no_spbu: {no_spbu}")
    query = sql.SQL("SELECT mor, provinsi, kota_kabupaten FROM tabel_spbu_master WHERE no_spbu = %s")
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (no_spbu,))
                result = cursor.fetchone()
                if result:
                    logging.warning(f"[DIAGNOSTIC] Found SPBU details: {result}")
                    return result
                else:
                    logging.warning(f"[DIAGNOSTIC] SPBU details not found for no_spbu: {no_spbu}")
                    return None, None, None
    except Exception as e:
        logging.error(f"Failed to get SPBU details for no_spbu {no_spbu}: {e}", exc_info=True)
        return None, None, None

def get_all_spbu_details(no_spbu_list: List[str]) -> dict:
    """
    Mengambil detail MOR, provinsi, dan kota/kabupaten untuk daftar no_spbu.
    Mengembalikan dictionary {no_spbu: (mor, provinsi, kota_kabupaten)}.
    """
    if not no_spbu_list:
        return {}

    logging.warning(f"[DIAGNOSTIC] Looking up details for {len(no_spbu_list)} unique SPBUs.")
    query = sql.SQL("SELECT no_spbu, mor, provinsi, kota_kabupaten FROM tabel_spbu_master WHERE no_spbu = ANY(%s)")
    
    spbu_details_map = {}
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (list(set(no_spbu_list)),)) # Use set to ensure unique SPBUs
                results = cursor.fetchall()
                for row in results:
                    spbu_details_map[row[0]] = (row[1], row[2], row[3])
        logging.warning(f"[DIAGNOSTIC] Found details for {len(spbu_details_map)} SPBUs.")
        return spbu_details_map
    except Exception as e:
        logging.error(f"Failed to get all SPBU details: {e}", exc_info=True)
        return {}

def insert_transaction_anomaly_criteria(db: Session):
    criteria_data = [
        {
            "anomaly_type": "VOLUME_EXCEED_60L_R4_BW", # Updated anomaly_type
            "min_volume_liter": 60,
            "plate_color": ["hitam", "putih"],
            "consumer_type": "roda 4",
            "description": "Volume di atas 60 Liter dalam sekali transaksi untuk 1 nomor polisi dalam 1 hari (plat hitam/putih, roda 4)"
        },
        {
            "anomaly_type": "VOLUME_EXCEED_80L_R4_Y", # Updated anomaly_type
            "min_volume_liter": 80,
            "plate_color": ["kuning"],
            "consumer_type": "roda 4",
            "description": "Volume di atas 80 Liter dalam sekali transaksi untuk 1 nomor polisi dalam 1 hari (plat kuning, roda 4)"
        },
        {
            "anomaly_type": "VOLUME_EXCEED_200L_R4_ALL", # Updated anomaly_type
            "min_volume_liter": 200,
            "plate_color": ["hitam", "putih", "kuning"],
            "consumer_type": "roda 4",
            "description": "Volume di atas 200 Liter dalam sekali transaksi untuk 1 nomor polisi dalam 1 hari (plat hitam/putih/kuning, roda 4)"
        },
        {
            "anomaly_type": "VOLUME_EXCEED_200L_R6_ALL", # Updated anomaly_type
            "min_volume_liter": 200,
            "plate_color": ["hitam", "putih", "kuning"],
            "consumer_type": "roda 6",
            "description": "Volume di atas 200 Liter dalam sekali transaksi untuk 1 nomor polisi dalam 1 hari (plat hitam/putih/kuning, roda 6)"
        },
    ]

    logging.info("Inserting TransactionAnomalyCriteria...")
    for data in criteria_data:
        existing_criteria = db.query(TransactionAnomalyCriteria).filter_by(
            anomaly_type=data["anomaly_type"],
            min_volume_liter=data["min_volume_liter"],
            consumer_type=data["consumer_type"]
        ).first()
        
        if existing_criteria:
            logging.info(f"TransactionAnomalyCriteria already exists: {data['description']}. Skipping.")
        else:
            criteria = TransactionAnomalyCriteria(**data)
            db.add(criteria)
            try:
                db.commit()
                db.refresh(criteria)
                logging.info(f"Inserted TransactionAnomalyCriteria: {criteria.description}")
            except IntegrityError:
                db.rollback()
                logging.warning(f"TransactionAnomalyCriteria already exists (IntegrityError): {data['description']}. Rolled back.")
            except Exception as e:
                db.rollback()
                logging.error(f"Error inserting TransactionAnomalyCriteria {data['description']}: {e}")

def insert_accumulated_anomaly_criteria(db: Session):
    criteria_data = [
        {
            "criteria_code": "ACC_VOLUME_EXCEED_60L_R4_BW",
            "criteria_name": "Akumulasi Volume > 60L (Roda 4, Plat Hitam/Putih)",
            "threshold_value": 60,
            "time_window_hours": 24,
            "group_by_field": "plat_nomor",
            "description": "Akumulasi volume di atas 60 Liter untuk 1 nomor polisi dalam 1 hari (plat hitam/putih, roda 4)"
        },
        {
            "criteria_code": "ACC_VOLUME_EXCEED_80L_R4_Y",
            "criteria_name": "Akumulasi Volume > 80L (Roda 4, Plat Kuning)",
            "threshold_value": 80,
            "time_window_hours": 24,
            "group_by_field": "plat_nomor",
            "description": "Akumulasi volume di atas 80 Liter untuk 1 nomor polisi dalam 1 hari (plat kuning, roda 4)"
        },
        {
            "criteria_code": "ACC_VOLUME_EXCEED_200L_R4_ALL",
            "criteria_name": "Akumulasi Volume > 200L (Roda 4, Semua Plat)",
            "threshold_value": 200,
            "time_window_hours": 24,
            "group_by_field": "plat_nomor",
            "description": "Akumulasi volume di atas 200 Liter untuk 1 nomor polisi dalam 1 hari (plat hitam/putih/kuning, roda 4)"
        },
        {
            "criteria_code": "ACC_VOLUME_EXCEED_200L_R6_ALL",
            "criteria_name": "Akumulasi Volume > 200L (Roda 6, Semua Plat)",
            "threshold_value": 200,
            "time_window_hours": 24,
            "group_by_field": "plat_nomor",
            "description": "Akumulasi volume di atas 200 Liter untuk 1 nomor polisi dalam 1 hari (plat hitam/putih/kuning, roda 6)"
        },
    ]

    logging.info("Inserting AccumulatedAnomalyCriteria...")
    for data in criteria_data:
        existing_criteria = db.query(AccumulatedAnomalyCriteria).filter_by(
            criteria_code=data["criteria_code"]
        ).first()

        if existing_criteria:
            logging.info(f"AccumulatedAnomalyCriteria already exists: {data['criteria_name']}. Skipping.")
        else:
            criteria = AccumulatedAnomalyCriteria(**data)
            db.add(criteria)
            try:
                db.commit()
                db.refresh(criteria)
                logging.info(f"Inserted AccumulatedAnomalyCriteria: {criteria.criteria_name}")
            except IntegrityError:
                db.rollback()
                logging.warning(f"AccumulatedAnomalyCriteria already exists (IntegrityError): {data['criteria_name']}. Rolled back.")
            except Exception as e:
                db.rollback()
                logging.error(f"Error inserting AccumulatedAnomalyCriteria {data['criteria_name']}: {e}")

def insert_special_anomaly_criteria(db: Session):
    criteria_data = [
        {
            "criteria_code": "MISSING_PLAT_NOMOR",
            "criteria_name": "Plat Nomor Hilang",
            "violation_rule": "plat_nomor IS NULL OR plat_nomor = ''",
            "description": "Deteksi transaksi yang tidak dilengkapi plat nomor."
        },
        {
            "criteria_code": "MISSING_NIK",
            "criteria_name": "NIK Hilang",
            "violation_rule": "nik IS NULL OR nik = ''",
            "description": "Deteksi transaksi yang tidak dilengkapi NIK."
        },
        {
            "criteria_code": "DUPLICATE_TRANSACTION",
            "criteria_name": "Duplikasi Transaksi",
            "violation_rule": "batch_original_duplicate_count > 0",
            "description": "Deteksi duplikasi data transaksi berdasarkan beberapa kolom."
        },
        {
            "criteria_code": "RED_PLATE_VEHICLE",
            "criteria_name": "Kendaraan Plat Merah",
            "violation_rule": "warna_plat = 'Merah'",
            "description": "Deteksi transaksi yang melibatkan kendaraan dengan plat nomor berwarna merah."
        },
        {
            "criteria_code": "TRANSACTION_INTERVAL_TOO_CLOSE",
            "criteria_name": "Interval Transaksi Terlalu Dekat",
            "violation_rule": "interval < value seconds", # Placeholder for Python logic
            "value": "120", # 2 minutes in seconds
            "description": "Deteksi transaksi yang dilakukan dalam interval waktu terlalu dekat untuk plat nomor yang sama."
        },
    ]

    logging.info("Inserting SpecialAnomalyCriteria...")
    for data in criteria_data:
        existing_criteria = db.query(SpecialAnomalyCriteria).filter_by(
            criteria_code=data["criteria_code"]
        ).first()

        if existing_criteria:
            logging.info(f"SpecialAnomalyCriteria already exists: {data['criteria_name']}. Skipping.")
        else:
            criteria = SpecialAnomalyCriteria(**data)
            db.add(criteria)
            try:
                db.commit()
                db.refresh(criteria)
                logging.info(f"Inserted SpecialAnomalyCriteria: {criteria.criteria_name}")
            except IntegrityError:
                db.rollback()
                logging.warning(f"SpecialAnomalyCriteria already exists (IntegrityError): {data['criteria_name']}. Rolled back.")
            except Exception as e:
                db.rollback()
                logging.error(f"Error inserting SpecialAnomalyCriteria {data['criteria_name']}: {e}")

# --- 3. LOGIC INISIALISASI DATABASE TERPADU ---
def initialize_default_anomaly_rules(db_session: Session):
    """
    Initializes default anomaly criteria and a default template if they don't exist.
    This function is called by init_db() within a single session.
    """
    from app.models import AnomalyTemplateMaster, TransactionAnomalyCriteria, SpecialAnomalyCriteria, AccumulatedAnomalyCriteria
    from app.schemas import AnomalyTemplateMasterCreate, TransactionAnomalyCriteriaCreate, SpecialAnomalyCriteriaCreate, AccumulatedAnomalyCriteriaCreate
    from crud import anomaly_crud

    logging.warning("--- [DIAGNOSTIC] Initializing default anomaly rules. ---")

    # 1. Create/Get Default Anomaly Template
    default_template = db_session.query(AnomalyTemplateMaster).filter_by(is_default=True).first()
    if not default_template:
        default_template = AnomalyTemplateMaster(
            role_name="Default Anomaly Template",
            description="Template default yang menautkan semua kriteria anomali yang tersedia.",
            is_default=True,
            created_by="System"
        )
        db_session.add(default_template)
        db_session.commit()
        db_session.refresh(default_template)
        logging.info(f"Created default AnomalyTemplateMaster with ID: {default_template.template_id}")
    else:
        logging.info(f"Default AnomalyTemplateMaster already exists with ID: {default_template.template_id}. Using existing.")

    template_id = default_template.template_id

    # 2. Link TransactionAnomalyCriteria
    transaction_criteria = db_session.query(TransactionAnomalyCriteria).all()
    for criteria in transaction_criteria:
        existing_link = db_session.query(TemplateCriteriaVolume).filter_by(
            template_id=template_id, criteria_id=criteria.criteria_id
        ).first()
        if not existing_link:
            link = TemplateCriteriaVolume(template_id=template_id, criteria_id=criteria.criteria_id)
            db_session.add(link)
            logging.info(f"Linked TransactionAnomalyCriteria ID {criteria.criteria_id} to Template ID {template_id}")
    
    # 3. Link AccumulatedAnomalyCriteria
    accumulated_criteria = db_session.query(AccumulatedAnomalyCriteria).all()
    for criteria in accumulated_criteria:
        existing_link = db_session.query(TemplateCriteriaAccumulated).filter_by(
            template_id=template_id, accumulated_criteria_id=criteria.accumulated_criteria_id
        ).first()
        if not existing_link:
            link = TemplateCriteriaAccumulated(template_id=template_id, accumulated_criteria_id=criteria.accumulated_criteria_id)
            db_session.add(link)
            logging.info(f"Linked AccumulatedAnomalyCriteria ID {criteria.accumulated_criteria_id} to Template ID {template_id}")

    # 4. Link SpecialAnomalyCriteria
    special_criteria = db_session.query(SpecialAnomalyCriteria).all()
    for criteria in special_criteria:
        existing_link = db_session.query(TemplateCriteriaSpecial).filter_by(
            template_id=template_id, special_criteria_id=criteria.special_criteria_id
        ).first()
        if not existing_link:
            link = TemplateCriteriaSpecial(template_id=template_id, special_criteria_id=criteria.special_criteria_id)
            db_session.add(link)
            logging.info(f"Linked SpecialAnomalyCriteria ID {criteria.special_criteria_id} to Template ID {template_id}")
    
    try:
        db_session.commit()
        logging.info(f"All criteria linked to default template ID: {template_id}")
    except Exception as e:
        db_session.rollback()
        logging.error(f"Error linking criteria to default template: {e}")
        raise


def init_db():
    """
    Initializes the database by dropping and recreating all tables,
    and then seeding initial data like default anomaly rules.
    This function ensures all operations happen in a coordinated way.
    """
    db = SessionLocal()
    try:
        logging.warning("--- [DIAGNOSTIC] Starting database initialization ---")

        # Drop schema to ensure a clean slate, including types and enums
        logging.warning("--- [DIAGNOSTIC] Dropping public schema CASCADE... ---")
        with engine.connect() as connection:
            connection.execute(text("DROP SCHEMA public CASCADE;"))
            connection.execute(text("CREATE SCHEMA public;"))
            connection.commit()
        logging.warning("--- [DIAGNOSTIC] Public schema dropped and recreated. ---")

        # Drop all tables
        logging.warning("--- [DIAGNOSTIC] Dropping all tables... ---")
        Base.metadata.drop_all(bind=engine)
        logging.warning("--- [DIAGNOSTIC] All tables dropped. ---")

        # Create all tables
        logging.warning("--- [DIAGNOSTIC] Creating all tables... ---")
        Base.metadata.create_all(bind=engine)
        logging.warning("--- [DIAGNOSTIC] All tables created. ---")

        # DIAGNOSTIC: Log all table names found by SQLAlchemy
        logging.warning(f"--- [DIAGNOSTIC] Tables found by SQLAlchemy metadata: {Base.metadata.tables.keys()} ---")

        # DIAGNOSTIC: Log columns of CsvSummaryMasterDaily
        if 'csv_summary_master_daily' in Base.metadata.tables:
            table = Base.metadata.tables['csv_summary_master_daily']
            logging.warning(f"--- [DIAGNOSTIC] Columns for csv_summary_master_daily: {[c.name for c in table.columns]} ---")
        else:
            logging.error("--- [DIAGNOSTIC] csv_summary_master_daily table not found in metadata after creation! ---")

        # Initialize default data
        logging.warning("--- [DIAGNOSTIC] Seeding initial data... ---")
        initialize_default_anomaly_rules(db)
        logging.warning("--- [DIAGNOSTIC] Initial data seeded. ---")

        logging.warning("--- [DIAGNOSTIC] Database initialization complete. ---")

    except Exception as e:
        logging.error(f"[FATAL] Could not initialize database: {e}", exc_info=True)
        raise
    finally:
        db.close()
