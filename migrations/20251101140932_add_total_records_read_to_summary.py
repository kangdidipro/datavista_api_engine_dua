import psycopg2
from psycopg2 import sql
import os

# Konfigurasi database dari environment variables atau default
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres_db"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "user": os.getenv("POSTGRES_USER", "datavista_api_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "DatavistaAPI@2025"),
    "database": os.getenv("POSTGRES_DB", "datavista_db")
}

def apply_migration():
    conn = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()

        # Add total_records_read column
        cursor.execute(sql.SQL("""
            ALTER TABLE csv_summary_master_daily
            ADD COLUMN total_records_read INTEGER DEFAULT 0;
        """))
        conn.commit()
        print("Migration applied: Added total_records_read to csv_summary_master_daily.")

    except Exception as e:
        print(f"Error applying migration: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def revert_migration():
    conn = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()

        # Remove total_records_read column
        cursor.execute(sql.SQL("""
            ALTER TABLE csv_summary_master_daily
            DROP COLUMN total_records_read;
        """))
        conn.commit()
        print("Migration reverted: Removed total_records_read from csv_summary_master_daily.")

    except Exception as e:
        print(f"Error reverting migration: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    apply_migration()
