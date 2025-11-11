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

        # Remove DEFAULT 0 constraint from total_records_read column
        cursor.execute(sql.SQL("""
            ALTER TABLE csv_summary_master_daily
            ALTER COLUMN total_records_read DROP DEFAULT;
        """))
        conn.commit()
        print("Migration applied: Removed DEFAULT 0 from total_records_read in csv_summary_master_daily.")

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

        # Add DEFAULT 0 constraint back to total_records_read column (for rollback)
        cursor.execute(sql.SQL("""
            ALTER TABLE csv_summary_master_daily
            ALTER COLUMN total_records_read SET DEFAULT 0;
        """))
        conn.commit()
        print("Migration reverted: Added DEFAULT 0 back to total_records_read in csv_summary_master_daily.")

    except Exception as e:
        print(f"Error reverting migration: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    apply_migration()
