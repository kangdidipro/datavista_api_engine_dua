from fastapi import APIRouter, UploadFile, File, HTTPException, status, Form
from fastapi.responses import JSONResponse
from typing import List
import pandas as pd
import io
import csv
import logging
import time
import json
from datetime import datetime
# Import Absolut yang sudah dikoreksi:
# from models.schemas import TransactionData
from app.database import bulk_insert_transactions, get_db_connection, create_summary_entry, update_summary_total_records, count_transactions_for_summary, insert_mor_if_not_exists, get_spbu_details_by_no_spbu, get_all_spbu_details


router = APIRouter(prefix="/v1/import", tags=["CSV Bulk Import"])

@router.post("/csv")
async def import_csv_to_db(
    file: UploadFile = File(..., description="File untuk diimport (CSV atau XLSX)"),
    title: str = Form(..., description="Judul untuk impor"),
    type_file: str = Form(..., description="Tipe file: 'A' untuk Asersi (CSV), 'P' untuk Pertamina (XLSX)")
):
    """
    API 1: Menerima file CSV/XLSX, memvalidasi, membersihkan, dan melakukan bulk insert.
    Menerapkan logic Pandas (pembersihan data, konversi tipe).
    """
    logging.warning("--- [DIAGNOSTIC] /v1/import/csv endpoint hit. Starting processing. ---")
    
    logging.warning(f"--- [DIAGNOSTIC] Menerima file: {file.filename}, Tipe Konten: {file.content_type}, Tipe File: {type_file} ---")
    
    try:
        df = pd.DataFrame() # Initialize DataFrame
        contents = await file.read()

        if type_file == 'A':
            if file.content_type not in ["text/csv", "application/vnd.ms-excel", "application/octet-stream"]:
                raise HTTPException(status_code=400, detail="Untuk Tipe A, hanya menerima format file CSV.")
            csv_buffer = io.BytesIO(contents)
            df = pd.read_csv(
                csv_buffer, 
                                sep=';',
                                decimal=',',
                                header=None,
                                skiprows=1,
                                comment='#',                names=[
                    'transaction_id_asersi', 'tanggal', 'jam', 'mor', 'provinsi', 
                    'kota_kabupaten', 'no_spbu', 'no_nozzle', 'no_dispenser', 
                    'produk', 'volume_liter', 'penjualan_rupiah', 'operator', 
                    'mode_transaksi', 'plat_nomor', 'nik', 'sektor_non_kendaraan', 
                    'jumlah_roda_kendaraan', 'kuota', 'warna_plat'
                ]
            )
            # Pastikan jumlah kolom sesuai skema (20 kolom)
            if df.shape[1] < 20:
                raise HTTPException(status_code=422, detail="Untuk Tipe A, format CSV tidak valid: Jumlah kolom kurang dari 20.")

        elif type_file == 'P':
            if file.content_type not in ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "application/vnd.ms-excel"]:
                raise HTTPException(status_code=400, detail="Untuk Tipe P, hanya menerima format file XLSX.")
            excel_buffer = io.BytesIO(contents)
            df = pd.read_excel(excel_buffer)
            
            # Define expected columns from Type P XLSX (based on actual datarow-spbu-54xxxx.xlsx)
            type_p_columns = [
                'tanggal', 'jam', 'code_spbu', 'nozzle', 'dispenser', 'produk', 
                'volume_terjual', 'revenue', 'petugas', 'odometer', 'delivery_type', 
                'plat_nomor', 'jenis_transaksi', 'agency_type', 'agency_name'
            ]
            
            # Check if all expected columns are present
            if not all(col in df.columns for col in type_p_columns):
                missing_cols = [col for col in type_p_columns if col not in df.columns]
                raise HTTPException(status_code=422, detail=f"Untuk Tipe P, file XLSX tidak valid: Kolom berikut tidak ditemukan: {', '.join(missing_cols)}")

            # Rename columns for Type P to match CsvImportLog schema
            df = df.rename(columns={
                'tanggal': 'tanggal',
                'jam': 'jam',
                'code_spbu': 'no_spbu',
                'nozzle': 'no_nozzle',
                'dispenser': 'no_dispenser',
                'produk': 'produk',
                'volume_terjual': 'volume_liter',
                'revenue': 'penjualan_rupiah',
                'petugas': 'operator',
                'plat_nomor': 'plat_nomor',
                'jenis_transaksi': 'mode_transaksi',
                # 'agency_type' and 'agency_name' are not directly mapped to CsvImportLog
                # They might be used for other purposes or can be ignored for now.
            })

            # Explicitly convert 'tanggal' and 'jam' to datetime objects
            df['tanggal'] = pd.to_datetime(df['tanggal'])
            df['jam'] = pd.to_datetime(df['jam']).dt.time # Extract only the time part

            # Derive transaction_id_asersi
            df['transaction_id_asersi'] = df['no_spbu'].astype(str) + '_' + \
                                          df['no_nozzle'].astype(str) + '_' + \
                                          df['tanggal'].dt.strftime('%Y%m%d') + '_' + \
                                          df['jam'].astype(str).str.replace(':', '')

            # Optimize SPBU details lookup
            unique_spbus = df['no_spbu'].astype(str).unique().tolist()
            spbu_details_map = get_all_spbu_details(unique_spbus)

            df['mor'] = df['no_spbu'].astype(str).map(lambda x: spbu_details_map.get(x, (None, None, None))[0])
            df['provinsi'] = df['no_spbu'].astype(str).map(lambda x: spbu_details_map.get(x, (None, None, None))[1])
            df['kota_kabupaten'] = df['no_spbu'].astype(str).map(lambda x: spbu_details_map.get(x, (None, None, None))[2])

            # Set NULL for columns not present in Type P
            df['no_dispenser'] = None
            df['plat_nomor'] = None
            df['nik'] = None
            df['sektor_non_kendaraan'] = None
            df['jumlah_roda_kendaraan'] = None
            df['kuota'] = None
            df['warna_plat'] = None

            # Set default values for other columns
            df['import_attempt_count'] = 1
            df['batch_original_duplicate_count'] = 0

            # Ensure all 20 columns + batch_original_duplicate_count are present and in order
            # This list should match the final cols_for_insert
            expected_final_columns = [
                'transaction_id_asersi', 'tanggal', 'jam', 'mor', 'provinsi', 
                'kota_kabupaten', 'no_spbu', 'no_nozzle', 'no_dispenser', 
                'produk', 'volume_liter', 'penjualan_rupiah', 'operator', 
                'mode_transaksi', 'plat_nomor', 'nik', 'sektor_non_kendaraan', 
                'jumlah_roda_kendaraan', 'kuota', 'warna_plat',
                'batch_original_duplicate_count'
            ]
            df = df[expected_final_columns] # Reorder and select columns

        else:
            raise HTTPException(status_code=400, detail="Tipe file tidak valid. Harus 'A' atau 'P'.")

        start_time = time.perf_counter()
        file_name = file.filename
        
        # --- 3. LOGIC PANDAS: Pembersihan Data dan Konversi Tipe ---
        
        # Konversi format tanggal dari DD/MM/YYYY ke YYYY-MM-DD (for Type A)
        # For Type P, pandas read_excel might already parse dates, or we need to handle it.
        # Assuming for Type P, 'tanggal' is already datetime object from pd.read_excel
        if type_file == 'A':
            df['tanggal'] = pd.to_datetime(df['tanggal'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
        elif type_file == 'P':
            # Ensure 'tanggal' is in YYYY-MM-DD string format for consistency
            df['tanggal'] = pd.to_datetime(df['tanggal']).dt.strftime('%Y-%m-%d')
            # Ensure 'jam' is in HH:MM:SS string format
            df['jam'] = pd.to_datetime(df['jam'], format='%H:%M:%S').dt.strftime('%H:%M:%S')

        # Mengonversi kolom ke tipe numerik, mengubah error menjadi NaN (Not a Number)
        df['volume_liter'] = pd.to_numeric(df['volume_liter'], errors='coerce')
        df['penjualan_rupiah'] = pd.to_numeric(df['penjualan_rupiah'], errors='coerce')
        df['mor'] = pd.to_numeric(df['mor'], errors='coerce')
        df['kuota'] = pd.to_numeric(df['kuota'], errors='coerce')

        # Hitung duplikasi dalam batch asli sebelum drop_duplicates
        duplicate_counts = df['transaction_id_asersi'].value_counts()
        df['batch_original_duplicate_count'] = df['transaction_id_asersi'].map(duplicate_counts)

        # Menghapus duplikat berdasarkan kolom ID (transaction_id_asersi)
        df.drop_duplicates(subset=['transaction_id_asersi'], keep='first', inplace=True) 
        
        # Mengganti NaN dengan None (Wajib untuk insert Psycopg2/PostgreSQL)
        df = df.replace({pd.NA: None, float('nan'): None, '': None})

        # --- Tambahkan MOR ke tabel_mor jika belum ada ---
        unique_mors = df[['mor']].dropna().drop_duplicates()
        logging.warning(f"[DIAGNOSTIC] Unique MORs extracted from CSV: {unique_mors.to_dict(orient='records')}")
        for index, row in unique_mors.iterrows():
            mor_id = int(row['mor'])
            mor_name = f"MOR {mor_id}" # Asumsi format nama MOR
            insert_mor_if_not_exists(mor_id, mor_name)
        
        # 3. PERSIAPAN DATA
        # Memastikan urutan kolom sesuai dengan yang diharapkan oleh 'bulk_insert_transactions'
        cols_for_insert = [
            'transaction_id_asersi', 'tanggal', 'jam', 'mor', 'provinsi', 
            'kota_kabupaten', 'no_spbu', 'no_nozzle', 'no_dispenser', 
            'produk', 'volume_liter', 'penjualan_rupiah', 'operator', 
            'mode_transaksi', 'plat_nomor', 'nik', 'sektor_non_kendaraan', 
            'jumlah_roda_kendaraan', 'kuota', 'warna_plat'
        ]
        # Menyelaraskan DataFrame dengan urutan kolom yang benar
        df_aligned = df[cols_for_insert]
        data_to_insert = [tuple(row) for row in df_aligned.values]

        # 4. MEMBUAT SUMMARY ENTRY AWAL (dengan total_records_inserted = 0)
        total_volume = df['volume_liter'].sum()
        total_penjualan = df['penjualan_rupiah'].astype(float).sum() # Pastikan tipe data numerik

        total_operator = df['operator'].nunique()
        produk_jbt = df[df['produk'].isin(['BIO_SOLAR', 'PERTALITE'])]['produk'].nunique()
        produk_jbkt = df[~df['produk'].isin(['BIO_SOLAR', 'PERTALITE'])]['produk'].nunique()

        total_volume_liter = df['volume_liter'].sum()
        total_penjualan_rupiah = df['penjualan_rupiah'].astype(float).sum()
        total_mode_transaksi = df['mode_transaksi'].nunique()
        total_plat_nomor = df['plat_nomor'].nunique()
        total_nik = df['nik'].nunique()
        total_sektor_non_kendaraan = df['sektor_non_kendaraan'].dropna().nunique() if 'sektor_non_kendaraan' in df.columns and not df['sektor_non_kendaraan'].dropna().empty else 0

        total_jumlah_roda_kendaraan_4 = df[df['jumlah_roda_kendaraan'] == '4']['jumlah_roda_kendaraan'].count()
        total_jumlah_roda_kendaraan_6 = df[df['jumlah_roda_kendaraan'] == '6']['jumlah_roda_kendaraan'].count()

        total_kuota = df['kuota'].sum()

        total_warna_plat_kuning = df[df['warna_plat'] == 'Kuning']['warna_plat'].count()
        total_warna_plat_hitam = df[df['warna_plat'] == 'Hitam']['warna_plat'].count()
        total_warna_plat_merah = df[df['warna_plat'] == 'Merah']['warna_plat'].count()
        total_warna_plat_putih = df[df['warna_plat'] == 'Putih']['warna_plat'].count()

        total_mor = df['mor'].nunique()
        total_provinsi = df['provinsi'].nunique()
        total_kota_kabupaten = df['kota_kabupaten'].nunique()
        total_no_spbu = df['no_spbu'].nunique()

        # Calculate numeric_totals
        numeric_totals = {
            "total_volume": float(total_volume),
            "total_penjualan": float(total_penjualan),
            "total_operator": float(total_operator),
            "produk_jbt": float(produk_jbt),
            "produk_jbkt": float(produk_jbkt),
            "total_volume_liter": float(total_volume_liter),
            "total_penjualan_rupiah": float(total_penjualan_rupiah),
            "total_mode_transaksi": float(total_mode_transaksi),
            "total_plat_nomor": float(total_plat_nomor),
            "total_nik": float(total_nik),
            "total_sektor_non_kendaraan": float(total_sektor_non_kendaraan),
            "total_jumlah_roda_kendaraan_4": float(total_jumlah_roda_kendaraan_4),
            "total_jumlah_roda_kendaraan_6": float(total_jumlah_roda_kendaraan_6),
            "total_kuota": float(total_kuota),
            "total_warna_plat_kuning": float(total_warna_plat_kuning),
            "total_warna_plat_hitam": float(total_warna_plat_hitam),
            "total_warna_plat_merah": float(total_warna_plat_merah),
            "total_warna_plat_putih": float(total_warna_plat_putih),
            "total_mor": float(total_mor),
            "total_provinsi": float(total_provinsi),
            "total_kota_kabupaten": float(total_kota_kabupaten),
            "total_no_spbu": float(total_no_spbu),
        }

        end_time = time.perf_counter()
        duration_ms = int((end_time - start_time) * 1000)

        logging.warning(f"--- [DIAGNOSTIC] df.shape[0] (total records in CSV after processing): {df.shape[0]} ---")

        # Buat entry summary awal dengan total_records_inserted = 0
        summary_id = create_summary_entry(
            import_datetime=datetime.now(),
            import_duration=float(duration_ms / 1000), # Convert ms to seconds
            file_name=file_name,
            title=title, # Gunakan title dari parameter
            total_records_inserted=0, # Akan diupdate setelah bulk insert
            total_records_read=df.shape[0], # Total records read from CSV
            file_type=type_file, # Pass the file type
            total_volume=float(total_volume),
            total_penjualan=str(total_penjualan),
            total_operator=float(total_operator),
            produk_jbt=str(produk_jbt),
            produk_jbkt=str(produk_jbkt),
            total_volume_liter=float(total_volume_liter),
            total_penjualan_rupiah=str(total_penjualan_rupiah),
            total_mode_transaksi=str(total_mode_transaksi),
            total_plat_nomor=str(total_plat_nomor),
            total_nik=str(total_nik),
            sektor_non_kendaraan=str(total_sektor_non_kendaraan),
            total_jumlah_roda_kendaraan_4=str(total_jumlah_roda_kendaraan_4),
            total_jumlah_roda_kendaraan_6=str(total_jumlah_roda_kendaraan_6),
            total_kuota=float(total_kuota),
            total_warna_plat_kuning=str(total_warna_plat_kuning),
            total_warna_plat_hitam=str(total_warna_plat_hitam),
            total_warna_plat_merah=str(total_warna_plat_merah),
            total_warna_plat_putih=str(total_warna_plat_putih),
            total_mor=float(total_mor),
            total_provinsi=float(total_provinsi),
            total_kota_kabupaten=float(total_kota_kabupaten),
            total_no_spbu=float(total_no_spbu),
            numeric_totals=json.dumps(numeric_totals) # Pass numeric_totals as JSON string
        )
        logging.warning(f"--- [DIAGNOSTIC] numeric_totals (dict): {numeric_totals} ---")
        logging.warning(f"--- [DIAGNOSTIC] numeric_totals (json string): {json.dumps(numeric_totals)} ---")
        logging.warning(f"--- [DIAGNOSTIC] Created summary entry with ID: {summary_id} ---")

        # 5. EKSEKUSI BULK INSERT
        logging.warning("--- [DIAGNOSTIC] Calling bulk_insert_transactions ---")
        rows_inserted = bulk_insert_transactions(data_to_insert, summary_id)
        logging.warning(f"--- [DIAGNOSTIC] Bulk insert completed. {rows_inserted} rows inserted. ---")

        # Update total_records_inserted di summary entry
        # Hitung ulang jumlah transaksi yang benar-benar terkait dengan summary_id ini
        actual_records_in_summary = count_transactions_for_summary(summary_id)
        update_summary_total_records(summary_id, actual_records_in_summary)
        logging.warning(f"--- [DIAGNOSTIC] Updated summary {summary_id} with {actual_records_in_summary} records inserted. ---")

        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={
                "message": "Import CSV berhasil diproses.",
                "total_rows_read": df.shape[0],
                "total_rows_inserted": actual_records_in_summary,
                "summary_id": summary_id
            }
        )
    except Exception as e:
        # Log traceback lengkap untuk debugging di sisi server
        logging.error(f"Gagal memproses file: {e}", exc_info=True)
        
        # Kirim pesan error yang spesifik ke client untuk debugging
        error_type = type(e).__name__
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, 
            detail=f"Proses file gagal. Error Sebenarnya: [{error_type}] - {str(e)}"
        )
