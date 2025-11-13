from sqlalchemy.orm import Session
from rq import get_current_job
import pandas as pd # Keep pandas for potential future data manipulation, though not used for file reading here
from app.models import AnomalyTemplateMaster, TransactionAnomalyCriteria, SpecialAnomalyCriteria, AccumulatedAnomalyCriteria, AnomalyResult, AnomalyExecution, AnomalyExecutionBatch, CsvSummaryMasterDaily, CsvImportLog, TabelMor
from app.schemas import AnomalyAnalysisRequest
from datetime import datetime
import logging
import uuid
from typing import Optional

logger = logging.getLogger(__name__)

def run_anomaly_analysis(execution_id: str, summary_ids: list, template_id: int, db: Session):
    logger.info(f"Starting anomaly analysis for execution_id: {execution_id}, summary_ids: {summary_ids}")
    job = get_current_job()

    # Fetch the anomaly template and its associated criteria
    template = db.query(AnomalyTemplateMaster).filter(
        AnomalyTemplateMaster.template_id == template_id
    ).first()

    if not template:
        logger.error(f"Anomaly template with ID {template_id} not found.")
        return {"status": "failed", "execution_id": execution_id, "message": f"Template {template_id} not found"}

    transaction_rules = template.transaction_criteria
    accumulated_rules = template.accumulated_criteria
    special_rules = template.special_criteria

    logger.info(f"Loaded {len(transaction_rules)} transaction rules, {len(accumulated_rules)} accumulated rules, {len(special_rules)} special rules for template {template_id}.")

    # Fetch data from CsvImportLog based on summary_ids
    # For simplicity, we'll fetch all logs associated with the provided summary_ids
    # In a real scenario, you might filter further based on template criteria
    
    # Ensure summary_ids is not empty
    if not summary_ids:
        logger.warning("No summary_ids provided for anomaly analysis. Skipping.")
        return {"status": "skipped", "execution_id": execution_id, "message": "No summary_ids provided"}

    # Fetch relevant CsvImportLog entries
    # Assuming CsvImportLog has a daily_summary_id column linking to CsvSummaryMasterDaily.summary_id
    transactions_to_analyze = db.query(CsvImportLog).filter(
        CsvImportLog.daily_summary_id.in_(summary_ids)
    ).all()

    if not transactions_to_analyze:
        logger.info(f"No transactions found for summary_ids: {summary_ids}. No anomalies to check.")
        return {"status": "completed", "execution_id": execution_id, "summary_ids": summary_ids, "message": "No transactions to analyze"}

    logger.info(f"Found {len(transactions_to_analyze)} transactions to analyze for summary_ids: {summary_ids}")

    # --- Dynamic Anomaly Detection Logic will go here ---
    # This section will be replaced with dynamic rule application based on the fetched template criteria.
    # For now, it's a placeholder.
    anomalies_found_count = 0
    
    # Prepare a dictionary for quick lookup of special rules by criteria_code
    special_rules_dict = {rule.criteria_code: rule for rule in special_rules}
    
    # Convert transactions to a DataFrame for easier manipulation, especially for accumulated rules
    # and interval checks. This also handles datetime conversions once.
    df_transactions = pd.DataFrame([
        {
            "transaction_id_asersi": t.transaction_id_asersi,
            "daily_summary_id": t.daily_summary_id,
            "tanggal": t.tanggal,
            "jam": t.jam,
            "mor": t.mor,
            "provinsi": t.provinsi,
            "kota_kabupaten": t.kota_kabupaten,
            "no_spbu": t.no_spbu,
            "no_nozzle": t.no_nozzle,
            "no_dispenser": t.no_dispenser,
            "produk": t.produk,
            "volume_liter": float(t.volume_liter) if t.volume_liter is not None else None,
            "penjualan_rupiah": float(t.penjualan_rupiah) if t.penjualan_rupiah is not None else None,
            "operator": t.operator,
            "mode_transaksi": t.mode_transaksi,
            "plat_nomor": t.plat_nomor,
            "nik": t.nik,
            "sektor_non_kendaraan": t.sektor_non_kendaraan,
            "jumlah_roda_kendaraan": t.jumlah_roda_kendaraan,
            "kuota": float(t.kuota) if t.kuota is not None else None,
            "warna_plat": t.warna_plat,
            "created_at": t.created_at,
            "updated_at": t.updated_at
        } for t in transactions_to_analyze
    ])

    # Convert 'tanggal' and 'jam' to datetime objects for proper sorting and interval calculation
    df_transactions['transaction_datetime'] = pd.to_datetime(df_transactions['tanggal'] + ' ' + df_transactions['jam'])
    df_transactions = df_transactions.sort_values(by=['plat_nomor', 'transaction_datetime']).reset_index(drop=True)

    # Dictionary to store anomaly results for each transaction_id_asersi
    transaction_anomaly_results = {}

    for index, transaction in df_transactions.iterrows():
        transaction_id_asersi = transaction['transaction_id_asersi']
        summary_id = transaction['daily_summary_id']
        
        is_anomalous = False
        anomaly_flags = []
        violation_details = {}

        # --- Apply Transaction Anomaly Rules ---
        for rule in transaction_rules:
            # Example: VOLUME_EXCEED_60L_R4_BW
            if rule.anomaly_type == "SINGLE_VOLUME_EXCEED":
                if transaction['volume_liter'] is not None and transaction['volume_liter'] > rule.min_volume_liter:
                    if rule.plate_color and transaction['warna_plat'] and transaction['warna_plat'].lower() in [pc.lower() for pc in rule.plate_color]:
                        if rule.consumer_type and transaction['jumlah_roda_kendaraan'] and str(transaction['jumlah_roda_kendaraan']) == rule.consumer_type.split(' ')[1]: # e.g., "roda 4" -> "4"
                            is_anomalous = True
                            anomaly_flags.append(rule.anomaly_type)
                            violation_details[rule.anomaly_type] = {
                                "threshold": rule.min_volume_liter,
                                "actual_volume": float(transaction['volume_liter']),
                                "plate_color": transaction['warna_plat'],
                                "consumer_type": transaction['jumlah_roda_kendaraan']
                            }
                            logger.debug(f"Anomaly detected: {rule.anomaly_type} for transaction {transaction_id_asersi}")

        # --- Apply Special Anomaly Rules ---
        for rule in special_rules:
            if rule.criteria_code == "MISSING_PLAT_NOMOR":
                if not transaction['plat_nomor']:
                    is_anomalous = True
                    anomaly_flags.append(rule.criteria_code)
                    violation_details[rule.criteria_code] = {"message": rule.description}
                    logger.debug(f"Anomaly detected: {rule.criteria_code} for transaction {transaction_id_asersi}")
            
            elif rule.criteria_code == "MISSING_NIK":
                if not transaction['nik']:
                    is_anomalous = True
                    anomaly_flags.append(rule.criteria_code)
                    violation_details[rule.criteria_code] = {"message": rule.description}
                    logger.debug(f"Anomaly detected: {rule.criteria_code} for transaction {transaction_id_asersi}")

            elif rule.criteria_code == "DUPLICATE_TRANSACTION":
                # This is handled during import, but we can re-check if needed
                # For now, assume batch_original_duplicate_count is reliable
                original_transaction = db.query(CsvImportLog).filter_by(transaction_id_asersi=transaction_id_asersi).first()
                if original_transaction and original_transaction.batch_original_duplicate_count > 0:
                    is_anomalous = True
                    anomaly_flags.append(rule.criteria_code)
                    violation_details[rule.criteria_code] = {"message": rule.description, "duplicate_count": original_transaction.batch_original_duplicate_count}
                    logger.debug(f"Anomaly detected: {rule.criteria_code} for transaction {transaction_id_asersi}")

            elif rule.criteria_code == "RED_PLATE_VEHICLE":
                if transaction['warna_plat'] and transaction['warna_plat'].lower() == 'merah':
                    is_anomalous = True
                    anomaly_flags.append(rule.criteria_code)
                    violation_details[rule.criteria_code] = {"message": rule.description}
                    logger.debug(f"Anomaly detected: {rule.criteria_code} for transaction {transaction_id_asersi}")
            
            elif rule.criteria_code == "TRANSACTION_INTERVAL_TOO_CLOSE":
                # This requires looking at previous transactions for the same plat_nomor
                # We need to ensure 'value' is an integer (seconds)
                try:
                    interval_threshold_seconds = int(rule.value)
                except (ValueError, TypeError):
                    logger.error(f"Invalid 'value' for TRANSACTION_INTERVAL_TOO_CLOSE rule: {rule.value}. Skipping.")
                    continue

                # Get previous transaction for the same plat_nomor
                if index > 0 and transaction['plat_nomor'] == df_transactions.loc[index-1, 'plat_nomor']:
                    prev_transaction_datetime = df_transactions.loc[index-1, 'transaction_datetime']
                    current_transaction_datetime = transaction['transaction_datetime']
                    
                    time_diff_seconds = (current_transaction_datetime - prev_transaction_datetime).total_seconds()

                    if time_diff_seconds < interval_threshold_seconds:
                        is_anomalous = True
                        anomaly_flags.append(rule.criteria_code)
                        violation_details[rule.criteria_code] = {
                            "message": rule.description,
                            "interval_threshold_seconds": interval_threshold_seconds,
                            "actual_interval_seconds": time_diff_seconds,
                            "previous_transaction_id": df_transactions.loc[index-1, 'transaction_id_asersi']
                        }
                        logger.debug(f"Anomaly detected: {rule.criteria_code} for transaction {transaction_id_asersi}")

        # Store results for current transaction
        if is_anomalous:
            anomalies_found_count += 1
            transaction_anomaly_results[transaction_id_asersi] = {
                "summary_id": summary_id,
                "is_anomalous": True,
                "anomaly_flags": anomaly_flags,
                "violation_details": violation_details,
                "anomaly_datetime": datetime.now()
            }
        else:
            # If no anomalies, ensure it's marked as not anomalous
            transaction_anomaly_results[transaction_id_asersi] = {
                "summary_id": summary_id,
                "is_anomalous": False,
                "anomaly_flags": [],
                "violation_details": {},
                "anomaly_datetime": datetime.now()
            }

    # --- Save Anomaly Results to Database ---
    for transaction_id_asersi, result_data in transaction_anomaly_results.items():
        existing_result = db.query(AnomalyResult).filter(
            AnomalyResult.execution_id == execution_id,
            AnomalyResult.transaction_id_asersi == transaction_id_asersi
        ).first()

        if not existing_result:
            actual_anomaly = AnomalyResult(
                execution_id=execution_id,
                summary_id=result_data['summary_id'],
                transaction_id_asersi=transaction_id_asersi,
                template_id=template_id, # Store template_id
                is_anomalous=result_data['is_anomalous'],
                anomaly_flags=result_data['anomaly_flags'],
                violation_details=result_data['violation_details'],
                anomaly_datetime=result_data['anomaly_datetime']
            )
            db.add(actual_anomaly)
            logger.info(f"Stored anomaly result for transaction_id_asersi {transaction_id_asersi}")
        else:
            existing_result.is_anomalous = result_data['is_anomalous']
            existing_result.anomaly_flags = result_data['anomaly_flags']
            existing_result.violation_details = result_data['violation_details']
            existing_result.anomaly_datetime = result_data['anomaly_datetime']
            existing_result.template_id = template_id # Update template_id
            logger.info(f"Updated existing anomaly result for transaction_id_asersi {transaction_id_asersi}")
    
    db.commit()
