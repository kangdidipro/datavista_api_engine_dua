from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, ForeignKey, Numeric, ARRAY, JSON, Time, BigInteger, TypeDecorator
)
from sqlalchemy.orm import relationship
from .base import Base # Import Base from models.base
from datetime import datetime
import json

class SQLiteARRAY(TypeDecorator):
    """Enables to store lists as comma separated strings in SQLite"""

    impl = String

    def process_bind_param(self, value, dialect):
        if value is not None:
            return json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            return json.loads(value)
        return value

class AnomalyTemplateMaster(Base):
    __tablename__ = 'anomaly_template_master'
    template_id = Column(Integer, primary_key=True, index=True)
    role_name = Column(String, unique=True, nullable=False)
    description = Column(String)
    is_default = Column(Boolean, default=False)
    created_datetime = Column(DateTime, default=datetime.utcnow)
    last_modified = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by = Column(String)

    # Relationships
    transaction_criteria = relationship("TransactionAnomalyCriteria", secondary="template_criteria_volume", back_populates="templates")
    special_criteria = relationship("SpecialAnomalyCriteria", secondary="template_criteria_special", back_populates="templates")
    video_parameters = relationship("VideoAiParameter", secondary="template_criteria_video", back_populates="templates")
    accumulated_criteria = relationship("AccumulatedAnomalyCriteria", secondary="template_criteria_accumulated", back_populates="templates")

class TransactionAnomalyCriteria(Base):
    __tablename__ = 'transaction_anomaly_criteria'
    criteria_id = Column(Integer, primary_key=True, index=True)
    anomaly_type = Column(String, nullable=False)
    min_volume_liter = Column(Integer, nullable=False)
    plate_color = Column(SQLiteARRAY)
    consumer_type = Column(String, nullable=False)
    description = Column(String)
    is_active = Column(Boolean, default=True)
    templates = relationship("AnomalyTemplateMaster", secondary="template_criteria_volume", back_populates="transaction_criteria")

class SpecialAnomalyCriteria(Base):
    __tablename__ = 'special_anomaly_criteria'
    special_criteria_id = Column(Integer, primary_key=True, index=True)
    criteria_code = Column(String, unique=True, nullable=False)
    criteria_name = Column(String, nullable=False)
    value = Column(String)
    unit = Column(String)
    violation_rule = Column(String, nullable=False)
    description = Column(String)
    templates = relationship("AnomalyTemplateMaster", secondary="template_criteria_special", back_populates="special_criteria")

class VideoAiParameter(Base):
    __tablename__ = 'video_ai_parameters'
    param_id = Column(Integer, primary_key=True, index=True)
    parameter_key = Column(String, unique=True, nullable=False)
    parameter_value = Column(Numeric(10, 3), nullable=False)
    unit = Column(String)
    module_name = Column(String, nullable=False)
    user_modifiable = Column(Boolean, default=True)
    last_modified_by = Column(String)
    templates = relationship("AnomalyTemplateMaster", secondary="template_criteria_video", back_populates="video_parameters")

# Linker Tables
class TemplateCriteriaVolume(Base):
    __tablename__ = 'template_criteria_volume'
    id = Column(Integer, primary_key=True)
    template_id = Column(Integer, ForeignKey('anomaly_template_master.template_id'))
    criteria_id = Column(Integer, ForeignKey('transaction_anomaly_criteria.criteria_id'))

class TemplateCriteriaSpecial(Base):
    __tablename__ = 'template_criteria_special'
    id = Column(Integer, primary_key=True)
    template_id = Column(Integer, ForeignKey('anomaly_template_master.template_id'))
    special_criteria_id = Column(Integer, ForeignKey('special_anomaly_criteria.special_criteria_id'))

class TemplateCriteriaVideo(Base):
    __tablename__ = 'template_criteria_video'
    id = Column(Integer, primary_key=True)
    template_id = Column(Integer, ForeignKey('anomaly_template_master.template_id'))
    param_id = Column(Integer, ForeignKey('video_ai_parameters.param_id'))



class AccumulatedAnomalyCriteria(Base):
    __tablename__ = 'accumulated_anomaly_criteria'
    accumulated_criteria_id = Column(Integer, primary_key=True, index=True)
    criteria_code = Column(String, unique=True, nullable=False) # e.g., 'MAX_DAILY_VOLUME'
    criteria_name = Column(String, nullable=False)
    threshold_value = Column(Numeric(20, 3), nullable=False) # e.g., max volume
    time_window_hours = Column(Integer, default=24) # e.g., for daily accumulation
    group_by_field = Column(String, nullable=False) # e.g., 'plat_nomor', 'nik'
    description = Column(String)
    is_active = Column(Boolean, default=True)
    templates = relationship("AnomalyTemplateMaster", secondary="template_criteria_accumulated", back_populates="accumulated_criteria")

class TemplateCriteriaAccumulated(Base):
    __tablename__ = 'template_criteria_accumulated'
    id = Column(Integer, primary_key=True)
    template_id = Column(Integer, ForeignKey('anomaly_template_master.template_id'))
    accumulated_criteria_id = Column(Integer, ForeignKey('accumulated_anomaly_criteria.accumulated_criteria_id'))

class AnomalyResult(Base):
    __tablename__ = 'anomaly_results'
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    execution_id = Column(String(50), ForeignKey('anomaly_executions.execution_id', ondelete="CASCADE"), nullable=False)
    transaction_id_asersi = Column(String)
    summary_id = Column(Integer, ForeignKey('csv_summary_master_daily.summary_id', ondelete="CASCADE"))
    template_id = Column(Integer, ForeignKey('anomaly_template_master.template_id'))
    criteria_id_violated = Column(Integer, ForeignKey('transaction_anomaly_criteria.criteria_id'))
    special_criteria_id_violated = Column(Integer, ForeignKey('special_anomaly_criteria.special_criteria_id'))
    accumulated_criteria_id_violated = Column(Integer, ForeignKey('accumulated_anomaly_criteria.accumulated_criteria_id'))
    anomaly_datetime = Column(DateTime)
    anomaly_type = Column(String)
    violation_value = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    execution = relationship("AnomalyExecution")
    summary = relationship("CsvSummaryMasterDaily")
    template = relationship("AnomalyTemplateMaster")
    transaction_criteria = relationship("TransactionAnomalyCriteria")
    special_criteria = relationship("SpecialAnomalyCriteria")
    accumulated_criteria = relationship("AccumulatedAnomalyCriteria")

class CsvSummaryMasterDaily(Base):
    __tablename__ = 'csv_summary_master_daily'
    summary_id = Column(Integer, primary_key=True, index=True, autoincrement=True) # Changed to BigInteger
    import_datetime = Column(DateTime, nullable=False)
    import_duration = Column(Numeric(20, 3))
    file_name = Column(String) # Removed length constraint
    title = Column(String) # Removed length constraint
    total_records_inserted = Column(Integer)
    total_records_read = Column(Integer)
    total_volume = Column(Numeric(20, 3))
    total_penjualan = Column(String) # Removed length constraint
    total_operator = Column(Numeric(20, 3))
    produk_jbt = Column(String) # Removed length constraint
    produk_jbkt = Column(String) # Removed length constraint
    total_volume_liter = Column(Numeric(10, 3))
    total_penjualan_rupiah = Column(String) # Removed length constraint
    total_mode_transaksi = Column(String) # Removed length constraint
    total_plat_nomor = Column(String) # Removed length constraint
    total_nik = Column(String) # Removed length constraint
    total_sektor_non_kendaraan = Column(String) # Removed length constraint
    total_jumlah_roda_kendaraan_4 = Column(String) # Removed length constraint
    total_jumlah_roda_kendaraan_6 = Column(String) # Removed length constraint
    total_kuota = Column(Numeric(10, 1))
    total_warna_plat_kuning = Column(String) # Removed length constraint
    total_warna_plat_hitam = Column(String) # Removed length constraint
    total_warna_plat_merah = Column(String) # Removed length constraint
    total_warna_plat_putih = Column(String) # Removed length constraint
    total_mor = Column(Numeric(20, 3))
    total_provinsi = Column(Numeric(20, 3))
    total_kota_kabupaten = Column(Numeric(20, 3))
    total_no_spbu = Column(Numeric(20, 3))
    numeric_totals = Column(JSON)

class CsvImportLog(Base):
    __tablename__ = 'csv_import_log'
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    transaction_id_asersi = Column(String(50), unique=True, nullable=False)
    tanggal = Column(String, nullable=False)
    jam = Column(String, nullable=False)
    mor = Column(String) # Corrected to String
    provinsi = Column(String) # Removed length constraint
    kota_kabupaten = Column(String) # Removed length constraint
    no_spbu = Column(String) # Removed length constraint
    no_nozzle = Column(String) # Removed length constraint
    no_dispenser = Column(String) # Removed length constraint
    produk = Column(String) # Removed length constraint
    volume_liter = Column(Numeric(10, 3))
    penjualan_rupiah = Column(Numeric(15, 2))
    operator = Column(String) # Removed length constraint
    mode_transaksi = Column(String) # Removed length constraint
    plat_nomor = Column(String) # Removed length constraint
    nik = Column(String) # Removed length constraint
    sektor_non_kendaraan = Column(String) # Removed length constraint
    jumlah_roda_kendaraan = Column(String) # Removed length constraint
    kuota = Column(String) # Corrected to String
    warna_plat = Column(String) # Removed length constraint
    daily_summary_id = Column(BigInteger, ForeignKey('csv_summary_master_daily.summary_id'))
    import_attempt_count = Column(Integer, default=1)
    batch_original_duplicate_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    summary = relationship("CsvSummaryMasterDaily")

class TabelMor(Base):
    __tablename__ = 'tabel_mor'
    mor_id = Column(Integer, primary_key=True, index=True)
    mor = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class AnomalyExecution(Base):
    __tablename__ = 'anomaly_executions'
    execution_id = Column(String(50), primary_key=True)
    template_id = Column(Integer, ForeignKey('anomaly_template_master.template_id'), nullable=False)
    execution_timestamp = Column(DateTime(timezone=True), nullable=False, default=datetime.now)
    executed_by = Column(String(50), nullable=False)
    status = Column(String(20), nullable=False)
    rules_applied = Column(SQLiteARRAY, nullable=False)
    rules_config = Column(JSON)
    total_batches_processed = Column(Integer, default=0)

    template = relationship("AnomalyTemplateMaster")
    batches = relationship("AnomalyExecutionBatch", back_populates="execution")

class AnomalyExecutionBatch(Base):
    __tablename__ = 'anomaly_execution_batches'
    detail_id = Column(Integer, primary_key=True, autoincrement=True)
    execution_id = Column(String(50), ForeignKey('anomaly_executions.execution_id', ondelete="CASCADE"), nullable=False)
    summary_id = Column(Integer, ForeignKey('csv_summary_master_daily.summary_id', ondelete="CASCADE"), nullable=False)
    batch_status = Column(String(50))
    anomalies_found = Column(Integer, default=0)
    # Kolom untuk menyimpan nilai anomali P1-P6
    p1_anomaly_value = Column(String, default="NA")
    p2_anomaly_value = Column(String, default="NA")
    p3_anomaly_value = Column(String, default="NA")
    p4_anomaly_value = Column(String, default="NA")
    p5_anomaly_value = Column(String, default="NA")
    p6_anomaly_value = Column(String, default="NA")

    execution = relationship("AnomalyExecution", back_populates="execution")
    summary = relationship("CsvSummaryMasterDaily")

