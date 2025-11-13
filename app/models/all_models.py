from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, JSON, Float, ForeignKey, Numeric, PrimaryKeyConstraint, BigInteger # Import Numeric, PrimaryKeyConstraint, BigInteger
from sqlalchemy.orm import relationship
from sqlalchemy.types import TypeDecorator # Import TypeDecorator
from ..db_base import Base # Import Base from app.db_base
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

# --- CsvSummaryMasterDaily (moved from where it was a duplicate definition) ---
class CsvSummaryMasterDaily(Base):
    __tablename__ = 'csv_summary_master_daily'
    summary_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    import_datetime = Column(DateTime, nullable=False)
    import_duration = Column(Numeric(20, 3))
    file_name = Column(String)
    title = Column(String)
    file_type = Column(String)
    total_records_inserted = Column(Integer)
    total_records_read = Column(Integer)
    total_volume = Column(Numeric(20, 3))
    total_penjualan = Column(String)
    total_operator = Column(Numeric(20, 3))
    produk_jbt = Column(String)
    produk_jbkt = Column(String)
    total_volume_liter = Column(Numeric(10, 3))
    total_penjualan_rupiah = Column(String)
    total_mode_transaksi = Column(String)
    total_plat_nomor = Column(String)
    total_nik = Column(String)
    sektor_non_kendaraan = Column(String)
    total_jumlah_roda_kendaraan_4 = Column(String)
    total_jumlah_roda_kendaraan_6 = Column(String)
    total_kuota = Column(Numeric(10, 1))
    total_warna_plat_kuning = Column(String)
    total_warna_plat_hitam = Column(String)
    total_warna_plat_merah = Column(String)
    total_warna_plat_putih = Column(String)
    total_mor = Column(Numeric(20, 3))
    total_provinsi = Column(Numeric(20, 3))
    total_kota_kabupaten = Column(Numeric(20, 3))
    total_no_spbu = Column(Numeric(20, 3))
    numeric_totals = Column(JSON)

    logs = relationship("CsvImportLog", back_populates="summary")

# --- Anomaly Models and Linker Tables ---
class AnomalyTemplateMaster(Base):
    __tablename__ = 'anomaly_template_master'
    template_id = Column(Integer, primary_key=True, index=True)
    role_name = Column(String, unique=True, nullable=False)
    description = Column(String)
    is_default = Column(Boolean, default=False)
    created_datetime = Column(DateTime, default=datetime.utcnow)
    last_modified = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by = Column(String)

    # Relationships (without back_populates/backref for now)
    transaction_criteria = relationship("TransactionAnomalyCriteria", secondary="template_criteria_volume", back_populates="templates")
    special_criteria = relationship("SpecialAnomalyCriteria", secondary="template_criteria_special", back_populates="templates")
    video_parameters = relationship("VideoAiParameter", secondary="template_criteria_video", back_populates="templates")
    accumulated_criteria = relationship("AccumulatedAnomalyCriteria", secondary="template_criteria_accumulated", back_populates="templates")
    results = relationship("AnomalyResult", back_populates="template")
    executions = relationship("AnomalyExecution", back_populates="template")

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
    criteria_code = Column(String, unique=True, nullable=False)
    criteria_name = Column(String, nullable=False)
    threshold_value = Column(Numeric(20, 3), nullable=False)
    time_window_hours = Column(Integer, default=24)
    group_by_field = Column(String, nullable=False)
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
    __table_args__ = (
        PrimaryKeyConstraint('execution_id', 'transaction_id_asersi'),
    )
    execution_id = Column(String(50), ForeignKey('anomaly_executions.execution_id', ondelete="CASCADE"), nullable=False)
    transaction_id_asersi = Column(String(50), nullable=False)
    summary_id = Column(Integer, nullable=False)
    template_id = Column(Integer, ForeignKey('anomaly_template_master.template_id'))
    
    is_anomalous = Column(Boolean, default=False)
    anomaly_flags = Column(SQLiteARRAY, default=[])
    violation_details = Column(JSON, default={})

    anomaly_datetime = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    execution = relationship("AnomalyExecution", back_populates="results")
    template = relationship("AnomalyTemplateMaster", back_populates="results")

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

    results = relationship("AnomalyResult", back_populates="execution")

    template = relationship("AnomalyTemplateMaster", back_populates="executions")
    batches = relationship("AnomalyExecutionBatch", back_populates="execution")

class AnomalyExecutionBatch(Base):
    __tablename__ = 'anomaly_execution_batches'
    detail_id = Column(Integer, primary_key=True, autoincrement=True)
    execution_id = Column(String(50), ForeignKey('anomaly_executions.execution_id', ondelete="CASCADE"), nullable=False)
    summary_id = Column(Integer, ForeignKey('csv_summary_master_daily.summary_id', ondelete="CASCADE"), nullable=False)
    batch_status = Column(String(50))
    anomalies_found = Column(Integer, default=0)
    p1_anomaly_value = Column(String, default="NA")
    p2_anomaly_value = Column(String, default="NA")
    p3_anomaly_value = Column(String, default="NA")
    p4_anomaly_value = Column(String, default="NA")
    p5_anomaly_value = Column(String, default="NA")
    p6_anomaly_value = Column(String, default="NA")

    execution = relationship("AnomalyExecution", back_populates="batches")

class CsvImportLog(Base):
    __tablename__ = 'csv_import_log'
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    transaction_id_asersi = Column(String(50), unique=True, nullable=False)
    tanggal = Column(String, nullable=False)
    jam = Column(String, nullable=False)
    mor = Column(String)
    provinsi = Column(String)
    kota_kabupaten = Column(String)
    no_spbu = Column(String)
    no_nozzle = Column(String)
    no_dispenser = Column(String)
    produk = Column(String)
    volume_liter = Column(Numeric(10, 3))
    penjualan_rupiah = Column(Numeric(15, 2))
    operator = Column(String)
    mode_transaksi = Column(String)
    plat_nomor = Column(String)
    nik = Column(String)
    sektor_non_kendaraan = Column(String)
    jumlah_roda_kendaraan = Column(String)
    kuota = Column(String)
    warna_plat = Column(String)
    daily_summary_id = Column(BigInteger, ForeignKey('csv_summary_master_daily.summary_id'))
    import_attempt_count = Column(Integer, default=1)
    batch_original_duplicate_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    summary = relationship("CsvSummaryMasterDaily", back_populates="logs")

class TabelMor(Base):
    __tablename__ = 'tabel_mor'
    mor_id = Column(Integer, primary_key=True, index=True)
    mor = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)