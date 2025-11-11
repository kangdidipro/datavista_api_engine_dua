from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

# Anomaly Template Master
class AnomalyTemplateMasterBase(BaseModel):
    role_name: str
    description: Optional[str] = None
    is_default: Optional[bool] = False
    created_by: Optional[str] = None

class AnomalyTemplateMasterCreate(AnomalyTemplateMasterBase):
    pass

class AnomalyTemplateMaster(AnomalyTemplateMasterBase):
    template_id: int
    created_datetime: datetime
    last_modified: datetime
    
    transaction_criteria: Optional[List['TransactionAnomalyCriteria']] = []
    special_criteria: Optional[List['SpecialAnomalyCriteria']] = []
    video_parameters: Optional[List['VideoAiParameter']] = []
    accumulated_criteria: Optional[List['AccumulatedAnomalyCriteria']] = []

    class Config:
        orm_mode = True

# Transaction Anomaly Criteria
class TransactionAnomalyCriteriaBase(BaseModel):
    anomaly_type: str
    min_volume_liter: int
    plate_color: Optional[List[str]] = None
    consumer_type: str
    description: Optional[str] = None
    is_active: Optional[bool] = True

class TransactionAnomalyCriteriaCreate(TransactionAnomalyCriteriaBase):
    pass

class TransactionAnomalyCriteriaUpdate(TransactionAnomalyCriteriaBase):
    anomaly_type: Optional[str] = None
    min_volume_liter: Optional[int] = None
    consumer_type: Optional[str] = None

class TransactionAnomalyCriteria(TransactionAnomalyCriteriaBase):
    criteria_id: int

    class Config:
        orm_mode = True

# Special Anomaly Criteria
class SpecialAnomalyCriteriaBase(BaseModel):
    criteria_code: str
    criteria_name: str
    value: Optional[str] = None
    unit: Optional[str] = None
    violation_rule: str
    description: Optional[str] = None

class SpecialAnomalyCriteriaCreate(SpecialAnomalyCriteriaBase):
    pass

class SpecialAnomalyCriteriaUpdate(SpecialAnomalyCriteriaBase):
    criteria_code: Optional[str] = None
    criteria_name: Optional[str] = None
    violation_rule: Optional[str] = None

class SpecialAnomalyCriteria(SpecialAnomalyCriteriaBase):
    special_criteria_id: int

    class Config:
        orm_mode = True

# Video AI Parameters
class VideoAiParameterBase(BaseModel):
    parameter_key: str
    parameter_value: float
    unit: Optional[str] = None
    module_name: str
    user_modifiable: Optional[bool] = True
    last_modified_by: Optional[str] = None

class VideoAiParameterCreate(VideoAiParameterBase):
    pass

class VideoAiParameter(VideoAiParameterBase):
    param_id: int

    class Config:
        orm_mode = True

# Accumulated Anomaly Criteria
class AccumulatedAnomalyCriteriaBase(BaseModel):
    criteria_code: str
    criteria_name: str
    threshold_value: float
    time_window_hours: Optional[int] = 24
    group_by_field: str
    description: Optional[str] = None
    is_active: Optional[bool] = True

class AccumulatedAnomalyCriteriaCreate(AccumulatedAnomalyCriteriaBase):
    pass

class AccumulatedAnomalyCriteriaUpdate(AccumulatedAnomalyCriteriaBase):
    criteria_code: Optional[str] = None
    criteria_name: Optional[str] = None
    threshold_value: Optional[float] = None
    time_window_hours: Optional[int] = None
    group_by_field: Optional[str] = None

class AccumulatedAnomalyCriteria(AccumulatedAnomalyCriteriaBase):
    accumulated_criteria_id: int

    class Config:
        orm_mode = True

class AnomalyAnalysisRequest(BaseModel):
    template_id: Optional[int] = None
    transaction_criteria_ids: Optional[List[int]] = None
    special_criteria_ids: Optional[List[int]] = None
    summary_ids: List[int]
    executed_by: str

# Anomaly Execution
class AnomalyExecutionBase(BaseModel):
    execution_id: str
    template_id: int # Changed from Optional[int]
    executed_by: str
    status: str
    rules_applied: List[str]
    rules_config: Optional[dict] = None
    total_batches_processed: Optional[int] = 0

class AnomalyExecutionCreate(BaseModel): # No execution_id for create, it's auto-generated
    template_id: int
    executed_by: str
    rules_applied: List[str]
    rules_config: Optional[dict] = None
    total_batches_processed: Optional[int] = 0
    summary_ids: List[int] # Add this to be passed from frontend

class AnomalyExecution(AnomalyExecutionBase):
    execution_timestamp: datetime
    
    batches: Optional[List['AnomalyExecutionBatch']] = [] # Forward reference

    class Config:
        orm_mode = True

# Anomaly Execution Batch
class AnomalyExecutionBatchBase(BaseModel):
    execution_id: str
    summary_id: int
    batch_status: str
    anomalies_found: int
    p1_anomaly_value: Optional[str] = "NA"
    p2_anomaly_value: Optional[str] = "NA"
    p3_anomaly_value: Optional[str] = "NA"
    p4_anomaly_value: Optional[str] = "NA"
    p5_anomaly_value: Optional[str] = "NA"
    p6_anomaly_value: Optional[str] = "NA"

class AnomalyExecutionBatchCreate(AnomalyExecutionBatchBase):
    pass

class AnomalyExecutionBatch(AnomalyExecutionBatchBase):
    detail_id: int

    class Config:
        orm_mode = True

class CsvSummaryMasterDailyBase(BaseModel):
    import_datetime: datetime
    import_duration: Optional[float] = None
    file_name: Optional[str] = None
    title: Optional[str] = None
    total_records_inserted: Optional[int] = None
    total_records_read: Optional[int] = None
    total_volume: Optional[float] = None
    total_penjualan: Optional[str] = None
    total_operator: Optional[float] = None
    produk_jbt: Optional[str] = None
    produk_jbkt: Optional[str] = None
    total_volume_liter: Optional[float] = None
    total_penjualan_rupiah: Optional[str] = None
    total_mode_transaksi: Optional[str] = None
    total_plat_nomor: Optional[str] = None

class CsvSummaryMasterDaily(CsvSummaryMasterDailyBase):
    summary_id: int
    total_nik: Optional[str] = None
    total_sektor_non_kendaraan: Optional[str] = None
    total_jumlah_roda_kendaraan_4: Optional[str] = None
    total_jumlah_roda_kendaraan_6: Optional[str] = None
    total_kuota: Optional[float] = None
    total_warna_plat_kuning: Optional[str] = None
    total_warna_plat_hitam: Optional[str] = None
    total_warna_plat_merah: Optional[str] = None
    total_warna_plat_putih: Optional[str] = None
    total_mor: Optional[float] = None
    total_provinsi: Optional[float] = None
    total_kota_kabupaten: Optional[float] = None
    total_no_spbu: Optional[float] = None
    numeric_totals: Optional[dict] = None

    class Config:
        orm_mode = True

# Update forward refs
AnomalyTemplateMaster.update_forward_refs()
AnomalyExecution.update_forward_refs()
