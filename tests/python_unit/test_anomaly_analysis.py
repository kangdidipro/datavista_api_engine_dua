import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from unittest.mock import MagicMock, patch
from datetime import datetime, time

from models.base import Base
from models import models
from crud import analysis_crud, anomaly_execution_crud, anomaly_crud

# Setup in-memory SQLite for testing
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db" # Use a file-based SQLite for easier debugging if needed
# For in-memory: SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@pytest.fixture(scope="function")
def db_session():
    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()
        Base.metadata.drop_all(bind=engine)

@pytest.fixture(scope="function")
def mock_rq_job():
    with patch('crud.analysis_crud.get_current_job') as mock_get_current_job:
        mock_job = MagicMock()
        mock_job.meta = {}
        mock_get_current_job.return_value = mock_job
        yield mock_job

def test_p1_red_plate_anomaly_detection(db_session, mock_rq_job):
    db = db_session

    # 1. Create SpecialAnomalyCriteria for RED_PLATE (P1)
    red_plate_criteria = models.SpecialAnomalyCriteria(
        criteria_code='RED_PLATE',
        criteria_name='Plat Merah Dilarang',
        violation_rule='warna_plat == "MERAH"',
        description='Deteksi kendaraan berplat merah'
    )
    db.add(red_plate_criteria)
    db.commit()
    db.refresh(red_plate_criteria)

    # 2. Create AnomalyTemplateMaster and link RED_PLATE criteria
    template = models.AnomalyTemplateMaster(
        role_name='Test Template P1',
        description='Template for P1 Red Plate Anomaly',
        created_by='test_user'
    )
    db.add(template)
    db.commit()
    db.refresh(template)

    # Link the special criteria to the template
    template_special_link = models.TemplateCriteriaSpecial(
        template_id=template.template_id,
        special_criteria_id=red_plate_criteria.special_criteria_id
    )
    db.add(template_special_link)
    db.commit()
    db.refresh(template) # Refresh template to load relationships

    # 3. Create CsvSummaryMasterDaily
    summary = models.CsvSummaryMasterDaily(
        import_datetime=datetime.now(),
        file_name='test_p1.csv',
        total_records_inserted=3,
        total_volume=100.0
    )
    db.add(summary)
    db.commit()
    db.refresh(summary)

    # 4. Create CsvImportLog entries
    # Anomaly case: Red Plate
    anomaly_log_red_plate = models.CsvImportLog(
        transaction_id_asersi='TX001',
        tanggal='2025-11-09',
        jam='10:00:00',
        warna_plat='MERAH',
        daily_summary_id=summary.summary_id,
        volume_liter=10.0
    )
    # Normal case: Black Plate
    normal_log_black_plate = models.CsvImportLog(
        transaction_id_asersi='TX002',
        tanggal='2025-11-09',
        jam='10:05:00',
        warna_plat='HITAM',
        daily_summary_id=summary.summary_id,
        volume_liter=15.0
    )
    # Normal case: White Plate
    normal_log_white_plate = models.CsvImportLog(
        transaction_id_asersi='TX003',
        tanggal='2025-11-09',
        jam='10:10:00',
        warna_plat='PUTIH',
        daily_summary_id=summary.summary_id,
        volume_liter=20.0
    )
    db.add_all([anomaly_log_red_plate, normal_log_black_plate, normal_log_white_plate])
    db.commit()
    db.refresh(anomaly_log_red_plate)
    db.refresh(normal_log_black_plate)
    db.refresh(normal_log_white_plate)

    # 5. Create AnomalyExecution record
    execution = models.AnomalyExecution(
        execution_id='EXEC001',
        template_id=template.template_id,
        execution_timestamp=datetime.now(),
        executed_by='test_user',
        status='QUEUED',
        rules_applied=['SC_RED_PLATE'],
        rules_config={}
    )
    db.add(execution)
    db.commit()
    db.refresh(execution)

    # 6. Run the anomaly analysis
    analysis_crud.run_anomaly_analysis(execution.execution_id, [summary.summary_id], db)

    # 7. Assertions
    anomalies = db.query(models.AnomalyResult).all()
    assert len(anomalies) == 1

    p1_anomaly = anomalies[0]
    assert p1_anomaly.transaction_id_asersi == 'TX001'
    assert p1_anomaly.special_criteria_id_violated == red_plate_criteria.special_criteria_id
    assert p1_anomaly.anomaly_type == 'RED_PLATE'
    assert p1_anomaly.violation_value == 'MERAH'

    # Verify execution status
    updated_execution = db.query(models.AnomalyExecution).filter_by(execution_id='EXEC001').first()
    assert updated_execution.status == 'COMPLETED'
    assert updated_execution.total_batches_processed == 1

    # Verify batch status
    batch = db.query(models.AnomalyExecutionBatch).filter_by(execution_id='EXEC001', summary_id=summary.summary_id).first()
    assert batch.batch_status == 'COMPLETED'
    assert batch.anomalies_found == 1
