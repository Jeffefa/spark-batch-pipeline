import pytest
import os
import json
from src.commons.config_loader import ConfigLoader
from src.core.spark_manager import SparkManager
from src.commons.execution_tracker import ExecutionTracker
from src.core.ods_ingestor import ODSIngestor

def test_full_ods_ingestion_flow(tmp_path):
    """
    Validates the complete ODS ingestion process using a test configuration
    """
    uploads_dir = tmp_path / "UPLOADS"
    processed_dir = tmp_path / "PROCESSED"
    control_dir = tmp_path / "CONTROL"
    ods_dir = tmp_path / "ODS"

    for d in [uploads_dir, processed_dir, control_dir, ods_dir]:
        d.mkdir()
    
    test_file = uploads_dir / "companyX_data_blocks_20260101.json"
    test_data = {"id": 1, "name": "Test Company"}
    with open(test_file, "w") as f:
        json.dump(test_data, f)
    
    config = ConfigLoader("pipeline_config.json")
    config.folders['UPLOADS'] = str(uploads_dir)
    config.folders['PROCESSED_FILES'] = str(processed_dir)
    config.folders['ODS_DTBL'] = str(ods_dir)
    config.folders['ODS_FMLT'] = str(ods_dir)
    config.folders['CONTROL'] = str(control_dir)
    config.folders['CONTROL_FILE'] = str(control_dir/"control_tracker.json")

    config.get_path = lambda key: config.folders[key]

    spark_manager = SparkManager(config)
    tracker = ExecutionTracker(config, spark_manager)
    ods_ingestor = ODSIngestor(spark_manager, config, tracker)

    ods_ingestor.run_ingestion()

    assert not os.path.exists(test_file)
    assert os.path.exists(processed_dir / "companyX_data_blocks_20260101.json")

    assert len(os.listdir(ods_dir)) > 0

    assert tracker.was_executed("ODSIngest_companyX_data_blocks", "20260101") is True

    spark_manager.stop_session()