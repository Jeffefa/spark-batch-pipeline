import os
import pytest
from src.commons.execution_tracker import ExecutionTracker
from src.commons.logger_setup import setup_logger

def test_config_loader_path_resolution(init_config):
    """
    Checks that ConfigLoader correctly resolves relative paths to absolute paths
    """
    loader, _ = init_config
    path = loader.get_path("UPLOADS")

    assert os.path.isabs(path)
    assert "workingArea/uploads"in path
    assert ".."not in path

def test_logger_creation():
    """
    Checks that the logger is created with the correct name and level
    """
    logger = setup_logger("TestLogger")
    assert logger.name == "TestLogger"
    assert logger.level == 20

def test_execution_tracker_flow(tmp_path, init_config):
    """
    Tests the ExecutionTracker's ability to register and check execution status
    """
    mock_config, spark_session = init_config
    mock_config.folders['CONTROL_FILE'] = str(tmp_path / "control.json")
    mock_config.folders['CONTROL'] = str(tmp_path / "control_parquet")
    mock_config.get_path = lambda key: mock_config.folders[key]

    step = "STAGE_TEST"
    ref_date = "20260101"
    fake_file = "companyX_data_blocks_20260101.json"

    tracker = ExecutionTracker(config=mock_config, spark_manager=spark_session )

    assert tracker.was_executed(step, ref_date) is False
    tracker.register_success(step, ref_date, fake_file)
    assert tracker.was_executed(step, ref_date) is True