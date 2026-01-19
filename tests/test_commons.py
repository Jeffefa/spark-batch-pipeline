import os
import pytest
from src.commons.config_loader import ConfigLoader
from src.commons.execution_tracker import ExecutionTracker
from src.commons.logger_setup import setup_logger

def test_config_loader_path_resolution():
    """
    Checks that ConfigLoader correctly resolves relative paths to absolute paths
    """
    loader = ConfigLoader(config_path="pipeline_config.json")
    
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

def test_execution_tracker_flow(tmp_path):
    """
    Tests the ExecutionTracker's ability to register and check execution status
    """
    control_file = tmp_path / "control.json"
    tracker = ExecutionTracker(str(control_file))

    step = "STAGE_TEST"
    ref_date = "2026-01-01"

    assert tracker.was_executed(step, ref_date) is False
    tracker.register_success(step, ref_date)
    assert tracker.was_executed(step, ref_date) is True