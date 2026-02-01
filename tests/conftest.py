import pytest
from src.commons.config_loader import ConfigLoader
from src.core.spark_manager import SparkManager

@pytest.fixture
def init_config():
    """
    Docstring for init_config
    Initializes ConfigLoader and SparkManager for testing
    """
    config_loader = ConfigLoader(config_path="pipeline_config.json")
    spark_session = SparkManager(config_loader)
    
    yield config_loader, spark_session

    spark_session.stop_session()