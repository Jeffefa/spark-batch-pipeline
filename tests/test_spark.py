from src.core.spark_manager import SparkManager
from src.commons.config_loader import ConfigLoader
import pytest

def test_spark_manager_initialization():
    """
    Checks that SparkManager initialize a Spark session.
    """
    config = ConfigLoader(config_path="pipeline_config.json")
    spark_manager = SparkManager(config)

    spark1 = spark_manager.get_session()
    spark2 = spark_manager.get_session()

    assert spark1 is spark2
    assert spark1.version is not None

    spark_manager.stop_session()