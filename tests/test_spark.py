import pytest

def test_spark_manager_initialization(init_config):
    """
    Checks that SparkManager initialize a Spark session.
    """
    _, spark_manager = init_config

    spark1 = spark_manager.get_session()
    spark2 = spark_manager.get_session()

    assert spark1 is spark2
    assert spark1.version is not None

    spark_manager.stop_session()