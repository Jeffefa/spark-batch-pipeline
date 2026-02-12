import pytest
import os
import json
from datetime import datetime
from pyspark.sql import Row
import pyspark.sql.functions as f
from src.core.dw_ingestor import DWIngestor
from src.commons.execution_tracker import ExecutionTracker

@pytest.fixture
def dw_ingestor(init_config):
    config, spark_manager = init_config
    tracker = ExecutionTracker(config, spark_manager)

    ingestor = DWIngestor(spark_manager, config, tracker)
    return config, spark_manager.get_session(), ingestor

def test_initial_validation_logic(dw_ingestor):
    """
    Test the initial validation logic to ensure that records with null mandatory fields are quarantined and valid records are passed through.
    """
    _, spark, dw_ingestor = dw_ingestor

    mockdata = [
        Row(raw_data=Row(duns="12345", company="Jeff Corp")),
        Row(raw_data=Row(duns=None, company="Bad Corp"))
    ]
    df = spark.createDataFrame(mockdata)

    dw_ingestor._mandatory_fields = lambda x: ["duns"]

    df_valid, df_quarantine = dw_ingestor._initial_validation(df, "any_schema")

    assert df_valid.count() == 1
    assert df_quarantine.count() == 1
    assert df_valid.collect()[0]["raw_data"]["company"] == "Jeff Corp"

def test_get_whitelist_projection(dw_ingestor):
    """
    Validate if the _get_whitelist method correctly projects the DataFrame based on the whitelist configuration, renaming fields as necessary.
    """
    _, spark, dw_ingestor = dw_ingestor

    mockdata = [Row(load_timestamp=datetime.now(), raw_data=Row(duns="123", taxId="999"))]
    df = spark.createDataFrame(mockdata)

    dw_ingestor._whitelist_json = lambda x, y: [
        {"path": "duns", "alias": "company_duns"},
        {"path": "taxId", "alias": "tax_identifier"}
    ]

    df_result = dw_ingestor._get_whitelist(df, "tabela", "whitelist")

    assert "company_duns" in df_result.columns
    assert "tax_identifier" in df_result.columns
    assert "raw_data" in df_result.columns

def test_save_with_explode_transformation(dw_ingestor, tmp_path):
    """
    Test the transformation logic of 1..N.
    This should create multiple rows for records that have array fields
    """
    _, spark, dw_ingestor = dw_ingestor

    mockdata = [Row(load_timestamp=datetime.now(), 
                raw_data=Row(duns="123", 
                             industryCodes=[
                                 Row(code="A", desc="Tech"), 
                                 Row(code="B", desc="Cloud")
                             ]))]
    df = spark.createDataFrame(mockdata)
    
    output_path = str(tmp_path / "dw_industry")

    dw_ingestor._whitelist_json = lambda x, y: [
        {"path": "duns", "alias": "duns"},
        {"path": "industryCodes.code", "alias": "industry_code"}
    ]

    dw_ingestor._save_with_explode("dw_industry", df, "wlist", output_path, "industryCodes")

    df_result = spark.read.parquet(output_path)

    assert df_result.count() == 2
    assert "industry_code" in df_result.columns

    codes = [row.industry_code for row in df_result.select("industry_code").collect()]
    assert "A" in codes and "B" in codes