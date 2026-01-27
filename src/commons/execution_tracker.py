import json
import os
from datetime import datetime
from src.core.spark_manager import SparkManager
from src.commons.config_loader import ConfigLoader
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

class ExecutionTracker:
    """
    Allows tracking the execution status of various steps in a data processing pipeline.
    """
    def __init__(self, config: ConfigLoader, spark_manager: SparkManager):
        self.control_file = config.get_path("CONTROL_FILE")
        self.control_path = config.get_path("CONTROL")
        self.shards = config.get_setting("SHARDS")
        self.spark = spark_manager.get_session()
        os.makedirs(os.path.dirname(self.control_file), exist_ok=True)

    def _get_history(self) -> dict:
        if os.path.exists(self.control_file):
            with open(self.control_file, 'r') as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    return {}
        return {}
    
    def was_executed(self, step_name: str, reference_date: str) -> bool:
        # Check if a step was executed successfully for a given reference date
        history = self._get_history()
        key = f"{reference_date}:{step_name}"
        return history.get(key) == "SUCCESS"
    
    def register_success(self, step_name: str, reference_date: str, file_name: str):
        # Register a successful execution of a step for a given reference date
        history = self._get_history()
        key = f"{reference_date}:{step_name}"
        history[key] = "SUCCESS"
        history[f"{key}_timestamp"] = datetime.now().isoformat()

        with open(self.control_file, 'w') as f:
            json.dump(history, f, indent=4)

        if self.spark:
            self._save_to_parquet(file_name, step_name, "SUCCESS")

    def _save_to_parquet(self, file_name, activity, status):
        schema = StructType([
            StructField("file_name", StringType(), True),
            StructField("activity", StringType(), True),
            StructField("status", StringType(), True),
            StructField("load_timestamp", TimestampType(), True)
        ])

        log_data = [(file_name, activity, status, datetime.now())]

        df_control = self.spark.createDataFrame(log_data, schema)
        df_control.repartition(self.shards).write.mode("append").parquet(self.control_path)