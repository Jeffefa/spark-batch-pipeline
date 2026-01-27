import os
import shutil
from pyspark.sql import functions as F
from src.commons.logger_setup import setup_logger
from src.commons.config_loader import ConfigLoader
from src.commons.execution_tracker import ExecutionTracker
from src.core.spark_manager import SparkManager

class ODSIngestor:
    """
    Ingest data into ODS.
    This class is responsible for manager and idempotent ingestion of files into the ODS layer.
    """
    def __init__(self, spark: SparkManager, config: ConfigLoader, tracker: ExecutionTracker):
        self.spark = spark.get_session()
        self.config = config
        self.tracker = tracker
        self.logger = setup_logger("ODSIngestor")

        self.upload_path = self.config.get_path("UPLOADS")
        self.processed_path = self.config.get_path("PROCESSED_FILES")
    
    def run_ingestion(self):
        """
        Run the ingestion process.
        """
        try:    
            files = [f for f in os.listdir(self.upload_path) if f.endswith(".json")]
            if not files:
                self.logger.info("No files to process.")
                return
            
            for file in files:
                self._process_file(file)

        except Exception as e:
            self.logger.error(f"Error during ingestion: {str(e)}")
            raise

    def _process_file(self, file_name: str):
        """
        Check and process a single file.
        """
        try:
            parts = file_name.split("_")
            if len(parts) < 4:
                self.logger.warning(f"Filename {file_name} does not aligned with expected format. Skipping.")
                return
            
            company = parts[0]
            file_type = f"{parts[1]}_{parts[2]}"
            ref_date = parts[3].replace(".json", "")
            step_key = f"ODSIngest_{company}_{file_type}"

            if self.tracker.was_executed(step_key, ref_date):
                self.logger.info(f"File {file_name} already processed for date {ref_date}. Skipping.")
                return
            
            self.logger.info(f"Processing file {file_name} for date {ref_date}.")

            input_full_path = os.path.join(self.upload_path, file_name)

            df = (self.spark.read.option("multiline", "true").json(input_full_path)
                    .withColumn("ref_date", F.lit(ref_date))
                    .withColumn("company", F.lit(company)))

            ods_key = "ODS_DTBL" if "data_blocks"in file_type else "ODS_FMLT"
            output_path = self.config.get_path(ods_key)

            df.write.mode("append").partitionBy("ref_date").parquet(output_path)

            self.tracker.register_success(step_key, ref_date, file_name)
            shutil.move(input_full_path, os.path.join(self.processed_path, file_name))

            self.logger.info(f"Successfully processed and moved file {file_name}.")
        
        except Exception as e:
            self.logger.error(f"Error processing file {file_name}: {str(e)}")
