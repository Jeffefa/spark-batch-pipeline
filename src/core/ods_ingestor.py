import os
import shutil
import traceback
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
        self.logger = setup_logger("- ODSIngestor")

        self.shards = self.config.get_setting("SHARDS")
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
            
            df = self.spark.read \
                .option("multiline", "true") \
                .option("recursiveFileLookup", "true") \
                .json(self.upload_path)
            
            file_to_move = []
            
            for file in files:
                success = self._process_file(df, file)
                if success:
                    file_to_move.append(file)

            for file in file_to_move:
                input_full_path = os.path.join(self.upload_path, file)
                shutil.move(input_full_path, os.path.join(self.processed_path, file))
                
            self.logger.info(f"Successfully processed.")

        except Exception as e:
            self.logger.error(f"Error during ingestion: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise

    def _process_file(self, df, file_name: str):
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

            df_raw = df.filter(F.input_file_name().contains(file_name))
            
            df_ods = df_raw.withColumn("raw_data", F.struct("*"))
            df_ods = df_ods.withColumn("file_name", F.lit(file_name)) \
                           .withColumn("company_name", F.lit(company)) \
                           .withColumn("load_timestamp", F.current_timestamp())

            df_save_ods = df_ods.select("company_name", "file_name", "raw_data", "load_timestamp")

            ods_key = "ODS_DTBL" if "data_blocks"in file_type else "ODS_FMLT"
            base_path = self.config.get_path(ods_key)
            final_path = os.path.join(base_path, ref_date)

            df_save_ods.repartition(self.shards).write.mode("append").parquet(final_path)
            
            self.tracker.register_success(step_key, ref_date, file_name)
            return True
        
        except Exception as e:
            self.logger.error(f"Error processing file {file_name}: {str(e)}")
            return False
