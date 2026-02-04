import os 
import sys
from datetime import datetime
from src.commons.config_loader import ConfigLoader
from src.commons.logger_setup import setup_logger
from src.commons.execution_tracker import ExecutionTracker
from src.core.spark_manager import SparkManager
from src.core.ods_ingestor import ODSIngestor
from src.core.dw_ingestor import DWIngestor

def main():
    """
    Main function to orchestrate the ETL pipeline.
    1. Load configuration.
    3. Initialize session.
    4. Execute ODS ingestion step.
    5. Execute DW ingestion steps.
    6. Execute APP ingestion steps.
    7. Handle exceptions and ensure Spark session is stopped.
    """
    config = ConfigLoader(config_path="pipeline_config.json")
    logger = setup_logger("Orchestrator")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    logger.info(f"Starting Pipeline Jeff v2 [{timestamp}]")

    try:
        spark_manager = SparkManager(config)
        tracker = ExecutionTracker(config, spark_manager)

        logger.info("Starting ODS Ingestion Step")
        ods = ODSIngestor(spark_manager, config, tracker)
        ods.run_ingestion()

        logger.info("Starting DW Ingestion Step")
        dw = DWIngestor(spark_manager, config, tracker)

        dirpath = config.get_path("ODS_DTBL")
        ref_date_list_dtbl = [s.name for s in os.scandir(dirpath) if s.is_dir() and not s.name.startswith('.')]
        dw.run_ingestion('DataBlocks', sorted(ref_date_list_dtbl))

        dirpath = config.get_path("ODS_FMLT")
        ref_date_list_fmlt = [s.name for s in os.scandir(dirpath) if s.is_dir() and not s.name.startswith('.')]
        dw.run_ingestion('FamilyTree', sorted(ref_date_list_fmlt))

        # logger.info("Starting APP ingestion Step")
        # app = APPIngestor(spark_manager, config, tracker)
        # app.run_ingestion()

        logger.info(f"Pipeline Completed Successfully [{datetime.now().strftime('%Y%m%d_%H%M%S')}]")
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)

    finally:
        if 'spark_manager' in locals():
            spark_manager.stop_session()

if __name__ == "__main__":
    main()