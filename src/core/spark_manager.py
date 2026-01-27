from pyspark.sql import SparkSession
from src.commons.logger_setup import setup_logger
from src.commons.config_loader import ConfigLoader

class SparkManager:
    """
    Manages the Spark session for the application.
    Centralizes Spark creation and configuration of the engine.
    """
    def __init__(self, config: ConfigLoader):
        self.config = config
        self.logger = setup_logger("SparkManager")
        self.app_name = self.config.get_setting("SPARK_BATCH_APP") or "Pipeline_Jeffv2"
        self._spark = None
    
    def get_session(self) -> SparkSession:
        """
        Returns the Spark session, creating it if it doesn't exist.
        """
        if not self._spark:
            self.logger.info(f"Creating Spark session with app name: {self.app_name}")

            shards = self.config.get_setting("SHARDS") or 2

            self._spark = (SparkSession.builder
                           .appName(self.app_name)
                           .config("spark.sql.shuffle.partitions", str(shards))
                           .config("spark.sql.adaptive.enabled", "true")
                           .getOrCreate())
            
            self.logger.info("Spark session created successfully.")
        return self._spark
    
    def stop_session(self):
        """
        Stops the Spark session if it exists.
        """
        if self._spark:
            self.logger.info("Stopping Spark session.")
            self._spark.stop()
            self._spark = None
            self.logger.info("Spark session stopped.")