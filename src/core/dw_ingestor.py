import os
import json
from datetime import datetime
import traceback
from pyspark.sql import functions as f
from src.commons.logger_setup import setup_logger
from src.commons.config_loader import ConfigLoader
from src.commons.execution_tracker import ExecutionTracker
from src.core.spark_manager import SparkManager

class DWIngestor:
    """
    Ingest data into Data Warehouse.
    This class is responsible for ingestion from ODS to DW layer.
    """
    def __init__(self, spark: SparkManager, config: ConfigLoader, tracker: ExecutionTracker):
        self.spark = spark.get_session()
        self.config = config
        self.tracker = tracker
        self.logger = setup_logger("- DWIngestor")

        self.mandatory_config = self._load_json_utils("mandatory_fields.json")
        self.whitelist_config = self._load_json_utils("whitelist.json")

        self.shards = self.config.get_setting("SHARDS")
        self.quarantine = self.config.get_path("QUARANTINE")
        self.ods_dtbl =  self.config.get_path("ODS_DTBL")
        self.ods_fmlt = self.config.get_path("ODS_FMLT")
        self.dw_company_data = self.config.get_path("DW_COMPANY_DATA")
        self.dw_industry_code = self.config.get_path("DW_INDUSTRY_CODE")
        self.dw_stock_exchanges = self.config.get_path("DW_STOCK_EXCHANGES")
        self.dw_family_tree = self.config.get_path("DW_FAMILY_TREE")
        self.dw_family_members = self.config.get_path("DW_FAMILY_MEMBERS")
    
    def _load_json_utils(self, file_name):
        """
        Get the json load from file
        """
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        full_path = os.path.join(base_path, "utils", file_name)
        with open(full_path, 'r') as f:
            return json.load(f)
    
    def _save_quarantine(self, df, reason):
        df_quarantine = df.withColumn("quarantine_reason", f.lit(reason)) \
            .withColumn("load_timestamp", f.lit(datetime.now()))

        df_quarantine.repartition(self.shards).write.mode("append").parquet(self.quarantine)

    def _initial_validation(self, df, mandatory_fields: str):
        self.logger.info("Validating mandatory fields...")
        mfield = self._mandatory_fields(mandatory_fields)
        base_columns = df.select("raw_data.*").schema.fieldNames()
        condition = None
        
        for field in mfield:
            if field in base_columns:
                base_condition = f.col(f"raw_data.{field}").isNotNull()
            else:
                base_condition = f.lit(False)

            if condition is None:
                condition = base_condition
            else:
                condition = condition & base_condition
        
        df_valid = df.filter(condition)
        df_quarantine = df.filter(~condition)
        
        return df_valid, df_quarantine

    def _get_whitelist(self, df, table, whitelist):
        output_cols = [
            f.col("load_timestamp"),
            f.col("raw_data")
        ]
        for field in self._whitelist_json(whitelist, table):
            fPath = field['path']
            fAlias = field['alias']
            
            output_cols.append(f.col(f"raw_data.{fPath}").alias(fAlias))

        return df.select(output_cols)
    
    def _mandatory_fields(self, mandatory_fields: str):
        return self.mandatory_config.get(mandatory_fields, [])

    def _whitelist_json(self, whitelist: str, table: str):
        return self.whitelist_config.get(whitelist, {}).get(table, [])

    def _save_without_explode(self, table, df, whitelist, path):
        df_dw = self._get_whitelist(df, table, whitelist)
        df_save_dw = df_dw.drop("raw_data")

        try:
            df_save_dw.repartition(self.shards).write.mode("append").parquet(path)
        except Exception as e:
            self.logger.error(f"Error processing table {table}: {str(e)}")

    def _save_with_explode(self, table, df, whitelist, path, explode_field):
        whitelist_fields = self._whitelist_json(whitelist, table)
        
        df_dw = self._get_whitelist(df, table, whitelist)
        df_exploded = df_dw.withColumn("exploded_item", f.explode(f"raw_data.{explode_field}"))

        final_cols = [f.col("load_timestamp")]
        for field in whitelist_fields:
            fPath = field['path']
            fAlias = field['alias']

            if fPath.startswith(explode_field):
                inner_path = fPath.replace(f"{explode_field}.", "")
                final_cols.append(f.col(f"exploded_item.{inner_path}").alias(fAlias))
            else:
                final_cols.append(f.col(fAlias))
            
        df_save_dw = df_exploded.select(final_cols)
        try:
            df_save_dw.repartition(self.shards).write.mode("append").parquet(path)
        except Exception as e:
            self.logger.error(f"Error processing table {table}: {str(e)}")

    def run_ingestion(self, data_type: str, ref_dates_list: list):
        """
        Execute the ingestion process from ODS to DW.
        """
        try:
            for ref_date in ref_dates_list:
                step_key = f"DWIngest_{data_type}_{ref_date}"

                if self.tracker.was_executed(step_key, ref_date):
                    self.logger.info(f"Parquet for {data_type} - {ref_date} already processed. Skipping.")
                    continue

                self.logger.info(f"Processing parquet {data_type} for date {ref_date}.")
                self._process_file(data_type, ref_date)

            self.logger.info(f"DW Ingestion for {data_type} Completed")

        except Exception as e:
            self.logger.error(f"Error during ingestion: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise

    def _process_file(self, data_type: str, ref_date: str):
        try:
            # # Ingest Company Data
            # company_df = self.spark.read.parquet(self.ods_dtbl)
            # company_df.write.mode("overwrite").parquet(self.dw_company_data)
            # self.logger.info("Ingested Company Data to DW")
            step_key = f"DWIngest_{data_type}_{ref_date}"
            if data_type == 'DataBlocks':
                v_fields = 'dtbl_mandatory_fields'
                v_path = self.ods_dtbl
                v_whitelist = 'whitelist_datablocks'
            elif data_type == 'FamilyTree':
                v_fields = 'fmlt_mandatory_fields'
                v_path = self.ods_fmlt
                v_whitelist = 'whitelist_familytree'
            else:
                self.logger.error(f"Unknown data type: {data_type}")
                return
            
            data_path = os.path.join(v_path, ref_date)
            df = self.spark.read \
                .option("mergeSchema", "true") \
                .parquet(data_path)

            df_valid, df_quarantine = self._initial_validation(df, v_fields)

            bad_dtbl = df_quarantine.count()
            if bad_dtbl > 0:
                self._save_quarantine(df_quarantine, f"Missing fields in {data_type} for date {ref_date}")

            if data_type == 'DataBlocks':
                self._save_without_explode("dw_company_name", df_valid, v_whitelist, self.dw_company_data)
                self._save_with_explode("dw_industry_code", df_valid, v_whitelist, self.dw_industry_code, "industryCodes")
                self._save_with_explode("dw_stock_exchange", df_valid, v_whitelist, self.dw_stock_exchanges, "stockExchanges")
            else:
                self._save_without_explode("dw_family_tree", df_valid, v_whitelist, self.dw_family_tree)
                self._save_with_explode("dw_family_tree_member", df_valid, v_whitelist, self.dw_family_members, "familyTreeMembers")

            self.tracker.register_success(step_key, ref_date, data_type)

        except Exception as e:
            self.logger.error(f"DW Ingestion Process Failed: {e}")
            raise