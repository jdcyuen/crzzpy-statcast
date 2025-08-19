import logging
import threading
from src.utils.bq_schema_helper import align_df_to_bq_schema
from src.writers.base_writer import DataWriter
from google.cloud import bigquery
from google.api_core.exceptions import (
    NotFound, Conflict, BadRequest, Forbidden,
    ServiceUnavailable, InternalServerError, DeadlineExceeded
)
import pandas as pd
from datetime import datetime
import pandas_gbq
from src.config.config import GCP_PROJECT_ID, GCP_DATASET_ID, GCP_TABLE_PREFIX
import numpy as np

class BQWriter(DataWriter):

    _lock = threading.Lock()
    _first_write_done = False

    def __init__(self, project_id: str, dataset_id: str, table_prefix: str):
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = dataset_id
        self.table_prefix = table_prefix

    def _truncate_table(self, table_id: str):
        try:
            self.client.get_table(table_id)  # Check if table exists
            logging.debug(f"Table {table_id} does exist.", exc_info=True)
        except NotFound:
            logging.debug(f"Table {table_id} does not exist, skipping truncate.", exc_info=True)
            return
        
        logging.info(f"Manually truncating BigQuery table {table_id}")
        query = f"TRUNCATE TABLE `{table_id}`"
        job = self.client.query(query)
        job.result()  # Wait for completion
        logging.info(f"Table {table_id} truncated successfully")    

    def write(self, df: pd.DataFrame, league: str, schema_fields: list, truncate_table: bool = False):
        """
        Upload the entire DataFrame to BigQuery.
        If truncate_table=True, truncates the table; otherwise, appends data.

        Only one thread will ever get WRITE_TRUNCATE.
        All others automatically switch to WRITE_APPEND, even if they were called with truncate_table=True.
        Works in parallel ThreadPoolExecutor or multi-threaded scenarios.
        """

        logging.debug(f"Writing data to  BigQuery table") 

        current_year = datetime.now().year
        table_name = f"{self.table_prefix}_{current_year}_{league}"
        table_id = f"{self.client.project}.{self.dataset_id}.{table_name}"
        
        # Ensure truncate happens only once
        with BQWriter._lock:
            if not BQWriter._first_write_done:
                do_truncate  = True
                BQWriter._first_write_done = True
            else:
                do_truncate  = False

        if df.empty:
            if do_truncate:
                # Manual truncate, no load (empty DF causes schema error)                
                self._truncate_table(table_id)
            else:
                logging.warning(f"Empty DataFrame with no truncate for table {table_id}; skipping upload.")
            return
       
        # For non-empty DF, use WRITE_TRUNCATE on first write, else append
        write_disposition = (
            bigquery.WriteDisposition.WRITE_TRUNCATE if do_truncate else bigquery.WriteDisposition.WRITE_APPEND
        )

         # Load job config to overwrite the table
        bq_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=False,
        )

        action = "Truncating" if truncate_table else "Appending to"
        logging.debug(f"{action} BigQuery table {table_id} with {len(df)} rows")
        logging.debug(f"truncate_table boolean: {truncate_table}")

        #load_job = self.client.load_table_from_dataframe(df, table_id, job_config=bq_config)
        load_job = None
        try:
            
            #print("🔎 Debugging `game_date` column:")
            #print(df["game_date"].dtype)
            #
            # 
            # 
            # print(df["game_date"].unique()[:10])  # peek at first 10 unique values    

            # Align df to table schema
            #df_aligned = align_df_to_bq_schema(df, schema_fields)

            # ✅ Convert DataFrame to list of dictionary records
            #rows = df_aligned.to_dict(orient="records")
            rows = df.to_dict(orient="records")


            #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
            logging.debug(f"After call to df.to_dict")
            # Get all game_date values into a list
            game_dates = [row.get("game_date") for row in rows]

            # Print the first 10
            logging.debug(game_dates[:10])                

            #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

            # Load from list of dicts using load_table_from_json
            logging.debug(f"Loading BigQuery table - load_table_from_json(): {table_id}")
            #print(df_aligned.dtypes)
            load_job = self.client.load_table_from_json(rows, table_id, job_config=bq_config)

            load_job.result()  # Wait for completion

        except (NotFound, Conflict, BadRequest, Forbidden) as e:
            logging.error(f"Known BigQuery error: {e}", exc_info=True)
        except (ServiceUnavailable, InternalServerError, DeadlineExceeded) as e:
            logging.error(f"Transient error – consider retrying: {e}", exc_info=True)
        except Exception as e:
            logging.error(f"Unexpected error: {e}", exc_info=True)
        finally:
            if load_job:
                logging.debug(f"Load job ID: {load_job.job_id}")
            else:
                logging.debug("No load_job was created; skipping job ID log")
        
        logging.debug(f"Upload complete to BigQuery: {len(df)} rows to {table_id}")