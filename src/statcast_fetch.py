import argparse
import datetime
import io
import pandas as pd
import logging
import sys
import time
import csv
import os
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from tqdm import tqdm
import numpy as np
from src.writers.bq_writer import BQWriter
from src.writers.csv_writer import CSVWriter
from src.utils.bq_schema_helper import align_df_to_bq_schema, get_field_type
from itertools import islice
import json
import re

from google.cloud import bigquery
from google.api_core.exceptions import (Conflict, BadRequest, Forbidden, NotFound,
    ServiceUnavailable, InternalServerError, DeadlineExceeded
)

from src.config.config import (
    BASE_MLB_URL, BASE_MiLB_URL,
    MLB_HEADERS, MiLB_HEADERS, PARAMS_DICT, GCP_PROJECT_ID, GCP_DATASET_ID, GCP_TABLE_PREFIX, KNOWN_COLUMN_TYPES
)
from src.config.logging_config import setup_logging

logger = logging.getLogger(__name__)

GLOBAL_SCHEMA = []

def _daterange(start_date, end_date, chunk_size, step_days=None):

    """
        Yields date chunks between start_date and end_date.

        Args:
            start_date (datetime.date): Start of the range.
            end_date (datetime.date): End of the range.
            chunk_size (int): Number of days in each chunk.
            step_days (int, optional): Days to step forward after each chunk. 
                                    Defaults to chunk_size (non-overlapping).

        Yields:
            tuple: (chunk_num, chunk_start_date, chunk_end_date)
    """


    logging.info(f"chunk_size {chunk_size}: step_days = {step_days}")

    chunk_num = 1
    step = datetime.timedelta(days=step_days if step_days else chunk_size)
    current = start_date

    while current <= end_date:
        chunk_start = current
        chunk_end = min(current + datetime.timedelta(days=chunk_size - 1), end_date)
        logging.debug(f"Chunk {chunk_num}: start = {chunk_start}  end = {chunk_end}")
        yield chunk_num, chunk_start, chunk_end
        chunk_num += 1
        current += step

# Recursive function to find all hit_location values
def find_key(d, key):
    if isinstance(d, dict):
        for k, v in d.items():
            if k == key:
                yield v
            else:
                yield from find_key(v, key)
    elif isinstance(d, list):
        for item in d:
            yield from find_key(item, key)

def _fetch_chunk(start_date_str, end_date_str, base_url, headers, parameters, max_retries=3, backoff_factor=2):
    params_copy = parameters.copy()
    params_copy["game_date_gt"] = start_date_str
    params_copy["game_date_lt"] = end_date_str

    attempt = 0
    while attempt <= max_retries:
        try:
            
            response = requests.get(base_url, headers=headers, params=params_copy, timeout=180)
            response.raise_for_status()
            

            #vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
            logging.debug(f"===============================================")
            logging.debug(f"Checking for field in response")
            # Use csv.DictReader to preserve raw values as strings
            reader = csv.DictReader(io.StringIO(response.text))
            # Print the first 10 raw values 
            for i, row in enumerate(reader):
                if i >= 10:
                    break
                logging.debug(row.get("game_date"))

            logging.debug(f"===============================================")                 

            #^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

            df = pd.read_csv(io.BytesIO(response.content), dtype=str)

            #vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
            logging.debug(f"===============================================")  
            logging.debug(f"Checking for field in dataframe")
            # Check if column exists
            if "game_date" in df.columns:
                # Print first 10 values
                logging.debug(df["game_date"].head(10).tolist())
            else:
                logging.debug("Column 'game_date' not found in DataFrame")

            logging.debug(f"===============================================")  
            #vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv


            logging.debug(f"✅ Downloaded data from {start_date_str} to {end_date_str} ({len(df)} rows)")
            return df

        except requests.exceptions.RequestException as e:
            #logging.error(f"❌ Request error ({attempt + 1}/{max_retries}) from {start_date_str} to {end_date_str}: {e}")
            logging.error(f"❌ Request error ({attempt}/{max_retries}) from {start_date_str} to {end_date_str}: {e}")
            attempt += 1
            if attempt > max_retries:
                logging.error(f"❌ All retries failed for {start_date_str} to {end_date_str}", exc_info=True)
                break
            sleep(backoff_factor ** attempt)

        except pd.errors.EmptyDataError:
            logging.error(f"⚠️ No data returned from {start_date_str} to {end_date_str}", exc_info=True)
            return pd.DataFrame()

        except Exception as e:
            logging.error(f"❌ Unexpected error: {e}", exc_info=True)
            return pd.DataFrame()


def _fetch_data_in_parallel(start_date, end_date, base_url, headers, parameters,
                            file_name, league, chunk_size=5, step_days=None, max_workers=4,
                            bqwriter=None,  csvwriter=None, progress=True):
    start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

    all_data = []
    chunks = list(_daterange(start_dt, end_dt, chunk_size, step_days))

    tqdm_func = tqdm if progress else lambda *args, **kwargs: DummyTqdm()

    current_year = datetime.datetime.now().year
    prefix =f"{GCP_TABLE_PREFIX}_{current_year}_{league}"
    #client = bigquery.Client("crzzpy")
    table_ref = f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{prefix}"

    truncate_table=True
    schema_generation_count = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        with tqdm_func(total=len(chunks), desc="Submitting chunks", unit="chunk", file=sys.stdout) as submit_bar:
            for _, chunk_start, chunk_end in chunks:
                chunk_start_str = chunk_start.strftime("%Y-%m-%d")
                chunk_end_str = chunk_end.strftime("%Y-%m-%d")
                future = executor.submit(
                    _fetch_chunk, chunk_start_str, chunk_end_str,
                    base_url, headers, parameters
                )
                future.chunk_info = (chunk_start_str, chunk_end_str)
                futures.append(future)
                submit_bar.update(1)

        with tqdm_func(total=len(futures), desc="Downloading chunks", unit="chunk", file=sys.stdout) as download_bar:
            for future in as_completed(futures):
                chunk_start_str, chunk_end_str = future.chunk_info
                try:
                    df_chunk = future.result()
                    logging.debug(f"📥 Raw chunk: {chunk_start_str} to {chunk_end_str}, rows={len(df_chunk)}")
                    
                    if not table_exists(GCP_PROJECT_ID, GCP_DATASET_ID, prefix):
                        logging.debug(f"🧼 Table does NOT exist: {table_ref}......................")
                        #bq_schema = generate_schema(KNOWN_COLUMN_TYPES, df_chunk.columns, target="bigquery")
                        GLOBAL_SCHEMA = generate_schema(KNOWN_COLUMN_TYPES, df_chunk.columns, target="bigquery")
                        table = create_bigquery_table(GCP_PROJECT_ID, GCP_DATASET_ID, GCP_TABLE_PREFIX, league,  GLOBAL_SCHEMA)

                        logging.debug(get_field_type(GLOBAL_SCHEMA, "arm_angle"))

                        # Loop through schema fields
                        for field in table.schema:
                            if field.name == "arm_angle":
                                logging.debug(f"DEBUG - Column: {field.name}, Type: {field.field_type} after table creation")

                        schema_generation_count+=1
                       
                    else:
                        if schema_generation_count <= 0:
                            GLOBAL_SCHEMA = generate_schema(KNOWN_COLUMN_TYPES, df_chunk.columns, target="bigquery")
                            schema_generation_count+=1
                    

                    if not df_chunk.empty:                    
                        logging.debug(f"🧼 Before cleaning: {df_chunk.shape}")
                        df_chunk = clean_dataframe(df_chunk)
                        logging.debug(f"🧼 After cleaning: {df_chunk.shape}")  

                        if bqwriter:
                                logging.debug(f"📤 Writing chunk {chunk_start_str} to {chunk_end_str} to BigQuery...")
                                bqwriter.write(df_chunk, league, GLOBAL_SCHEMA, truncate_table)
                                truncate_table=False

                        all_data.append(df_chunk)
                except Exception as e:
                    logging.error(f"💥 Exception in chunk {chunk_start_str} to {chunk_end_str}: {e}", exc_info=True)
                finally:
                    download_bar.update(1)

    if all_data:
        '''
        final_df = pd.concat(all_data, ignore_index=True)
        with open(file_name, mode="w", newline="", encoding="utf-8") as f:
            csv_writer = writer or csv.writer(f)
            csv_writer.writerow(final_df.columns)
            for row in tqdm_func(final_df.itertuples(index=False, name=None),
                                 total=len(final_df), desc="Saving to CSV", unit="row"):
           
                # logging.debug(f"💾 row saved: {row}")
                csv_writer.writerow(row)        
        logging.debug(f"💾 Data saved to {file_name} ({len(final_df)} rows)")
        '''
    else:
        logging.warning("⚠️ No data fetched")


def clean_dataframe(df_chunk):
    """Cleans DataFrame for BigQuery insertion: handles NaN, None, and timestamps."""
    df_chunk = df_chunk.copy()  # Avoid modifying the original DataFrame
    
    # ✅ Convert timestamps to string format
    for col in df_chunk.select_dtypes(include=["datetime64"]).columns:
        df_chunk[col] = df_chunk[col].astype(str)  # Converts to 'YYYY-MM-DD HH:MM:SS'
    
    # ✅ Convert game_date to YYYY-MM-DD format
    if "game_date" in df_chunk.columns:
        df_chunk["game_date"] = pd.to_datetime(df_chunk["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # ✅ Convert all NaN-like values to None
    df_chunk = df_chunk.replace({np.nan: None, pd.NA: None, None: None})

    return df_chunk

def count_rows_in_csv(file_name):
    with open(file_name, mode='r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        row_count = sum(1 for _ in reader)
    logging.info(f"  Total number of rows in '{file_name}': {row_count}.")
    return row_count

def table_exists(project_id: str, dataset_id: str, table_id: str) -> bool:

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    logging.debug(f"🧼 table_ref: {table_ref}......................")

    try:
        client.get_table(table_ref)
        logging.debug(f"🧼 Table does exist: {table_ref}......................")
        return True
    
    except NotFound:
        logging.error(f"🧼 Table does NOT exist: {table_ref}......................", exc_info=True)
        return False 
    
def generate_schema(known_column_types: dict, table_headers: list,
                    target: str = "bigtable", column_family: str = "cf1"):
    """
    Generate a schema for Bigtable or BigQuery based on KNOWN_COLUMN_TYPES and table headers.

    Args:
        known_column_types (dict): Column → type mapping
        table_headers (list): List of table headers
        target (str): "bigtable" or "bigquery"
        column_family (str): Bigtable column family name (default "cf1")

    Returns:
        dict or list: Bigtable-style schema (dict) or BigQuery SchemaField list

    Usage:
        # Bigtable schema dictionary
        bt_schema = generate_schema(KNOWN_COLUMN_TYPES, table_headers, target="bigtable")
        print("Bigtable Schema:", bt_schema)

        # BigQuery BigQuery SchemaField list
        bq_schema = generate_schema(KNOWN_COLUMN_TYPES, table_headers, target="bigquery")
        print("\nBigQuery Schema:", bq_schema)
    """

    logging.debug(f"Creating schema...")
    #logging.debug(f"Dataframe headers: {table_headers}")

    if target.lower() == "bigtable":
        schema = {"column_families": {column_family: []}}
        for col in table_headers:
            col_type = known_column_types.get(col, "STRING")
            schema["column_families"][column_family].append({
                "qualifier": col,
                "type": col_type
            })
        return schema

    elif target.lower() == "bigquery":
        '''
        schema = [
            bigquery.SchemaField(col, known_column_types.get(col, "STRING"))
            for col in table_headers
        ]
        '''

        schema = []
        for col in table_headers:
            col_type = known_column_types.get(col, "STRING")
            #logging.debug(f"[DEBUG] Adding SchemaField for '{col}' with type '{col_type}'")
            schema.append(bigquery.SchemaField(col, col_type))
        return schema

    else:
        raise ValueError("target must be either 'bigtable' or 'bigquery'")
    
def create_bigquery_table(project_id: str, dataset_id: str, table_id: str, league:str,  schema_fields: list):
    """
    Creates a BigQuery table.

    Args:
        project_id (str): Google Cloud project ID.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.
        schema_fields (list): List of bigquery.SchemaField objects defining the table schema.

    Returns:
        bigquery.Table: The created table object.
    """
    logging.debug(f"Creating table {table_id}...")

    current_year = datetime.datetime.now().year

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}_{current_year}_{league}"

    logging.debug(f"arm_angle:")
    logging.debug(get_field_type(schema_fields, "arm_angle"))

    # Define and create table
    table = bigquery.Table(table_ref, schema=schema_fields)

    try:

        table = client.create_table(table)
        logging.debug(f"✅ Table created: {table.full_table_id}")

        # Loop through schema fields
        for field in table.schema:
            if field.name == "arm_angle":
                logging.debug(f"DEBUG - Column: {field.name}, Type: {field.field_type} after table creation")

        return table
    
    except Conflict:
        logging.error(f"⚠ Table already exists: {table_ref}", exc_info=True)
    except NotFound:
        logging.error(f"❌ Dataset not found: {dataset_id}", exc_info=True)
    except Forbidden:
        logging.error("❌ Permission denied. Check your IAM roles.", exc_info=True)
    except BadRequest as e:
        logging.error(f"❌ Invalid schema: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"❌ Unexpected error: {e}", exc_info=True)


def run_statcast_download(start_date, end_date, bq_writer=None, csv_writer=None, league="mlb", file_name=None,
                          chunk_size=5, step_days=None, max_workers=4,
                          log_level="INFO", progress=True):
    #setup_logging(log_level)

    start_time = time.time()
    if league in ("mlb", "both"):
        logging.info("📦 Fetching MLB data...")
        file = file_name or "statcast_mlb.csv"
        _fetch_data_in_parallel(
            start_date, end_date, BASE_MLB_URL, MLB_HEADERS, PARAMS_DICT,
            file, "mlb", chunk_size, step_days, max_workers, bq_writer, csv_writer, progress=progress
        )

        if os.path.exists(file):
            count = count_rows_in_csv(file)


    if league in ("milb", "both"):
        logging.info("📦 Fetching MiLB data...")
        milb_params = PARAMS_DICT.copy()
        milb_params["minors"] = "true"
        file = (file_name.replace(".csv", "_milb.csv")
                if file_name else "statcast_milb.csv")
        _fetch_data_in_parallel(
            start_date, end_date, BASE_MiLB_URL, MiLB_HEADERS, milb_params,
            file, "milb", chunk_size, step_days, max_workers, bq_writer, csv_writer, progress=progress
        )

        if os.path.exists(file):
            count = count_rows_in_csv(file)

    elapsed = (time.time() - start_time) / 60
    logging.info(f"⏱️ Completed in {elapsed:.2f} minutes")


def main():
    parser = argparse.ArgumentParser(description="Download Statcast data.")
    parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--league", choices=["mlb", "milb", "both"], default="mlb")
    parser.add_argument("--file_name", help="Output CSV file name")
    parser.add_argument("--chunk_size", type=int, default=5)
    parser.add_argument("--step_days", type=int)
    parser.add_argument("--max_workers", type=int, default=4)
    parser.add_argument("--log_level", default="INFO")
    parser.add_argument("--no_progress", action="store_true", help="Disable progress bars")
    parser.add_argument("--destination", choices=["bq", "csv", "both"], default="bq")
    parser.add_argument("--csv_dir", default="csv_data", help="Directory to save CSV files")


    '''
    | Command                    | Behavior                   |
    | -------------------------- | -------------------------- |
    | `--log-to-file`            | Logs to `logs/statcast.log`|
    | `--log-to-file my_log.txt` | Logs to `my_log.txt`       |
    | (No `--log-to-file`)       | Console logging only       |

    '''
    parser.add_argument("--log-to-file", nargs="?", const="statcast.log", metavar="LOG_FILE",
        help="Enable logging to a file (default: statcast.log). Optionally provide a custom log file name.")  

    args = parser.parse_args()
    setup_logging(args.log_level, log_file=args.log_to_file)

    bq_writer = BQWriter(GCP_PROJECT_ID, GCP_DATASET_ID, GCP_TABLE_PREFIX)
    if args.destination in ("csv", "both"):
        csv_writer = CSVWriter(args.csv_dir)
    else:
        csv_writer = None

    run_statcast_download(
        start_date=args.start_date,
        end_date=args.end_date,
        league=args.league,
        file_name=args.file_name,
        chunk_size=args.chunk_size,
        step_days=args.step_days,
        max_workers=args.max_workers,
        log_level=args.log_level,
        progress=not args.no_progress,
        bq_writer=bq_writer,
        csv_writer = csv_writer
    )

class DummyTqdm:
    """Fallback when progress bars are disabled."""
    def __init__(self, *args, **kwargs): pass
    def update(self, n=1): pass
    def close(self): pass
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def __iter__(self): return iter([])
    def write(self, x): pass

if __name__ == "__main__":
    main()
