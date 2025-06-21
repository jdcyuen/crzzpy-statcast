import argparse
import datetime
import io
# writers/base_writer.py
from abc import ABC, abstractmethod
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import os
from time import sleep
from config.logging_config import setup_logging
import logging
import sys
import time
import threading

from config.config import BASE_MLB_URL, BASE_MiLB_URL, MLB_HEADERS, MiLB_HEADERS, PARAMS_DICT

import requests
from requests.exceptions import ConnectionError
# from pybaseball import statcast


logger = logging.getLogger(__name__)

class DataWriter(ABC):
    @abstractmethod
    def write(self, df: pd.DataFrame, file_name: str):
        pass

class CSVWriter(DataWriter):
    def write(self, df: pd.DataFrame, file_name: str):
        with open(file_name, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(df.columns)
            for row in tqdm(df.itertuples(index=False, name=None), total=len(df), desc="Saving to CSV", unit="row"):
                writer.writerow(row)
class ParquetWriter(DataWriter):
    def write(self, df: pd.DataFrame, file_name: str):
        df.to_parquet(file_name, index=False)

class JSONWriter(DataWriter):
    def write(self, df: pd.DataFrame, file_name: str):
        df.to_json(file_name, orient="records", lines=True)

class LeagueFetcher(ABC):
    @abstractmethod
    def fetch(self, start_date, end_date, file_name, args):
        pass               

class MLBFetcher(LeagueFetcher):
    def fetch(self, start_date, end_date, file_name, args):
        logging.info("Getting data for mlb")
        _fetch_data_in_parallel(start_date, end_date, BASE_MLB_URL, MLB_HEADERS, PARAMS_DICT, file_name, args.chunk_size, args.step_days, args.max_workers, args.writer)

class MiLBFetcher(LeagueFetcher):
    def fetch(self, start_date, end_date, file_name, args):
        logging.info("Getting data for milb")
        params = PARAMS_DICT.copy()
        params.update({"minors": "true"})
        _fetch_data_in_parallel(start_date, end_date, BASE_MiLB_URL, MiLB_HEADERS, params, file_name, args.chunk_size, args.step_days, args.max_workers, args.writer)        

#start_date: the beginning date of the range (expected to be a datetime.date object).
#end_date: the end date of the range (inclusive).
#chunk_size: how many days are in each chunk.
#step_days: optional; how many days to move forward after each chunk. If not given, defaults to chunk_size.
#           overlapping, non-overlapping, or gapped windows depending on step_days
#           This uses step = chunk_size, so the chunks do not overlap
def _daterange(start_date, end_date, chunk_size, step_days=None):

    """
    Generator that yields (chunk_number, chunk_start_date, chunk_end_date)
    for each chunk in the date range.
    """
    chunk_num = 1
    step = datetime.timedelta(days=step_days if step_days else chunk_size)
    current = start_date

    while current < end_date:
        chunk_start = current
        chunk_end = min(current + datetime.timedelta(days=chunk_size - 1), end_date)
        logging.debug(f"Chunk {chunk_num}: start = {chunk_start}  end = {chunk_end}")  # 👈 print chunk_end
        yield chunk_num, chunk_start, chunk_end
        chunk_num += 1
        current += step
        

def _fetch_chunk(start_date_str, end_date_str, base_url, headers, parameters, max_retries=3, backoff_factor=2):
    """
    Fetch a chunk of data from Baseball Savant using HTTP headers to avoid 403 errors.
    """
    #threading.current_thread().name = f"Fetcher-{start_date_str}_to_{end_date_str}"

    params_copy = parameters.copy()
    params_copy["game_date_gt"] = start_date_str
    params_copy["game_date_lt"] = end_date_str

    logging.debug(f"\nbase url: {base_url} \nheaders: {headers} \nparameters: {params_copy}")

    attempt = 0
    while attempt <= max_retries:
        try:
            response = requests.get(base_url, headers=headers, params=params_copy, timeout=180)
            response.raise_for_status()  # Raise HTTPError for bad status codes

            df = pd.read_csv(io.BytesIO(response.content))
            logging.debug(f"✅========== Downloaded data from {start_date_str} to {end_date_str} ({len(df)} rows)")
            return df

        except requests.exceptions.HTTPError as http_err:
            logging.error(f"❌ HTTP error from {start_date_str} to {end_date_str}, Request attempt {attempt + 1} failed: {http_err}")
            attempt += 1
            if attempt > max_retries:
                logging.error(f"❌ All {max_retries} retries failed for {start_date_str} to {end_date_str}")
                break
            sleep_time = backoff_factor ** attempt
            logging.error(f"⏳ Retrying after {sleep_time} seconds...")
            time.sleep(sleep_time)

        except requests.exceptions.RequestException as req_err:
            logging.error(f"❌ Request failed from {start_date_str} to {end_date_str}, Request attempt {attempt + 1} failed: {req_err}")
            attempt += 1
            if attempt > max_retries:
                logging.error(f"❌ All {max_retries} retries failed for {start_date_str} to {end_date_str}")
                break
            sleep_time = backoff_factor ** attempt
            logging.error(f"⏳ Retrying after {sleep_time} seconds...")
            time.sleep(sleep_time)

        except pd.errors.EmptyDataError:
            logging.error(f"⚠️ No data returned from {start_date_str} to {end_date_str}")
            return pd.DataFrame()  # ⬅️ Explicitly return empty DataFrame
        except Exception as e:
            logging.error(f"❌ Failed to fetch data from {start_date_str} to {end_date_str}: {e}")
            return None

#Input Parameters:   
#   start_date, end_date: Strings in the format YYYY-MM-DD representing the full date range.
#   base_url: API endpoint or base URL used to fetch Statcast data.
#   headers:
#   parameters:
#   file_name: Path to the output CSV file.
#   chunk_size: How many days of data each chunk should cover (default is 7).
#   step_days: Optional; if set, controls the sliding window step size (e.g., step size smaller than chunk size means overlapping).
#   max_workers: Number of threads to use concurrently.
def _fetch_data_in_parallel(start_date, end_date, base_url, headers, parameters, file_name, chunk_size, step_days, max_workers, writer):
    """
    Shared logic to fetch Statcast data in parallel and write to a CSV file.
    """
    logging.debug(f"\nbase url: {base_url} \nheaders: {headers} \nparameters: {parameters} \nfile_name:{file_name} \nchunk_size:{chunk_size} \nstep_days:{step_days}")

    start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

    all_data = []
    chunks = list(_daterange(start_dt, end_dt, chunk_size, step_days))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        # Progress bar for submitting
        with tqdm(total=len(chunks), desc="Submitting chunks", unit="chunk", file=sys.stdout) as submit_bar:
            for _, chunk_start, chunk_end in chunks:

                chunk_start_str = chunk_start.strftime("%Y-%m-%d")
                chunk_end_str = chunk_end.strftime("%Y-%m-%d")
                
                logging.debug(f"📥 Submitting download for chunk {chunk_start_str} to {chunk_end_str}")
                future = executor.submit(_fetch_chunk, chunk_start_str, chunk_end_str, base_url, headers, parameters)
                future.chunk_info = (chunk_start_str, chunk_end_str)
                futures.append(future)
                submit_bar.update(1)

        # Progress bar for downloading
        with tqdm(total=len(futures), desc="Downloading chunks", unit="chunk", file=sys.stdout) as download_bar:
            for future in as_completed(futures):
                chunk_start_str, chunk_end_str = future.chunk_info
                try:
                    df_chunk = future.result()

                    if df_chunk is None:
                        logging.debug(f"df_chunk is None for {chunk_start_str} to {chunk_end_str}")
                    elif df_chunk.empty:
                        logging.debug(f"df_chunk is empty for {chunk_start_str} to {chunk_end_str}")
                    else:
                        logging.debug(f"Appending chunk for {chunk_start_str} to {chunk_end_str}")
                        all_data.append(df_chunk)
                except Exception as e:
                    logging.debug(f"💥 Exception for chunk {chunk_start_str} to {chunk_end_str}: {e}")
                finally:
                    download_bar.update(1)

    if all_data:
        logging.debug("⚠️ Saving data to file...")
        final_df = pd.concat(all_data, ignore_index=True)
        save_dataframe_to_csv(final_df, file_name)
    else:
        logging.debug("⚠️ No data fetched.")



def save_dataframe_to_csv(df: pd.DataFrame, file_name: str):
    with open(file_name, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(df.columns)
        for row in tqdm(df.itertuples(index=False, name=None), total=len(df), desc="Saving data to CSV", unit="row"):
            writer.writerow(row)
    logging.info(f"💾 Data saved to {file_name} ({len(df)} total rows)")

def calculate_days(start_date_str, end_date_str, date_format="%Y-%m-%d"):
    # Convert string dates to datetime objects
    start_date = datetime.strptime(start_date_str, date_format)
    end_date = datetime.strptime(end_date_str, date_format)
    
    # Calculate the difference
    delta = end_date - start_date
    
    # Return the number of days
    return delta.days    
  

def main():    

    #Main function to handle command-line arguments.
    parser = argparse.ArgumentParser(description="Download Statcast data for a given date range.")
    parser.add_argument("start_date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--league", choices=["mlb", "milb", "both"], default="both",
                        help="Which league data to download: 'mlb', 'milb', or 'both'")
    parser.add_argument("--file", type=str, default="statcast", help="Output CSV file name (default: statcast)")
    parser.add_argument("--format", choices=["csv", "parquet", "json"], default="csv")
    parser.add_argument("--chunk_size", type=int, default=7, help="Days per chunk")
    parser.add_argument("--step_days", type=int, default=None, help="Optional custom step between chunks")
    parser.add_argument("--max_workers", type=int, default=4, help="Number of parallel threads")
    parser.add_argument("--log", default="INFO", help="Set the logging level: DEBUG, INFO, WARNING, ERROR, or CRITICAL (default: INFO)")

    args = parser.parse_args()
    setup_logging(args.log)

    writer_map = {
        "csv": CSVWriter(),
        "parquet": ParquetWriter(),
        "json": JSONWriter()
    }

    args.writer = writer_map[args.format]
    file_ext = args.format if args.format != "csv" else "csv"


    if args.league == "mlb":
        start_time = time.time()
    
        MLBFetcher().fetch(args.start_date, args.end_date, f"{args.file}_{args.league}.{file_ext}", args)
        end_time = time.time()
        elapsed_time = (end_time - start_time)/60
        logging.info(f"Function took {elapsed_time:.4f} minutes")

    elif args.league == "milb":
        start_time = time.time()
        
        MiLBFetcher().fetch(args.start_date, args.end_date, f"{args.file}_{args.league}.{file_ext}", args)
        end_time = time.time()
        elapsed_time = (end_time - start_time)/60
        logging.info(f"Function took {elapsed_time:.4f} minutes")

    elif args.league == "both":
        start_time = time.time()
        
        MLBFetcher().fetch(args.start_date, args.end_date, f"{args.file}_mlb.{file_ext}", args)
        print(f"=========================================================================================================================")
       
        MiLBFetcher().fetch(args.start_date, args.end_date, f"{args.file}_milb.{file_ext}", args)
        end_time = time.time()
        elapsed_time = (end_time - start_time)/60
        logging.info(f"Function took {elapsed_time:.4f} minutes for both")

if __name__ == "__main__":
    main()
