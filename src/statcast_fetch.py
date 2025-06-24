import argparse
import datetime
import io
import pandas as pd
import logging
import sys
import time
import csv
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from src.config.config import (
    BASE_MLB_URL, BASE_MiLB_URL,
    MLB_HEADERS, MiLB_HEADERS, PARAMS_DICT
)
from src.config.logging_config import setup_logging


logger = logging.getLogger(__name__)


def _daterange(start_date, end_date, chunk_size, step_days=None):
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


def _fetch_chunk(start_date_str, end_date_str, base_url, headers, parameters, max_retries=3, backoff_factor=2):
    params_copy = parameters.copy()
    params_copy["game_date_gt"] = start_date_str
    params_copy["game_date_lt"] = end_date_str

    attempt = 0
    while attempt <= max_retries:
        try:
            import requests
            response = requests.get(base_url, headers=headers, params=params_copy, timeout=180)
            response.raise_for_status()
            df = pd.read_csv(io.BytesIO(response.content))
            logging.debug(f"✅ Downloaded data from {start_date_str} to {end_date_str} ({len(df)} rows)")
            return df

        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Request error ({attempt + 1}/{max_retries}) from {start_date_str} to {end_date_str}: {e}")
            attempt += 1
            if attempt > max_retries:
                logging.error(f"❌ All retries failed for {start_date_str} to {end_date_str}")
                break
            sleep(backoff_factor ** attempt)

        except pd.errors.EmptyDataError:
            logging.warning(f"⚠️ No data returned from {start_date_str} to {end_date_str}")
            return pd.DataFrame()

        except Exception as e:
            logging.error(f"❌ Unexpected error: {e}")
            return pd.DataFrame()


def _fetch_data_in_parallel(start_date, end_date, base_url, headers, parameters,
                            file_name, chunk_size=7, step_days=None, max_workers=4,
                            writer=None, progress=True):
    start_dt = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()

    all_data = []
    chunks = list(_daterange(start_dt, end_dt, chunk_size, step_days))

    tqdm_func = tqdm if progress else lambda *args, **kwargs: DummyTqdm()

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
                    if not df_chunk.empty:
                        all_data.append(df_chunk)
                except Exception as e:
                    logging.debug(f"💥 Exception in chunk {chunk_start_str} to {chunk_end_str}: {e}")
                finally:
                    download_bar.update(1)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        with open(file_name, mode="w", newline="", encoding="utf-8") as f:
            csv_writer = writer or csv.writer(f)
            csv_writer.writerow(final_df.columns)
            for row in tqdm_func(final_df.itertuples(index=False, name=None),
                                 total=len(final_df), desc="Saving to CSV", unit="row"):
                csv_writer.writerow(row)
        logging.info(f"💾 Data saved to {file_name} ({len(final_df)} rows)")
    else:
        logging.warning("⚠️ No data fetched")


class DummyTqdm:
    def __init__(self, *args, **kwargs): pass
    def update(self, n): pass
    def __enter__(self): return self
    def __exit__(self, *args): pass


def run_statcast_pipeline(start_date, end_date, league="mlb", file_name=None,
                          chunk_size=7, step_days=None, max_workers=4,
                          log_level="INFO", progress=True):
    setup_logging(log_level)

    start_time = time.time()
    if league in ("mlb", "both"):
        logging.info("📦 Fetching MLB data...")
        file = file_name or "statcast_mlb.csv"
        _fetch_data_in_parallel(
            start_date, end_date, BASE_MLB_URL, MLB_HEADERS, PARAMS_DICT,
            file, chunk_size, step_days, max_workers, progress=progress
        )

    if league in ("milb", "both"):
        logging.info("📦 Fetching MiLB data...")
        milb_params = PARAMS_DICT.copy()
        milb_params["minors"] = "true"
        file = (file_name.replace(".csv", "_milb.csv")
                if file_name else "statcast_milb.csv")
        _fetch_data_in_parallel(
            start_date, end_date, BASE_MiLB_URL, MiLB_HEADERS, milb_params,
            file, chunk_size, step_days, max_workers, progress=progress
        )

    elapsed = (time.time() - start_time) / 60
    logging.info(f"⏱️ Completed in {elapsed:.2f} minutes")


def main():
    parser = argparse.ArgumentParser(description="Download Statcast data.")
    parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--league", choices=["mlb", "milb", "both"], default="mlb")
    parser.add_argument("--file_name", help="Output CSV file name")
    parser.add_argument("--chunk_size", type=int, default=7)
    parser.add_argument("--step_days", type=int)
    parser.add_argument("--max_workers", type=int, default=4)
    parser.add_argument("--log_level", default="INFO")
    parser.add_argument("--no_progress", action="store_true", help="Disable progress bars")

    args = parser.parse_args()

    run_statcast_pipeline(
        start_date=args.start_date,
        end_date=args.end_date,
        league=args.league,
        file_name=args.file_name,
        chunk_size=args.chunk_size,
        step_days=args.step_days,
        max_workers=args.max_workers,
        log_level=args.log_level,
        progress=not args.no_progress
    )


if __name__ == "__main__":
    main()
